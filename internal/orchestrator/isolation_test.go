package orchestrator

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/facts"
	"github.com/agent0ai/spynel/internal/workspace"
)

// isolatedFixture wires one manager against a real temporary Git repository
// with git-worktree isolation and a scripted harness whose turns edit their
// worktree and move the durable document exactly like real agents.
type isolatedFixture struct {
	t       *testing.T
	root    string
	cfg     config.Config
	manager *Manager
	fake    *emitCaptureHarness
	backend *execws.LocalGit
	baseSHA string
	target  string
}

func runFixtureGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func newIsolatedFixture(t *testing.T, checks []config.OrchestratorCheck) *isolatedFixture {
	t.Helper()
	root := t.TempDir()
	runFixtureGit(t, root, "init", "-q", "--initial-branch=main", ".")
	runFixtureGit(t, root, "config", "user.email", "fixture@example.com")
	runFixtureGit(t, root, "config", "user.name", "Fixture")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("# fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runFixtureGit(t, root, "add", "-A")
	runFixtureGit(t, root, "commit", "-qm", "base")
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Orchestrator.WorkspaceIsolation = config.WorkspaceIsolationGitWorktree
	cfg.Orchestrator.Checks = checks
	fake := newEmitCaptureHarness()
	manager := New(cfg, fake, extensions.Runner{})
	backend, err := execws.NewLocalGit(root)
	if err != nil {
		t.Fatal(err)
	}
	manager.WorkspaceBackend = backend
	fixture := &isolatedFixture{
		t: t, root: root, cfg: cfg, manager: manager, fake: fake, backend: backend,
		baseSHA: runFixtureGit(t, root, "rev-parse", "HEAD"),
		target:  "refs/heads/main",
	}
	fixture.installDefaultHook()
	return fixture
}

// installDefaultHook implements the standard agent behavior per session: an
// implementation turn edits its worktree and moves the document to review; a
// review turn verifies its workspace pins the reviewed result and moves the
// document to done. Notification turns finish immediately.
func (f *isolatedFixture) installDefaultHook() {
	f.setHook(func(h *emitCaptureHarness, key, prompt string) {
		if strings.HasPrefix(key, "orchestrator:notification:") {
			h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "notified", Done: true})
			return
		}
		lease, ok := f.leaseForSession(key)
		if !ok {
			return
		}
		if strings.Contains(key, ":task_implementation:") {
			workspaceView := f.openWorkspace(lease)
			if err := os.WriteFile(filepath.Join(workspaceView.ProviderView(), "feature.txt"), []byte("implemented by "+lease.LaunchID), 0o600); err != nil {
				f.t.Fatal(err)
			}
			f.moveDocument(lease.File, "review")
		} else if strings.Contains(key, ":task_review:") {
			workspaceView := f.openWorkspace(lease)
			// RV1: the reviewer workspace must be exactly the reviewed result.
			if err := workspaceView.VerifyAt(context.Background(), f.latestReviewRequestSHA(lease)); err != nil {
				f.t.Fatalf("reviewer workspace is not at the reviewed result: %v", err)
			}
			f.moveDocument(lease.File, "done")
		}
		h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "done", Done: true})
	})
}

func (f *isolatedFixture) setHook(hook func(h *emitCaptureHarness, key, prompt string)) {
	f.fake.mu.Lock()
	f.fake.onSend = hook
	f.fake.mu.Unlock()
}

func (h *emitCaptureHarness) emitNow(key string, event core.Event) {
	h.mu.Lock()
	emitters := h.emits[key]
	h.mu.Unlock()
	if len(emitters) == 0 {
		return
	}
	if event.ThreadID == "" {
		event.ThreadID = "thread-" + key
	}
	emitters[len(emitters)-1](event)
}

func (f *isolatedFixture) leaseForSession(key string) (Lease, bool) {
	leases, err := f.manager.loadLeases()
	if err != nil {
		return Lease{}, false
	}
	for _, lease := range leases {
		if lease.SessionKey == key {
			return lease, true
		}
	}
	return Lease{}, false
}

func (f *isolatedFixture) openWorkspace(lease Lease) execws.Workspace {
	f.t.Helper()
	view, err := f.backend.Open(context.Background(), execws.Ref{ID: lease.WorkspaceID, Kind: execws.BackendGitWorktree})
	if err != nil {
		f.t.Fatalf("open workspace %s: %v", lease.WorkspaceID, err)
	}
	return view
}

func (f *isolatedFixture) latestReviewRequestSHA(lease Lease) string {
	evidence, err := f.manager.documentFacts(lease.Route, documentIDForLease(lease))
	if err != nil {
		return ""
	}
	request, ok := latestReviewRequest(evidence)
	if !ok {
		return ""
	}
	return request.ReviewSHA
}

// moveDocument emulates the agent-authored durable transition through the
// ordinary Markdown workflow.
func (f *isolatedFixture) moveDocument(from, status string) {
	target := filepath.Join(filepath.Dir(filepath.Dir(from)), status, filepath.Base(from))
	if err := moveDocument(from, target, status, time.Now().UTC()); err != nil {
		f.t.Fatal(err)
	}
}

func (f *isolatedFixture) createTask(title string) string {
	f.t.Helper()
	path, err := Create(f.cfg, "tasks", title, "")
	if err != nil {
		f.t.Fatal(err)
	}
	return path
}

func (f *isolatedFixture) scan() {
	f.t.Helper()
	if err := f.manager.ScanOnce(context.Background()); err != nil {
		f.t.Fatalf("scan: %v", err)
	}
	f.manager.Wait()
}

func (f *isolatedFixture) factKinds(documentID string) []string {
	f.t.Helper()
	evidence, err := f.manager.documentFacts("tasks", documentID)
	if err != nil {
		f.t.Fatal(err)
	}
	kinds := make([]string, 0, len(evidence))
	for _, fact := range evidence {
		kinds = append(kinds, fact.Kind)
	}
	return kinds
}

func (f *isolatedFixture) promptFor(phaseMarker string) string {
	f.fake.mu.Lock()
	defer f.fake.mu.Unlock()
	for key, prompt := range f.fake.prompts {
		if strings.Contains(key, phaseMarker) {
			return prompt
		}
	}
	return ""
}

// normalizeAdmissionOrder accepts a provider terminal that arrived before the
// dispatch goroutine recorded admission: synchronous providers may emit their
// final event inside Send. The pair's relative order is not an invariant.
func normalizeAdmissionOrder(kinds []string) []string {
	normalized := append([]string(nil), kinds...)
	for i := 0; i+1 < len(normalized); i++ {
		if normalized[i] == facts.KindProviderTerminal && normalized[i+1] == facts.KindProviderAdmitted {
			normalized[i], normalized[i+1] = normalized[i+1], normalized[i]
			i++
		}
	}
	return normalized
}

func mustLeases(t *testing.T, manager *Manager) []Lease {
	t.Helper()
	leases, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	return leases
}

// Isolated end-to-end acceptance: launch_created through
// integration_completed, with the target branch moved to exactly R and no
// merge commit ever created.
func TestIsolatedEndToEnd(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "verify", Command: "git", Args: []string{"rev-parse", "HEAD"}},
	})
	taskPath := fixture.createTask("isolated end to end")
	documentID := documentIDFromPath(t, taskPath)

	// Implementation claim, launch, workspace, provider turn, terminal,
	// capture, and checks.
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 {
		t.Fatalf("implementation leases = %#v", leases)
	}
	implLease := leases[0]
	if implLease.WorkspaceKind != execws.WorkspaceKindGitWorktree || implLease.WorkspaceID == "" || implLease.LaunchID == "" {
		t.Fatalf("isolated lease = %#v", implLease)
	}
	if !strings.HasSuffix(implLease.SessionKey, ":"+implLease.LaunchID) {
		t.Fatalf("isolated session key must embed the launch: %q", implLease.SessionKey)
	}
	if !strings.Contains(fixture.promptFor(":task_implementation:"), "DO NOT run git commit") {
		t.Fatal("implementation prompt missing the do-not-commit rule")
	}
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	var resultSHA string
	for _, fact := range evidence {
		switch fact.Kind {
		case facts.KindResultCaptured:
			resultSHA = fact.ResultSHA
		case facts.KindCheckCompleted:
			if fact.Outcome != execws.CheckOutcomePassed || fact.ResultSHA != resultSHA || fact.Check != "verify" {
				t.Fatalf("check evidence = %+v for result %s", fact, resultSHA)
			}
		}
	}
	if resultSHA == "" || resultSHA == fixture.baseSHA {
		t.Fatalf("implementation did not capture a changed result: %q", resultSHA)
	}
	if implLease.State != "finalized" {
		t.Fatalf("implementation lease state = %q", implLease.State)
	}

	// The agent moved the document to review during its turn; the next scan
	// reconciles the isolated transition, records the review request, and —
	// because the scripted reviewer completes inside its own Send — claims and
	// finalizes the reviewer in the same scan.
	fixture.scan()
	evidence, err = fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	request, ok := latestReviewRequest(evidence)
	if !ok || request.ReviewSHA != resultSHA || request.ImplLaunch != implLease.LaunchID {
		t.Fatalf("review request = %+v, want SHA %s from launch %s", request, resultSHA, implLease.LaunchID)
	}

	// Reviewer claim: a fresh isolated reviewer pinned to the exact result.
	leases = mustLeases(t, fixture.manager)
	if len(leases) != 1 {
		t.Fatalf("reviewer leases = %#v", leases)
	}
	reviewLease := leases[0]
	if reviewLease.Phase != phaseTaskReview || reviewLease.LaunchID == implLease.LaunchID {
		t.Fatalf("reviewer lease = %#v", reviewLease)
	}
	if reviewLease.WorkspaceID == implLease.WorkspaceID {
		t.Fatal("the reviewer must not reuse the developer workspace")
	}
	if reviewLease.SessionKey == implLease.SessionKey {
		t.Fatal("the reviewer must use a fresh session")
	}
	// RV5: the reviewer prompt carries the framework read-only rule.
	reviewPrompt := fixture.promptFor(":task_review:")
	if !strings.Contains(reviewPrompt, "READ ONLY") || !strings.Contains(reviewPrompt, "read-only Git worktree") {
		t.Fatal("reviewer prompt missing the framework read-only rule")
	}
	if reviewLease.State != "finalized" {
		t.Fatalf("reviewer lease state = %q", reviewLease.State)
	}

	// Accepted review integrates ff-only before the done effect. The
	// implementation lease already reconciled; only the reviewer remains.
	fixture.scan()
	if remaining := mustLeases(t, fixture.manager); len(remaining) != 0 {
		t.Fatalf("review lease did not reconcile: %#v", remaining[0])
	}
	targetNow := runFixtureGit(t, fixture.root, "rev-parse", fixture.target)
	if targetNow != resultSHA {
		t.Fatalf("target = %s, want the accepted result %s", targetNow, resultSHA)
	}
	merges := runFixtureGit(t, fixture.root, "rev-list", "--merges", fixture.baseSHA+".."+resultSHA)
	if merges != "" {
		t.Fatalf("integration created merge commits: %s", merges)
	}
	// The full durable evidence sequence exists for the document.
	wantSequence := []string{
		facts.KindLaunchCreated, facts.KindWorkspaceReady, facts.KindProviderAdmitted, facts.KindProviderTerminal,
		facts.KindResultCaptured, facts.KindCheckCompleted, facts.KindReviewRequested,
		facts.KindLaunchCreated, facts.KindWorkspaceReady, facts.KindProviderAdmitted, facts.KindProviderTerminal,
		facts.KindReviewCompleted, facts.KindIntegrationCompleted,
	}
	kinds := normalizeAdmissionOrder(fixture.factKinds(documentID))
	if fmt.Sprint(kinds) != fmt.Sprint(wantSequence) {
		t.Fatalf("fact sequence =\n%v\nwant\n%v", kinds, wantSequence)
	}
	// The integrated feature is visible in the root checkout.
	if _, err := os.Stat(filepath.Join(fixture.root, "feature.txt")); err != nil {
		t.Fatalf("integrated feature missing from the root checkout: %v", err)
	}
}

// F2-A/F2-B: both an explicit review move and a policy redirect from done
// establish a request bound to the current finalized implementation result.
func TestIsolatedReviewRoutesBindCurrentImplementation(t *testing.T) {
	for _, requestedStatus := range []string{"review", "done"} {
		t.Run(requestedStatus, func(t *testing.T) {
			fixture := newIsolatedFixture(t, nil)
			taskPath := fixture.createTask("review route " + requestedStatus)
			documentID := documentIDFromPath(t, taskPath)
			fixture.setHook(func(h *emitCaptureHarness, key, prompt string) {
				if strings.HasPrefix(key, "orchestrator:notification:") {
					h.emitNow(key, core.Event{Kind: core.EventFinal, Done: true})
					return
				}
				lease, ok := fixture.leaseForSession(key)
				if !ok {
					return
				}
				if strings.Contains(key, ":task_implementation:") {
					view := fixture.openWorkspace(lease)
					if err := os.WriteFile(filepath.Join(view.ProviderView(), "route.txt"), []byte(requestedStatus), 0o600); err != nil {
						t.Fatal(err)
					}
					fixture.moveDocument(lease.File, requestedStatus)
				}
				h.emitNow(key, core.Event{Kind: core.EventFinal, Done: true})
			})
			fixture.scan()
			implementation := mustLeases(t, fixture.manager)[0]
			evidence, err := fixture.manager.implementationEvidence("tasks", documentID, implementation.LaunchID)
			if err != nil {
				t.Fatal(err)
			}
			fixture.scan()
			all, err := fixture.manager.documentFacts("tasks", documentID)
			if err != nil {
				t.Fatal(err)
			}
			request, ok := fixture.manager.currentReviewRequest("tasks", documentID, all)
			if !ok || request.ImplLaunch != implementation.LaunchID || request.ReviewSHA != evidence.Result.ResultSHA || request.TargetRef != evidence.Created.TargetRef || request.TargetOld != evidence.Created.TargetOld {
				t.Fatalf("current review request = %+v, implementation=%s result=%s", request, implementation.LaunchID, evidence.Result.ResultSHA)
			}
		})
	}
}

// F2-C: after R1 is rejected and a correction produces R2, only R2 may be
// dispatched to the next reviewer or integrated.
func TestReviewerNeverSelectsRejectedHistoricalResult(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	taskPath := fixture.createTask("review corrected result")
	documentID := documentIDFromPath(t, taskPath)
	implementationRound := 0
	var reviewed []string
	fixture.setHook(func(h *emitCaptureHarness, key, prompt string) {
		if strings.HasPrefix(key, "orchestrator:notification:") {
			h.emitNow(key, core.Event{Kind: core.EventFinal, Done: true})
			return
		}
		lease, ok := fixture.leaseForSession(key)
		if !ok {
			return
		}
		switch {
		case strings.Contains(key, ":task_implementation:"):
			implementationRound++
			view := fixture.openWorkspace(lease)
			if err := os.WriteFile(filepath.Join(view.ProviderView(), "feature.txt"), []byte(fmt.Sprintf("R%d", implementationRound)), 0o600); err != nil {
				t.Fatal(err)
			}
			fixture.moveDocument(lease.File, "review")
		case strings.Contains(key, ":task_review:"):
			created, err := fixture.manager.launchCreatedOf("tasks", documentID, lease.LaunchID)
			if err != nil {
				t.Fatal(err)
			}
			reviewed = append(reviewed, created.ReviewSHA)
			if len(reviewed) == 1 {
				fixture.moveDocument(lease.File, "todo")
			} else {
				fixture.moveDocument(lease.File, "done")
			}
		}
		h.emitNow(key, core.Event{Kind: core.EventFinal, Done: true})
	})
	for i := 0; i < 5; i++ {
		fixture.scan()
	}
	if len(reviewed) != 2 || reviewed[0] == reviewed[1] {
		t.Fatalf("reviewed SHAs = %v, want distinct R1 then R2", reviewed)
	}
	target := runFixtureGit(t, fixture.root, "rev-parse", fixture.target)
	if target != reviewed[1] {
		t.Fatalf("integrated target = %s, want current R2 %s", target, reviewed[1])
	}
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range evidence {
		if fact.Kind == facts.KindIntegrationCompleted && fact.ResultSHA == reviewed[0] {
			t.Fatalf("historical R1 was integrated: %+v", fact)
		}
	}
}

// A historical request is not a fallback while the current finalized result
// is awaiting its own request. Selecting merely the newest request fact would
// authorize R1 during this R2 crash window.
func TestCurrentReviewRequestRejectsLatestHistoricalRequest(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	taskPath := fixture.createTask("historical request fence")
	documentID := documentIDFromPath(t, taskPath)
	for _, round := range []struct {
		launch  string
		result  string
		request bool
	}{
		{launch: "ln-historical-r1", result: fixture.baseSHA, request: true},
		{launch: "ln-current-r2", result: runFixtureGit(t, fixture.root, "commit-tree", runFixtureGit(t, fixture.root, "rev-parse", fixture.baseSHA+"^{tree}"), "-p", fixture.baseSHA, "-m", "R2"), request: false},
	} {
		if _, err := fixture.manager.appendFact(facts.Fact{
			Kind: facts.KindLaunchCreated, Key: launchCreatedKey(round.launch),
			Doc: documentDoc("tasks", documentID), Launch: round.launch, Phase: phaseTaskImplementation,
			WorkspaceKind: execws.WorkspaceKindGitWorktree, BaseSHA: fixture.baseSHA,
			TargetRef: fixture.target, TargetOld: fixture.baseSHA, CheckSet: execws.CheckSetDigest(nil),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.manager.appendFact(facts.Fact{
			Kind: facts.KindResultCaptured, Key: resultCapturedKey(round.launch),
			Doc: documentDoc("tasks", documentID), Launch: round.launch, Phase: phaseTaskImplementation,
			WorkspaceKind: execws.WorkspaceKindGitWorktree, BaseSHA: fixture.baseSHA,
			ResultSHA: round.result, Changed: true, TargetRef: fixture.target, TargetOld: fixture.baseSHA,
		}); err != nil {
			t.Fatal(err)
		}
		if round.request {
			if _, err := fixture.manager.appendFact(facts.Fact{
				Kind: facts.KindReviewRequested, Key: reviewRequestedKey(round.launch),
				Doc: documentDoc("tasks", documentID), Launch: round.launch, Phase: phaseTaskReview,
				WorkspaceKind: execws.WorkspaceKindGitWorktree, ImplLaunch: round.launch,
				ReviewSHA: round.result, TargetRef: fixture.target, TargetOld: fixture.baseSHA,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	if request, ok := fixture.manager.currentReviewRequest("tasks", documentID, evidence); ok {
		t.Fatalf("historical request authorized the current result: %+v", request)
	}
}

// F3: changed no-review completion integrates the exact canonical result and
// records integration evidence before notification or any done effect.
func TestIsolatedDirectDoneIntegratesBeforeTerminalEffects(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	taskPath := fixture.createTask("direct isolated completion")
	document, err := ReadDocument(taskPath)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["review_required"] = false
	if err := WriteDocument(taskPath, document); err != nil {
		t.Fatal(err)
	}
	documentID := documentIDFromPath(t, taskPath)
	terminalEffectsCalled := false
	terminalEffectsBeforeIntegration := false
	fixture.manager.beforeTerminalEffects = func(lease Lease, status string) {
		terminalEffectsCalled = true
		evidence, factErr := fixture.manager.documentFacts("tasks", documentID)
		if factErr != nil {
			terminalEffectsBeforeIntegration = true
			return
		}
		_, integrated := findFact(evidence, integrationKey(evidenceResultLaunch(evidence)))
		if !integrated || runFixtureGit(t, fixture.root, "rev-parse", fixture.target) == fixture.baseSHA {
			terminalEffectsBeforeIntegration = true
		}
	}
	fixture.setHook(func(h *emitCaptureHarness, key, prompt string) {
		if strings.HasPrefix(key, "orchestrator:notification:") {
			h.emitNow(key, core.Event{Kind: core.EventFinal, Done: true})
			return
		}
		lease, ok := fixture.leaseForSession(key)
		if !ok || !strings.Contains(key, ":task_implementation:") {
			return
		}
		view := fixture.openWorkspace(lease)
		if err := os.WriteFile(filepath.Join(view.ProviderView(), "direct.txt"), []byte("integrated before done"), 0o600); err != nil {
			t.Fatal(err)
		}
		fixture.moveDocument(lease.File, "done")
		donePath := filepath.Join(fixture.cfg.StatePath("tasks", "done"), filepath.Base(taskPath))
		doneDocument, readErr := ReadDocument(donePath)
		if readErr != nil {
			t.Fatal(readErr)
		}
		updated, ok := timestampField(doneDocument, "updated_at")
		if !ok {
			t.Fatal("done document has no updated_at")
		}
		doneDocument.FrontMatter["completion_summary"] = map[string]any{
			"verdict": "completed", "outcome": "implemented", "evidence": "focused integration proof",
			"uncertainty": "none", "completed_at": updated.Format(time.RFC3339),
		}
		if err := WriteDocument(donePath, doneDocument); err != nil {
			t.Fatal(err)
		}
		h.emitNow(key, core.Event{Kind: core.EventFinal, Done: true})
	})
	fixture.scan()
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := findFact(evidence, resultCapturedKey(evidenceResultLaunch(evidence)))
	if !ok || !result.Changed {
		t.Fatalf("captured result = %+v", result)
	}
	if got := runFixtureGit(t, fixture.root, "rev-parse", fixture.target); got != fixture.baseSHA {
		t.Fatalf("target moved before reconciliation: %s", got)
	}
	fixture.scan()
	evidence, err = fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	integrated, ok := findFact(evidence, integrationKey(result.Launch))
	if !ok || integrated.ResultSHA != result.ResultSHA {
		t.Fatalf("integration evidence = %+v, want %s", integrated, result.ResultSHA)
	}
	if got := runFixtureGit(t, fixture.root, "rev-parse", fixture.target); got != result.ResultSHA {
		t.Fatalf("target = %s, want canonical result %s", got, result.ResultSHA)
	}
	if _, err := os.Stat(filepath.Join(fixture.root, "direct.txt")); err != nil {
		t.Fatalf("root checkout lacks integrated change: %v", err)
	}
	if merges := runFixtureGit(t, fixture.root, "rev-list", "--merges", fixture.baseSHA+".."+result.ResultSHA); merges != "" {
		t.Fatalf("direct integration created a merge commit: %s", merges)
	}
	if !terminalEffectsCalled || terminalEffectsBeforeIntegration {
		t.Fatal("terminal hooks/notification boundary ran before integration evidence and target movement")
	}
}

// F3 continuation: a no-op continuation launch adds no new content, so its
// own Changed is false, yet its canonical ResultSHA is a prior result that the
// frozen target never received. Integration eligibility is target-relative
// (ResultSHA != TargetOld), so done must still integrate before terminal
// effects; Changed alone would silently drop the round one work.
func TestIsolatedNoOpContinuationDirectDoneIntegratesResult(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	targetOld := fixture.baseSHA
	taskPath := fixture.createTask("no-op continuation direct done")
	document, err := ReadDocument(taskPath)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["review_required"] = false
	if err := WriteDocument(taskPath, document); err != nil {
		t.Fatal(err)
	}
	documentID := documentIDFromPath(t, taskPath)

	implementationRounds := 0
	var roundOne, roundTwo string
	terminalEffectsCalled := false
	terminalEffectsBeforeIntegration := false
	fixture.manager.beforeTerminalEffects = func(lease Lease, status string) {
		terminalEffectsCalled = true
		evidence, factErr := fixture.manager.documentFacts("tasks", documentID)
		if factErr != nil {
			terminalEffectsBeforeIntegration = true
			return
		}
		integrated, ok := findFact(evidence, integrationKey(roundTwo))
		if !ok || integrated.ResultSHA == "" || integrated.ResultSHA == targetOld ||
			runFixtureGit(t, fixture.root, "rev-parse", fixture.target) != integrated.ResultSHA {
			terminalEffectsBeforeIntegration = true
		}
	}
	fixture.setHook(func(h *emitCaptureHarness, key, prompt string) {
		if strings.HasPrefix(key, "orchestrator:notification:") {
			h.emitNow(key, core.Event{Kind: core.EventFinal, Done: true})
			return
		}
		lease, ok := fixture.leaseForSession(key)
		if !ok || !strings.Contains(key, ":task_implementation:") {
			return
		}
		implementationRounds++
		switch implementationRounds {
		case 1:
			roundOne = lease.LaunchID
			view := fixture.openWorkspace(lease)
			if err := os.WriteFile(filepath.Join(view.ProviderView(), "continuation.txt"), []byte("round one change"), 0o600); err != nil {
				t.Fatal(err)
			}
			// Round one continues later: the task returns to todo without a
			// terminal transition.
			fixture.moveDocument(lease.File, "todo")
		case 2:
			roundTwo = lease.LaunchID
			// No content change: the continuation is a no-op relative to the
			// round one result it started from.
			fixture.moveDocument(lease.File, "done")
			donePath := filepath.Join(fixture.cfg.StatePath("tasks", "done"), filepath.Base(taskPath))
			doneDocument, readErr := ReadDocument(donePath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			updated, ok := timestampField(doneDocument, "updated_at")
			if !ok {
				t.Fatal("done document has no updated_at")
			}
			doneDocument.FrontMatter["completion_summary"] = map[string]any{
				"verdict": "completed", "outcome": "continued the prior result without new changes",
				"evidence": "focused continuation integration proof", "uncertainty": "none",
				"completed_at": updated.Format(time.RFC3339),
			}
			if err := WriteDocument(donePath, doneDocument); err != nil {
				t.Fatal(err)
			}
		}
		h.emitNow(key, core.Event{Kind: core.EventFinal, Done: true})
	})

	// ROUND 1: an isolated launch from A creates a change, captures R1, and
	// moves the task back to todo. The target must still be A and R1 must not
	// be integrated yet.
	fixture.scan()
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	roundOneResult, ok := findFact(evidence, resultCapturedKey(roundOne))
	if !ok {
		t.Fatal("round one captured no result")
	}
	if roundOneResult.ResultSHA == targetOld {
		t.Fatal("round one result equals the original target")
	}
	if !roundOneResult.Changed {
		t.Fatalf("round one result must be changed: %+v", roundOneResult)
	}
	if got := runFixtureGit(t, fixture.root, "rev-parse", fixture.target); got != targetOld {
		t.Fatalf("round one moved the target to %s before any completion", got)
	}
	if _, integrated := findFact(evidence, integrationKey(roundOne)); integrated {
		t.Fatal("round one integrated without a completion request")
	}

	// ROUND 2: the continuation launches from R1 through isolatedStartSHA,
	// makes no content change, captures Changed=false with ResultSHA still
	// R1, and requests done with review disabled.
	fixture.scan()
	evidence, err = fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	roundTwoCreated, err := fixture.manager.launchCreatedOf("tasks", documentID, roundTwo)
	if err != nil {
		t.Fatal(err)
	}
	roundTwoResult, ok := findFact(evidence, resultCapturedKey(roundTwo))
	if !ok {
		t.Fatal("round two captured no result")
	}
	if roundTwoCreated.BaseSHA != roundOneResult.ResultSHA {
		t.Fatalf("round two base = %s, want round one result %s", roundTwoCreated.BaseSHA, roundOneResult.ResultSHA)
	}
	if roundTwoCreated.TargetOld != targetOld {
		t.Fatalf("round two target_old = %s, want frozen %s", roundTwoCreated.TargetOld, targetOld)
	}
	if roundTwoResult.Changed {
		t.Fatalf("round two must be an unchanged continuation: %+v", roundTwoResult)
	}
	if roundTwoResult.ResultSHA != roundOneResult.ResultSHA {
		t.Fatalf("round two result = %s, want the round one canonical result %s", roundTwoResult.ResultSHA, roundOneResult.ResultSHA)
	}
	if roundTwoResult.ResultSHA == roundTwoCreated.TargetOld {
		t.Fatal("this regression requires ResultSHA != TargetOld")
	}
	if got := runFixtureGit(t, fixture.root, "rev-parse", fixture.target); got != targetOld {
		t.Fatalf("round two moved the target to %s before reconciliation", got)
	}

	// The done reconciliation integrates the exact ResultSHA before any
	// terminal effect; Changed=false would have skipped this entirely.
	fixture.scan()
	evidence, err = fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	integrated, ok := findFact(evidence, integrationKey(roundTwo))
	if !ok || integrated.ResultSHA != roundOneResult.ResultSHA {
		t.Fatalf("integration evidence = %+v, want result %s", integrated, roundOneResult.ResultSHA)
	}
	if got := runFixtureGit(t, fixture.root, "rev-parse", fixture.target); got != roundOneResult.ResultSHA {
		t.Fatalf("target = %s, want integrated result %s", got, roundOneResult.ResultSHA)
	}
	if _, err := os.Stat(filepath.Join(fixture.root, "continuation.txt")); err != nil {
		t.Fatalf("root checkout lacks the round one change: %v", err)
	}
	if merges := runFixtureGit(t, fixture.root, "rev-list", "--merges", targetOld+".."+roundOneResult.ResultSHA); merges != "" {
		t.Fatalf("direct integration created a merge commit: %s", merges)
	}
	if !terminalEffectsCalled || terminalEffectsBeforeIntegration {
		t.Fatal("terminal hooks/notification boundary ran before integration evidence and target movement")
	}
	if _, err := ReadDocument(filepath.Join(fixture.cfg.StatePath("tasks", "done"), filepath.Base(taskPath))); err != nil {
		t.Fatalf("task did not complete done: %v", err)
	}
	if implementationRounds != 2 {
		t.Fatalf("implementation rounds = %d, want exactly two", implementationRounds)
	}
}

func evidenceResultLaunch(evidence []facts.Fact) string {
	for index := len(evidence) - 1; index >= 0; index-- {
		if evidence[index].Kind == facts.KindResultCaptured {
			return evidence[index].Launch
		}
	}
	return ""
}
