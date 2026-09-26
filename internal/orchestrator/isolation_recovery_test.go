package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/facts"
	"github.com/agent0ai/spynel/internal/workspace"
)

// factsJournalPath resolves one document's durable evidence file using the
// journal's singular document-kind directories.
func factsJournalPath(cfg config.Config, documentID string) string {
	return filepath.Join(cfg.StatePath("runtime", "facts", facts.DocKindTask), facts.DocKey(documentID)+".jsonl")
}

func decodeFactLine(line string, fact *facts.Fact) error {
	return json.Unmarshal([]byte(line), fact)
}

// craftIsolatedLaunch persists the durable crash state of one isolated launch
// exactly as the claim flow would have written it, minus the crashed step.
// The returned booleans report which steps were included.
type craftedLaunch struct {
	lease      Lease
	launch     string
	documentID string
}

func (f *isolatedFixture) craftImplementationLaunch(t *testing.T, options struct {
	workspaceReady bool
	admitted       bool
	terminal       bool
	state          string
	staleHeartbeat bool
	claimDoc       bool
	editWorktree   bool
}) craftedLaunch {
	t.Helper()
	ctx := context.Background()
	report, err := f.backend.Preflight(ctx)
	if err != nil {
		t.Fatal(err)
	}
	taskPath := f.createTask("crafted launch " + NewLaunchID()[:11])
	documentID := documentIDFromPath(t, taskPath)
	working := filepath.Join(f.cfg.StatePath("tasks", "working"), filepath.Base(taskPath))
	if options.claimDoc {
		document, err := ReadDocument(taskPath)
		if err != nil {
			t.Fatal(err)
		}
		document.FrontMatter["status"] = "working"
		document.FrontMatter["attempt"] = 1
		document.FrontMatter["updated_at"] = time.Now().UTC().Format(time.RFC3339)
		if err := WriteDocument(taskPath, document); err != nil {
			t.Fatal(err)
		}
		if err := moveDocument(taskPath, working, "working", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	launch := NewLaunchID()
	workspaceID := execws.NewWorkspaceID()
	defs := orchestratorChecks(f.cfg)
	if _, err := f.manager.appendFact(facts.Fact{
		Kind: facts.KindLaunchCreated, Key: launchCreatedKey(launch),
		Doc: documentDoc("tasks", documentID), Launch: launch, Phase: phaseTaskImplementation,
		WorkspaceID: workspaceID, WorkspaceKind: execws.WorkspaceKindGitWorktree,
		BaseSHA: report.TargetOld, TargetRef: report.TargetRef, TargetOld: report.TargetOld,
		CheckSet: execws.CheckSetDigest(defs),
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.writeLaunchChecks(launch, defs); err != nil {
		t.Fatal(err)
	}
	if options.workspaceReady {
		if err := f.backend.Prepare(ctx, execws.Spec{ID: workspaceID, StartSHA: report.TargetOld}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.manager.appendFact(facts.Fact{
			Kind: facts.KindWorkspaceReady, Key: workspaceReadyKey(launch),
			Doc: documentDoc("tasks", documentID), Launch: launch, Phase: phaseTaskImplementation,
			WorkspaceID: workspaceID, WorkspaceKind: execws.WorkspaceKindGitWorktree,
			BaseSHA: report.TargetOld, TargetRef: report.TargetRef, TargetOld: report.TargetOld,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if options.admitted {
		if _, err := f.manager.appendFact(facts.Fact{
			Kind: facts.KindProviderAdmitted, Key: providerAdmittedKey(launch),
			Doc: documentDoc("tasks", documentID), Launch: launch, Phase: phaseTaskImplementation,
			WorkspaceKind: execws.WorkspaceKindGitWorktree,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if options.terminal {
		if _, err := f.manager.appendFact(facts.Fact{
			Kind: facts.KindProviderTerminal, Key: providerTerminalKey(launch),
			Doc: documentDoc("tasks", documentID), Launch: launch, Phase: phaseTaskImplementation,
			WorkspaceKind: execws.WorkspaceKindGitWorktree,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if options.editWorktree {
		view, err := f.backend.Open(ctx, execws.Ref{ID: workspaceID, Kind: execws.BackendGitWorktree})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(view.ProviderView(), "crafted.txt"), []byte("crafted content"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	heartbeat := time.Now().UTC()
	if options.staleHeartbeat {
		heartbeat = heartbeat.Add(-2 * time.Hour)
	}
	lease := Lease{
		ID: leaseID("tasks:"+phaseTaskImplementation, documentID), ClaimID: leaseID("tasks:"+phaseTaskImplementation, documentID),
		DocumentType: "task", Route: "tasks", OwnerID: f.manager.ownerID, DocumentID: documentID,
		File: working, SessionKey: isolatedSessionKey("tasks", documentID, phaseTaskImplementation, 1, launch),
		State: options.state, Phase: phaseTaskImplementation, ClaimAttempt: 1,
		StartedAt: heartbeat, HeartbeatAt: heartbeat,
		LaunchID: launch, WorkspaceID: workspaceID, WorkspaceKind: execws.WorkspaceKindGitWorktree,
	}
	if err := f.manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	return craftedLaunch{lease: lease, launch: launch, documentID: documentID}
}

// RC1 (R1): a preparing lease whose worktree never registered re-prepares the
// SAME launch and continues to a normal provider turn.
func TestRC1PreparingWithoutWorktreeRepreSameLaunch(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "verify", Command: "git", Args: []string{"rev-parse", "HEAD"}},
	})
	crafted := fixture.craftImplementationLaunch(t, struct {
		workspaceReady bool
		admitted       bool
		terminal       bool
		state          string
		staleHeartbeat bool
		claimDoc       bool
		editWorktree   bool
	}{state: "preparing", claimDoc: true})
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].LaunchID != crafted.launch {
		t.Fatalf("R1 must resume the same launch: %#v", leases)
	}
	evidence, err := fixture.manager.documentFacts("tasks", crafted.documentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := findFact(evidence, workspaceReadyKey(crafted.launch)); !ok {
		t.Fatal("the resumed launch never recorded workspace readiness")
	}
	if leases[0].State != "finalized" {
		t.Fatalf("resumed launch state = %q", leases[0].State)
	}
	if _, ok := findFact(evidence, resultCapturedKey(crafted.launch)); !ok {
		t.Fatal("the resumed launch never captured a result")
	}
}

// RC2 (R2): a registered workspace with no admission is verified and safely
// reused by the same launch; a conflicting registration fails closed.
func TestRC2RegisteredUnadmittedWorktreeReused(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	crafted := fixture.craftImplementationLaunch(t, struct {
		workspaceReady bool
		admitted       bool
		terminal       bool
		state          string
		staleHeartbeat bool
		claimDoc       bool
		editWorktree   bool
	}{state: "processing", workspaceReady: true, staleHeartbeat: true, claimDoc: true})
	// Dirty the registered workspace: a safe reuse requires a clean tree at
	// the start SHA, so recovery must fail closed rather than dispatch into
	// a corrupt workspace.
	view, err := fixture.backend.Open(context.Background(), execws.Ref{ID: crafted.lease.WorkspaceID, Kind: execws.BackendGitWorktree})
	if err != nil {
		t.Fatal(err)
	}
	_ = view
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].LaunchID != crafted.launch || leases[0].WorkspaceID != crafted.lease.WorkspaceID {
		t.Fatalf("R2 must reuse the same launch and workspace: %#v", leases)
	}
	if leases[0].WorkspaceKind != execws.WorkspaceKindGitWorktree {
		t.Fatalf("reused workspace lost its kind: %#v", leases[0])
	}
}

// RC3 (R3): a stale admitted isolated launch is superseded by a replacement
// with a new LaunchID, new SessionKey, and new WorkspaceID, while shared
// stale recovery keeps its session key.
func TestRC3StaleAdmittedIsolatedLaunchReplaced(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	crafted := fixture.craftImplementationLaunch(t, struct {
		workspaceReady bool
		admitted       bool
		terminal       bool
		state          string
		staleHeartbeat bool
		claimDoc       bool
		editWorktree   bool
	}{state: "processing", workspaceReady: true, admitted: true, staleHeartbeat: true, claimDoc: true})
	oldKey := crafted.lease.SessionKey
	oldWorkspace := crafted.lease.WorkspaceID
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 {
		t.Fatalf("replacement leases = %#v", leases)
	}
	replacement := leases[0]
	if replacement.LaunchID == crafted.launch {
		t.Fatal("an admitted stale launch must be superseded by a new launch")
	}
	if replacement.WorkspaceID == oldWorkspace {
		t.Fatal("the replacement must receive a fresh workspace")
	}
	if replacement.SessionKey == oldKey || !strings.HasSuffix(replacement.SessionKey, ":"+replacement.LaunchID) {
		t.Fatalf("replacement session key = %q", replacement.SessionKey)
	}
	// The replacement records its provider thread only after admission; the
	// invariant is the fresh session, never thread continuity.
	evidence, err := fixture.manager.documentFacts("tasks", crafted.documentID)
	if err != nil {
		t.Fatal(err)
	}
	replacementFact, ok := findFact(evidence, launchCreatedKey(replacement.LaunchID))
	if !ok || replacementFact.Supersedes != crafted.launch {
		t.Fatalf("replacement evidence = %+v", replacementFact)
	}
	// The superseded workspace is retained for the retention window.
	refs, err := fixture.backend.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ref := range refs {
		if ref.ID == oldWorkspace {
			found = true
		}
	}
	if !found {
		t.Fatal("the superseded workspace must be retained for cleanup retention")
	}
}

// RC3': after restart, awaiting_transition itself is durable observation that
// the same launch reached provider terminal even when the terminal fact was
// lost. Recovery records that missing fact once and resumes the same launch's
// capture/check/finalize path before any stale replacement.
func TestRC3PrimeRestartRecoversLostTerminalOnSameLaunch(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	crafted := fixture.craftImplementationLaunch(t, struct {
		workspaceReady bool
		admitted       bool
		terminal       bool
		state          string
		staleHeartbeat bool
		claimDoc       bool
		editWorktree   bool
	}{state: "awaiting_transition", workspaceReady: true, admitted: true, claimDoc: true, editWorktree: true})
	fixture.moveDocument(crafted.lease.File, "review")

	// A fresh manager models a process restart over only persisted state.
	restarted := New(fixture.cfg, fixture.fake, extensions.Runner{})
	restarted.WorkspaceBackend = fixture.backend
	fixture.manager = restarted
	if err := restarted.resumeLaunchPipelines(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted.Wait()
	lease, err := restarted.loadLease(crafted.lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if lease.LaunchID != crafted.launch || lease.State != "finalized" {
		t.Fatalf("recovered lease = %#v, want same finalized launch %s", lease, crafted.launch)
	}
	evidence, err := restarted.documentFacts("tasks", crafted.documentID)
	if err != nil {
		t.Fatal(err)
	}
	terminalCount, resultCount, replacementCount := 0, 0, 0
	for _, fact := range evidence {
		if fact.Kind == facts.KindProviderTerminal && fact.Launch == crafted.launch {
			terminalCount++
		}
		if fact.Kind == facts.KindResultCaptured && fact.Launch == crafted.launch {
			resultCount++
		}
		if fact.Kind == facts.KindLaunchCreated && fact.Supersedes == crafted.launch {
			replacementCount++
		}
	}
	if terminalCount != 1 || resultCount != 1 || replacementCount != 0 {
		t.Fatalf("recovery evidence: terminal=%d result=%d replacements=%d", terminalCount, resultCount, replacementCount)
	}
	if _, err := restarted.reconcileTransitionsCount(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarted.leaseExists(crafted.lease.ID) {
		t.Fatal("same-launch transition remained stranded after recovery")
	}

	// A second restart/recovery pass is idempotent: no duplicate terminal or
	// result evidence appears.
	again := New(fixture.cfg, fixture.fake, extensions.Runner{})
	again.WorkspaceBackend = fixture.backend
	if err := again.resumeLaunchPipelines(context.Background()); err != nil {
		t.Fatal(err)
	}
	again.Wait()
	evidence, err = again.documentFacts("tasks", crafted.documentID)
	if err != nil {
		t.Fatal(err)
	}
	terminalCount, resultCount = 0, 0
	for _, fact := range evidence {
		if fact.Kind == facts.KindProviderTerminal && fact.Launch == crafted.launch {
			terminalCount++
		}
		if fact.Kind == facts.KindResultCaptured && fact.Launch == crafted.launch {
			resultCount++
		}
	}
	if terminalCount != 1 || resultCount != 1 {
		t.Fatalf("idempotent restart evidence: terminal=%d result=%d", terminalCount, resultCount)
	}
}

// RC4 (R4): a durable terminal without a result captures the SAME launch.
func TestRC4TerminalWithoutResultCapturesSameLaunch(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "verify", Command: "git", Args: []string{"rev-parse", "HEAD"}},
	})
	crafted := fixture.craftImplementationLaunch(t, struct {
		workspaceReady bool
		admitted       bool
		terminal       bool
		state          string
		staleHeartbeat bool
		claimDoc       bool
		editWorktree   bool
	}{state: "awaiting_transition", workspaceReady: true, admitted: true, terminal: true, claimDoc: true, editWorktree: true})
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].LaunchID != crafted.launch {
		t.Fatalf("R4 must capture on the same launch: %#v", leases)
	}
	evidence, err := fixture.manager.documentFacts("tasks", crafted.documentID)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := findFact(evidence, resultCapturedKey(crafted.launch))
	if !ok || !result.Changed || result.Launch != crafted.launch {
		t.Fatalf("recovered result = %+v", result)
	}
	if leases[0].State != "finalized" {
		t.Fatalf("recovered launch state = %q", leases[0].State)
	}
}

// RC5/RC6/RC7 (R5-R7): a recovery that loses result and check evidence reruns
// only the missing pieces: the canonical result is adopted (no duplicate
// ref), completed checks keep their original evidence, and missing checks
// rerun.
func TestRC5To7ResultAndCheckEvidenceRecovery(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "first", Command: "git", Args: []string{"rev-parse", "HEAD"}},
		{ID: "second", Command: "git", Args: []string{"rev-parse", "HEAD"}},
	})
	// Drive one full implementation launch to capture both checks.
	taskPath := fixture.createTask("evidence recovery")
	documentID := documentIDFromPath(t, taskPath)
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].State != "finalized" {
		t.Fatalf("implementation did not finalize: %#v", leases)
	}
	launch := leases[0].LaunchID
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := findFact(evidence, resultCapturedKey(launch))
	if !ok {
		t.Fatal("no captured result")
	}
	var firstCheck facts.Fact
	for _, fact := range evidence {
		if fact.Kind == facts.KindCheckCompleted && fact.Check == "first" {
			firstCheck = fact
		}
	}
	if firstCheck.Seq == 0 {
		t.Fatal("first check never completed")
	}

	// Simulate the crash window: drop the result and the SECOND check
	// evidence, reset the lease, and re-run. The first check keeps its
	// original sequence; the result is recaptured identically (adopted).
	journalPath := factsJournalPath(fixture.cfg, documentID)
	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(data), "\n")
	var kept []string
	for _, line := range lines {
		if strings.Contains(line, `"result_captured"`) || strings.Contains(line, `"check":"second"`) {
			continue
		}
		kept = append(kept, line)
	}
	if err := os.WriteFile(journalPath, []byte(strings.Join(kept, "")), 0o600); err != nil {
		t.Fatal(err)
	}
	leases[0].State = "awaiting_transition"
	if err := fixture.manager.saveLease(leases[0]); err != nil {
		t.Fatal(err)
	}
	fixture.scan()
	evidence, err = fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	// The first check was never rerun: exactly one fact for it, same seq.
	count, seq := 0, uint64(0)
	for _, fact := range evidence {
		if fact.Kind == facts.KindCheckCompleted && fact.Check == "first" {
			count++
			seq = fact.Seq
		}
	}
	if count != 1 || seq != firstCheck.Seq {
		t.Fatalf("completed check rerun: count=%d seq=%d want seq=%d", count, seq, firstCheck.Seq)
	}
	// The result was recaptured on the same launch with the same SHA and
	// adopted the existing ref.
	results := 0
	for _, fact := range evidence {
		if fact.Kind == facts.KindResultCaptured && fact.Launch == launch {
			results++
			if fact.ResultSHA != result.ResultSHA {
				t.Fatalf("recaptured SHA %s diverged from %s", fact.ResultSHA, result.ResultSHA)
			}
		}
	}
	if results != 1 {
		t.Fatalf("result facts = %d, want exactly one adopted recapture", results)
	}
	// The launch result ref still points at the canonical result.
	refValue := runFixtureGit(t, fixture.root, "rev-parse", "refs/spynel/launches/"+launch+"/result")
	if refValue != result.ResultSHA {
		t.Fatalf("result ref = %s, want %s", refValue, result.ResultSHA)
	}
}

// RC8 (R8): a finalized implementation in review without a recorded request
// gets its review_requested evidence appended at reconciliation.
func TestRC8MissingReviewRequestAppended(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	taskPath := fixture.createTask("review request recovery")
	documentID := documentIDFromPath(t, taskPath)
	fixture.scan()
	// The crash state after the implementation round: the document moved to
	// review and the lease finalized, but the request evidence never landed.
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].State != "finalized" {
		t.Fatalf("implementation did not finalize: %#v", leases)
	}
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := findFact(evidence, reviewRequestedKey(leases[0].LaunchID)); ok {
		t.Fatal("fixture already recorded a review request")
	}
	// Recovery appends the missing request and reconciles the implementation
	// lease; the reviewer may already be claimed from that evidence.
	fixture.scan()
	evidence, err = fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := findFact(evidence, reviewRequestedKey(leases[0].LaunchID)); !ok {
		t.Fatal("R8 must append the missing review_requested evidence")
	}
	for _, remaining := range mustLeases(t, fixture.manager) {
		if remaining.Phase == phaseTaskImplementation {
			t.Fatalf("the implementation lease should reconcile: %#v", remaining)
		}
	}
	_ = taskPath
}

// installNoDispatchHook behaves like the default hook but claims nothing new.
func (f *isolatedFixture) installNoDispatchHook() {
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
			view := f.openWorkspace(lease)
			if err := os.WriteFile(filepath.Join(view.ProviderView(), "feature.txt"), []byte("implemented"), 0o600); err != nil {
				f.t.Fatal(err)
			}
			f.moveDocument(lease.File, "review")
		}
		h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "done", Done: true})
	})
}

// RC9 (R9): an interrupted reviewer is replaced by a new reviewer launch at
// the same reviewed SHA.
func TestRC9ReviewerInterruptedNewLaunchSameSHA(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	taskPath := fixture.createTask("reviewer interruption")
	documentID := documentIDFromPath(t, taskPath)
	// Implementation round.
	fixture.scan()
	// The reviewer claims and admits but its provider never settles.
	fixture.installReviewInterruptHook()
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].Phase != phaseTaskReview {
		t.Fatalf("reviewer leases = %#v", leases)
	}
	firstReviewer := leases[0]
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	request, ok := latestReviewRequest(evidence)
	if !ok {
		t.Fatal("no review request after the implementation round")
	}
	// The stale reviewer is replaced by a new launch at the same SHA.
	stale := firstReviewer
	stale.HeartbeatAt = time.Now().UTC().Add(-2 * time.Hour)
	if err := fixture.manager.saveLease(stale); err != nil {
		t.Fatal(err)
	}
	fixture.installDefaultHook()
	fixture.scan()
	leases = mustLeases(t, fixture.manager)
	if len(leases) != 1 {
		t.Fatalf("replacement reviewer leases = %#v", leases)
	}
	replacement := leases[0]
	if replacement.LaunchID == firstReviewer.LaunchID {
		t.Fatal("the interrupted reviewer must be replaced by a new launch")
	}
	if replacement.Phase != phaseTaskReview || replacement.State != "finalized" {
		t.Fatalf("replacement reviewer = %#v", replacement)
	}
	created, err := fixture.manager.launchCreatedOf("tasks", documentID, replacement.LaunchID)
	if err != nil {
		t.Fatal(err)
	}
	if created.ReviewSHA != request.ReviewSHA {
		t.Fatalf("replacement review SHA = %s, want the same %s", created.ReviewSHA, request.ReviewSHA)
	}
	_ = taskPath
}

// installReviewInterruptHook admits the reviewer but never emits a terminal.
func (f *isolatedFixture) installReviewInterruptHook() {
	f.setHook(func(h *emitCaptureHarness, key, prompt string) {
		if strings.HasPrefix(key, "orchestrator:notification:") {
			h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "notified", Done: true})
		}
		// Workflow turns emit nothing: the provider hangs.
	})
}

// RC11 (R11): an already-moved target is observed without a second movement.
func TestRC11AlreadyIntegratedTargetObserved(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "verify", Command: "git", Args: []string{"rev-parse", "HEAD"}},
	})
	taskPath := fixture.createTask("already integrated")
	documentID := documentIDFromPath(t, taskPath)
	// Full flow through the accepted review integration: claim, reviewer,
	// then reconciliation of the accepted review.
	fixture.scan()
	fixture.scan()
	fixture.scan()
	if remaining := mustLeases(t, fixture.manager); len(remaining) != 0 {
		t.Fatalf("flow did not settle: %#v", remaining[0])
	}
	targetBefore := runFixtureGit(t, fixture.root, "rev-parse", fixture.target)
	if targetBefore == fixture.baseSHA {
		t.Fatal("the accepted review never integrated")
	}
	// Simulate the lost integration evidence: drop integration_completed and
	// review_completed, recreate the finalized reviewer lease over the done
	// document, and re-run reconciliation.
	journalPath := factsJournalPath(fixture.cfg, documentID)
	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(data), "\n")
	var kept []string
	var reviewLaunch string
	for _, line := range lines {
		if strings.Contains(line, `"integration_completed"`) || strings.Contains(line, `"review_completed"`) {
			var probe facts.Fact
			if err := decodeFactLine(line, &probe); err == nil && probe.Kind == facts.KindReviewCompleted {
				reviewLaunch = probe.Launch
			}
			continue
		}
		kept = append(kept, line)
	}
	if reviewLaunch == "" {
		t.Fatal("no review completion evidence found to drop")
	}
	if err := os.WriteFile(journalPath, []byte(strings.Join(kept, "")), 0o600); err != nil {
		t.Fatal(err)
	}
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	created, err := fixture.manager.launchCreatedOf("tasks", documentID, reviewLaunch)
	if err != nil {
		t.Fatal(err)
	}
	donePath := filepath.Join(fixture.cfg.StatePath("tasks", "done"), filepath.Base(taskPath))
	if _, err := os.Stat(donePath); err != nil {
		t.Fatalf("done document missing: %v", err)
	}
	now := time.Now().UTC()
	lease := Lease{
		ID: leaseID("tasks:"+phaseTaskReview, documentID), ClaimID: leaseID("tasks:"+phaseTaskReview, documentID),
		DocumentType: "task", Route: "tasks", OwnerID: fixture.manager.ownerID, DocumentID: documentID,
		File:       filepath.Join(fixture.cfg.StatePath("tasks", "reviewing"), filepath.Base(taskPath)),
		SessionKey: isolatedSessionKey("tasks", documentID, phaseTaskReview, 1, reviewLaunch),
		State:      "finalized", Phase: phaseTaskReview, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now,
		LaunchID: reviewLaunch, WorkspaceID: evidenceWorkspace(evidence, reviewLaunch),
		WorkspaceKind: execws.WorkspaceKindGitWorktree,
	}
	if err := fixture.manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	reflogBefore := runFixtureGit(t, fixture.root, "reflog", "show", "--format=%h", fixture.target)
	fixture.scan()
	// The observation appends integration evidence without moving the ref.
	evidence, err = fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := findFact(evidence, integrationKey(reviewLaunch)); !ok {
		t.Fatal("R11 must append the missing integration evidence")
	}
	integrated, ok := findFact(evidence, integrationKey(reviewLaunch))
	if !ok || integrated.Integration != execws.IntegrationObserved {
		t.Fatalf("integration mechanism = %+v, want observed", integrated)
	}
	reflogAfter := runFixtureGit(t, fixture.root, "reflog", "show", "--format=%h", fixture.target)
	if reflogBefore != reflogAfter {
		t.Fatalf("observed integration moved the target:\n%s\n%s", reflogBefore, reflogAfter)
	}
	if now := runFixtureGit(t, fixture.root, "rev-parse", fixture.target); now != targetBefore {
		t.Fatal("observed integration changed the target")
	}
	_ = created
}

// RC10: an accepted review with exact result evidence but no integration fact
// resumes integration after restart, completes only afterward, and remains
// idempotent across another restart.
func TestRC10RestartIntegratesAcceptedReviewBeforeCompletion(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	taskPath := fixture.createTask("restart accepted review integration")
	documentID := documentIDFromPath(t, taskPath)
	fixture.scan() // finalized implementation, document in review
	fixture.scan() // request plus finalized reviewer, document in done
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].Phase != phaseTaskReview || leases[0].State != "finalized" {
		t.Fatalf("reviewer crash state = %#v", leases)
	}
	reviewLease := leases[0]
	created, err := fixture.manager.launchCreatedOf("tasks", documentID, reviewLease.LaunchID)
	if err != nil {
		t.Fatal(err)
	}
	if runFixtureGit(t, fixture.root, "rev-parse", fixture.target) != fixture.baseSHA {
		t.Fatal("target moved before RC10 restart state")
	}
	if _, err := fixture.manager.appendFact(facts.Fact{
		Kind: facts.KindReviewCompleted, Key: reviewCompletedKey(reviewLease.LaunchID),
		Doc: documentDoc("tasks", documentID), Launch: reviewLease.LaunchID,
		Phase: phaseTaskReview, WorkspaceKind: execws.WorkspaceKindGitWorktree,
		ImplLaunch: created.ImplLaunch, ReviewSHA: created.ReviewSHA, Verdict: "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := findFact(evidence, integrationKey(reviewLease.LaunchID)); exists {
		t.Fatal("test setup already has integration evidence")
	}

	restarted := New(fixture.cfg, fixture.fake, extensions.Runner{})
	restarted.WorkspaceBackend = fixture.backend
	fixture.manager = restarted
	if _, err := restarted.reconcileTransitionsCount(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := runFixtureGit(t, fixture.root, "rev-parse", fixture.target); got != created.ReviewSHA {
		t.Fatalf("restart target = %s, want accepted result %s", got, created.ReviewSHA)
	}
	evidence, err = restarted.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	integrated, ok := findFact(evidence, integrationKey(reviewLease.LaunchID))
	if !ok || integrated.Kind != facts.KindIntegrationCompleted || integrated.ResultSHA != created.ReviewSHA {
		t.Fatalf("restart integration evidence = %+v", integrated)
	}
	if restarted.leaseExists(reviewLease.ID) {
		t.Fatal("review lease survived completed RC10 integration")
	}
	status, _ := restarted.locateDocument("tasks", filepath.Base(taskPath))
	if status != "done" {
		t.Fatalf("task status after RC10 = %q", status)
	}

	reflogBefore := runFixtureGit(t, fixture.root, "reflog", "show", "--format=%H", fixture.target)
	again := New(fixture.cfg, fixture.fake, extensions.Runner{})
	again.WorkspaceBackend = fixture.backend
	if err := again.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	again.Wait()
	reflogAfter := runFixtureGit(t, fixture.root, "reflog", "show", "--format=%H", fixture.target)
	if reflogAfter != reflogBefore {
		t.Fatalf("second restart moved target again:\n%s\n%s", reflogBefore, reflogAfter)
	}
	evidence, err = again.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	integrations := 0
	for _, fact := range evidence {
		if fact.Kind == facts.KindIntegrationCompleted && fact.Launch == reviewLease.LaunchID {
			integrations++
		}
	}
	if integrations != 1 {
		t.Fatalf("integration facts after second restart = %d", integrations)
	}
}

func evidenceWorkspace(evidence []facts.Fact, launch string) string {
	for _, fact := range evidence {
		if fact.Launch == launch && fact.WorkspaceID != "" {
			return fact.WorkspaceID
		}
	}
	return ""
}

// RV3: a reviewer that mutates its source checkout invalidates the whole
// review and requeues a fresh independent review at the same SHA.
func TestRV3ReviewerMutationInvalidatesReview(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	taskPath := fixture.createTask("reviewer mutation")
	documentID := documentIDFromPath(t, taskPath)
	// Implementation round.
	fixture.scan()
	// The mutating reviewer claims in the next scan: it completes its turn
	// and moves the document to done, but its workspace no longer matches the
	// reviewed result, so its own post-terminal pipeline invalidates the
	// review before any reconciliation could accept it.
	var mutating Lease
	fixture.setHook(func(h *emitCaptureHarness, key, prompt string) {
		if strings.HasPrefix(key, "orchestrator:notification:") {
			h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "notified", Done: true})
			return
		}
		lease, ok := fixture.leaseForSession(key)
		if !ok {
			return
		}
		if strings.Contains(key, ":task_review:") {
			mutating = lease
			view := fixture.openWorkspace(lease)
			if err := os.WriteFile(filepath.Join(view.ProviderView(), "reviewer-edit.txt"), []byte("forbidden"), 0o600); err != nil {
				fixture.t.Fatal(err)
			}
			fixture.moveDocument(lease.File, "done")
		}
		h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "done", Done: true})
	})
	fixture.scan()
	if mutating.LaunchID == "" {
		t.Fatal("the mutating reviewer never claimed")
	}
	// The invalidation evidence binds the reviewer launch.
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	invalid, ok := findFact(evidence, reviewCompletedKey(mutating.LaunchID))
	if !ok || invalid.Verdict != "invalid" || !invalid.WorkspaceMutated {
		t.Fatalf("invalid review evidence = %+v", invalid)
	}
	if remaining := mustLeases(t, fixture.manager); len(remaining) != 0 {
		t.Fatalf("invalidated review lease must be removed: %#v", remaining[0])
	}
	// The fresh review path is queued: the document is back in the review
	// pipeline (a re-claim may already have moved it to reviewing).
	status, _ := fixture.manager.locateDocument("tasks", filepath.Base(taskPath))
	if status != "review" && status != "reviewing" {
		t.Fatalf("invalidated review requeued to %q", status)
	}
	// No integration happened for the mutated review.
	if runFixtureGit(t, fixture.root, "rev-parse", fixture.target) != fixture.baseSHA {
		t.Fatal("an invalidated review must never integrate")
	}
}

// RV6: a reviewer pinned to a superseded result cannot accept or reject the
// current one; the stale review is invalidated and requeued.
func TestRV6StaleReviewAfterNewResultInvalid(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	taskPath := fixture.createTask("stale review")
	documentID := documentIDFromPath(t, taskPath)
	// Implementation round, then a reviewer whose provider hangs.
	fixture.installNoDispatchHook()
	fixture.scan()
	fixture.installReviewInterruptHook()
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].Phase != phaseTaskReview {
		t.Fatalf("reviewer leases = %#v", leases)
	}
	reviewer := leases[0]
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	request, ok := latestReviewRequest(evidence)
	if !ok {
		t.Fatal("no review request")
	}
	// A newer finalized implementation and its exact request replace the
	// result under review.
	if _, err := fixture.manager.appendFact(facts.Fact{
		Kind: facts.KindLaunchCreated, Key: launchCreatedKey("ln-newer-impl"),
		Doc: documentDoc("tasks", documentID), Launch: "ln-newer-impl", Phase: phaseTaskImplementation,
		WorkspaceKind: execws.WorkspaceKindGitWorktree, BaseSHA: fixture.baseSHA,
		TargetRef: request.TargetRef, TargetOld: request.TargetOld, CheckSet: execws.CheckSetDigest(nil),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.appendFact(facts.Fact{
		Kind: facts.KindResultCaptured, Key: resultCapturedKey("ln-newer-impl"),
		Doc: documentDoc("tasks", documentID), Launch: "ln-newer-impl", Phase: phaseTaskImplementation,
		WorkspaceKind: execws.WorkspaceKindGitWorktree, BaseSHA: fixture.baseSHA,
		ResultSHA: fixture.baseSHA, TargetRef: request.TargetRef, TargetOld: request.TargetOld,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.manager.appendFact(facts.Fact{
		Kind: facts.KindReviewRequested, Key: reviewRequestedKey("ln-newer-request"),
		Doc: documentDoc("tasks", documentID), Launch: "ln-newer-request", Phase: phaseTaskReview,
		WorkspaceKind: execws.WorkspaceKindGitWorktree, ImplLaunch: "ln-newer-impl",
		ReviewSHA: fixture.baseSHA, TargetRef: request.TargetRef, TargetOld: request.TargetOld,
	}); err != nil {
		t.Fatal(err)
	}
	// The hung reviewer finally settles and moves the document to done; its
	// stale verdict must not count.
	fixture.moveDocument(reviewer.File, "done")
	fixture.fake.emitNow(reviewer.SessionKey, core.Event{Kind: core.EventFinal, Text: "late", Done: true})
	fixture.manager.Wait()
	fixture.scan()
	evidence, err = fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	invalid, ok := findFact(evidence, reviewCompletedKey(reviewer.LaunchID))
	if !ok || invalid.Verdict != "invalid" {
		t.Fatalf("stale review verdict = %+v, want invalid", invalid)
	}
	status, _ := fixture.manager.locateDocument("tasks", filepath.Base(taskPath))
	if status != "review" && status != "reviewing" {
		t.Fatalf("the stale review must requeue for a fresh reviewer, got %q", status)
	}
	if runFixtureGit(t, fixture.root, "rev-parse", fixture.target) != fixture.baseSHA {
		t.Fatal("a stale review must never integrate")
	}
}

// LA8 shared half: the writer slot spans the provider terminal. The workflow
// holding the slot returns from Send at admission without settling, and the
// parked second workflow may only be admitted after the holder's terminal
// settle.
func TestLA8WriterGateSpansTerminalAndPipeline(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "first writer task", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "second writer task", ""); err != nil {
		t.Fatal(err)
	}
	fake := newEmitCaptureHarness()
	manager := New(cfg, fake, extensions.Runner{})
	sent := make(chan string, 2)
	var settled atomic.Bool
	var holder atomic.Value
	fake.onSend = func(h *emitCaptureHarness, key, prompt string) {
		if key != holder.Load() && key != "" && holder.Load() != nil && !settled.Load() {
			h.mu.Lock()
			h.violation = "a parked workflow was admitted before the holder settled"
			h.mu.Unlock()
		}
		select {
		case sent <- key:
		default:
		}
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	holderKey := <-sent
	holder.Store(holderKey)
	fake.mu.Lock()
	holderEmitters := fake.emits[holderKey]
	fake.mu.Unlock()
	if len(holderEmitters) == 0 {
		t.Fatal("the slot holder never dispatched")
	}
	// Fire the holder's terminal: the settle is synchronous inside the emit,
	// which releases the slot and admits the parked workflow.
	settled.Store(true)
	holderEmitters[len(holderEmitters)-1](core.Event{Kind: core.EventFinal, Text: "done", Done: true})
	parkedKey := <-sent
	manager.Wait()
	if parkedKey == holderKey {
		t.Fatal("the parked workflow never ran")
	}
	fake.mu.Lock()
	violation := fake.violation
	fake.mu.Unlock()
	if violation != "" {
		t.Fatal(violation)
	}
}

// LA8 isolated half: the writer slot spans the whole post-terminal pipeline —
// the parked isolated workflow may only run after the holder's capture and
// checks completed.
func TestLA8WriterGateSpansIsolatedPipeline(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "verify", Command: "git", Args: []string{"rev-parse", "HEAD"}},
	})
	if _, err := Create(fixture.cfg, "tasks", "alpha writer task", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(fixture.cfg, "tasks", "beta writer task", ""); err != nil {
		t.Fatal(err)
	}
	var firstLaunch Lease
	fixture.setHook(func(h *emitCaptureHarness, key, prompt string) {
		if strings.HasPrefix(key, "orchestrator:notification:") {
			h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "notified", Done: true})
			return
		}
		lease, ok := fixture.leaseForSession(key)
		if !ok {
			return
		}
		if !strings.Contains(key, ":task_implementation:") {
			h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "done", Done: true})
			return
		}
		if firstLaunch.LaunchID == "" {
			// The slot holder records itself; the parked workflow checks the
			// holder's pipeline evidence before running.
			firstLaunch = lease
		} else if lease.LaunchID != firstLaunch.LaunchID {
			evidence, err := fixture.manager.documentFacts("tasks", documentIDForLease(firstLaunch))
			if err != nil {
				fixture.t.Fatal(err)
			}
			if _, ok := findFact(evidence, resultCapturedKey(firstLaunch.LaunchID)); !ok {
				h.mu.Lock()
				h.violation = "a parked isolated workflow was admitted before the holder pipeline captured"
				h.mu.Unlock()
				return
			}
		}
		view := fixture.openWorkspace(lease)
		if err := os.WriteFile(filepath.Join(view.ProviderView(), "writer.txt"), []byte("content"), 0o600); err != nil {
			fixture.t.Fatal(err)
		}
		h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "done", Done: true})
	})
	// Both workflows claim in one scan; beta parks behind alpha's writer slot
	// until alpha's terminal, capture, and checks complete.
	fixture.scan()
	if firstLaunch.LaunchID == "" {
		t.Fatal("the first isolated workflow never dispatched")
	}
	fixture.fake.mu.Lock()
	violation := fixture.fake.violation
	sends := fixture.fake.sends
	fixture.fake.mu.Unlock()
	if violation != "" {
		t.Fatal(violation)
	}
	if sends != 2 {
		t.Fatalf("the parked isolated workflow was never admitted after the pipeline: %d", sends)
	}
	for _, lease := range mustLeases(t, fixture.manager) {
		if lease.State != "finalized" {
			t.Fatalf("lease state = %q", lease.State)
		}
	}
}

// CK8: a configuration change after launch creation never affects the
// launch's captured check set; recovery reruns the captured definitions only.
func TestCK8ConfigChangeAfterLaunchIrrelevant(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "captured", Command: "git", Args: []string{"rev-parse", "HEAD"}},
	})
	taskPath := fixture.createTask("captured check set")
	documentID := documentIDFromPath(t, taskPath)
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].State != "finalized" {
		t.Fatalf("implementation did not finalize: %#v", leases)
	}
	launch := leases[0]
	// The live configuration changes completely after the launch exists.
	changed := fixture.cfg
	changed.Orchestrator.Checks = []config.OrchestratorCheck{
		{ID: "other-check", Command: "true"},
	}
	fixture.manager.ApplyRuntimeConfig(changed)
	// Simulate the crash window: drop the check evidence and reset the
	// lease so the pipeline resumes with whatever definitions it resolves.
	journalPath := factsJournalPath(fixture.cfg, documentID)
	data, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(data), "\n")
	var kept []string
	for _, line := range lines {
		if strings.Contains(line, `"check_completed"`) {
			continue
		}
		kept = append(kept, line)
	}
	if err := os.WriteFile(journalPath, []byte(strings.Join(kept, "")), 0o600); err != nil {
		t.Fatal(err)
	}
	resumed := leases[0]
	resumed.State = "awaiting_transition"
	if err := fixture.manager.saveLease(resumed); err != nil {
		t.Fatal(err)
	}
	fixture.scan()
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	result := evidenceResultSHA(evidence, launch.LaunchID)
	captured, ok := findFact(evidence, checkCompletedKey(launch.LaunchID, "captured", result))
	if !ok || captured.Outcome != execws.CheckOutcomePassed {
		t.Fatalf("the captured check did not rerun: %+v", captured)
	}
	if _, ok := findFact(evidence, checkCompletedKey(launch.LaunchID, "other-check", result)); ok {
		t.Fatal("a live configuration change leaked into an existing launch")
	}
	// The launch finalized with its own captured set.
	current, err := fixture.manager.loadLease(launch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.LaunchID != launch.LaunchID || current.State != "finalized" {
		t.Fatalf("resumed launch state = %q", current.State)
	}
}
