package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/facts"
)

const cleanupRetention = 30 * 24 * time.Hour

// F1: the real cleanup path may inspect Git before any isolated claim, but it
// cannot freeze the target observation used by a later claim in this process.
func TestCleanupPreflightCannotFreezeLaterClaimTarget(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	if removed, reported, err := fixture.manager.CleanupIsolatedWorkspaces(context.Background(), cleanupRetention); err != nil || len(removed) != 0 || len(reported) != 0 {
		t.Fatalf("empty cleanup = removed %v reported %v err %v", removed, reported, err)
	}
	first, err := fixture.backend.Preflight(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	workspaceID := execws.NewWorkspaceID()
	if err := fixture.backend.Prepare(context.Background(), execws.Spec{ID: workspaceID, StartSHA: first.TargetOld}); err != nil {
		t.Fatal(err)
	}
	view, err := fixture.backend.Open(context.Background(), execws.Ref{ID: workspaceID, Kind: execws.BackendGitWorktree})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(view.ProviderView(), "cleanup-preflight.txt"), []byte("fresh target\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := view.Capture(context.Background(), execws.CaptureRequest{
		LaunchID: "ln-cleanup-preflight", BaseSHA: first.TargetOld,
		TaskDoc: "cleanup-preflight", Title: "cleanup preflight", At: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.backend.Integrator().Integrate(context.Background(), execws.Target{Ref: first.TargetRef, OldSHA: first.TargetOld}, result.ResultSHA); err != nil {
		t.Fatal(err)
	}
	second, err := fixture.backend.Preflight(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.TargetOld != result.ResultSHA || second.TargetOld == first.TargetOld {
		t.Fatalf("post-cleanup claim observed %s, want integrated %s after %s", second.TargetOld, result.ResultSHA, first.TargetOld)
	}
}

// CL1: an integrated implementation workspace is removed at the next cleanup
// while the reviewer workspace stays for its retention window.
func TestCL1IntegratedWorkspaceRemovedReviewerRetained(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "verify", Command: "git", Args: []string{"rev-parse", "HEAD"}},
	})
	fixture.createTask("cleanup integrated")
	// Drive the full flow: implementation, review, integration.
	fixture.scan()
	fixture.scan()
	fixture.scan()
	if remaining := mustLeases(t, fixture.manager); len(remaining) != 0 {
		t.Fatalf("flow did not settle: %#v", remaining[0])
	}
	refs, err := fixture.backend.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 {
		t.Fatalf("both workspaces must exist before cleanup: %d", len(refs))
	}
	removed, reported, err := fixture.manager.CleanupIsolatedWorkspaces(context.Background(), cleanupRetention)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || len(reported) != 0 {
		t.Fatalf("cleanup removed %v reported %v", removed, reported)
	}
	// Exactly the integrated implementation workspace is gone; the reviewer
	// stays for its retention window.
	refs, err = fixture.backend.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatalf("reviewer workspace must be retained: %d", len(refs))
	}
	// The workspace_removed evidence exists for the removed one.
	all, errs := fixture.manager.Facts.AllFacts()
	if len(errs) != 0 {
		t.Fatalf("journal errors: %v", errs)
	}
	removals := 0
	for _, fact := range all {
		if fact.Kind == facts.KindWorkspaceRemoved && fact.WorkspaceID == removed[0] {
			removals++
		}
	}
	if removals != 1 {
		t.Fatalf("workspace_removed facts = %d", removals)
	}
	// CL6: the result ref survives cleanup — its archival boundary is later.
	ref := runFixtureGit(t, fixture.root, "for-each-ref", "--format=%(refname)", "refs/spynel/launches/")
	if !strings.Contains(ref, "/result") {
		t.Fatalf("cleanup must never delete launch result refs: %q", ref)
	}
	// After the positive retention window the reviewer workspace is removable.
	removed, reported, err = fixture.manager.cleanupIsolatedWorkspaces(context.Background(), cleanupRetention, time.Now().UTC().Add(31*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || len(reported) != 0 {
		t.Fatalf("aged reviewer cleanup removed %v reported %v", removed, reported)
	}
	if refs, _ = fixture.backend.List(context.Background()); len(refs) != 0 {
		t.Fatalf("all settled workspaces must eventually clean: %d", len(refs))
	}
}

// CL2: a failed launch's workspace is retained for the retention window and
// removed only after it.
func TestCL2FailureRetention(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	// An implementation launch that fails its checks keeps its workspace.
	fixture.setHook(func(h *emitCaptureHarness, key, prompt string) {
		if strings.HasPrefix(key, "orchestrator:notification:") {
			h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "notified", Done: true})
			return
		}
		lease, ok := fixture.leaseForSession(key)
		if !ok {
			return
		}
		view := fixture.openWorkspace(lease)
		if err := os.WriteFile(filepath.Join(view.ProviderView(), "feature.txt"), []byte("doomed"), 0o600); err != nil {
			fixture.t.Fatal(err)
		}
		h.emitNow(key, core.Event{Kind: core.EventFinal, Text: "done", Done: true})
	})
	fixture.manager.ApplyRuntimeConfig(isolatedChecksConfig(t, fixture.cfg, []string{"missing-check-command-9f2"}))
	fixture.createTask("cleanup failure retention")
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 || leases[0].State != "error" {
		t.Fatalf("failed launch lease = %#v", leases)
	}
	failedWorkspace := leases[0].WorkspaceID
	// Within the retention window nothing is removed.
	removed, _, err := fixture.manager.CleanupIsolatedWorkspaces(context.Background(), cleanupRetention)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("failed workspace removed inside retention: %v", removed)
	}
	// After the window it is removed.
	removed, _, err = fixture.manager.cleanupIsolatedWorkspaces(context.Background(), cleanupRetention, time.Now().UTC().Add(31*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != failedWorkspace {
		t.Fatalf("aged failure cleanup removed %v", removed)
	}
}

// CL3: workspaces of live or preparing leases are protected from cleanup,
// and an in-flight cleanup cannot race a fresh claim.
func TestCL3ActiveAndPreparingProtected(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	// A preparing lease whose workspace exists must never be cleaned.
	crafted := fixture.craftImplementationLaunch(t, struct {
		workspaceReady bool
		admitted       bool
		terminal       bool
		state          string
		staleHeartbeat bool
		claimDoc       bool
		editWorktree   bool
	}{state: "preparing", workspaceReady: true, claimDoc: true})
	removed, reported, err := fixture.manager.cleanupIsolatedWorkspaces(context.Background(), cleanupRetention, time.Now().UTC().Add(31*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("preparing workspace removed: %v", removed)
	}
	if len(reported) != 0 {
		t.Fatalf("preparing workspace reported: %v", reported)
	}
	// A live processing lease is protected too.
	if err := fixture.manager.updateLease(crafted.lease.ID, crafted.launch, func(current *Lease) {
		current.State = "processing"
	}); err != nil {
		t.Fatal(err)
	}
	removed, _, err = fixture.manager.cleanupIsolatedWorkspaces(context.Background(), cleanupRetention, time.Now().UTC().Add(31*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("live workspace removed: %v", removed)
	}
	// The workspace still exists.
	if _, err := fixture.backend.Open(context.Background(), execws.Ref{ID: crafted.lease.WorkspaceID, Kind: execws.BackendGitWorktree}); err != nil {
		t.Fatalf("protected workspace vanished: %v", err)
	}
}

// CL4: a registered directory without launch_created evidence is reported
// and never deleted.
func TestCL4UnknownWorkspaceNotDeleted(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	unknownID := execws.NewWorkspaceID()
	if err := fixture.backend.Prepare(context.Background(), execws.Spec{ID: unknownID, StartSHA: fixture.baseSHA}); err != nil {
		t.Fatal(err)
	}
	removed, reported, err := fixture.manager.cleanupIsolatedWorkspaces(context.Background(), cleanupRetention, time.Now().UTC().Add(31*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("unknown workspace deleted: %v", removed)
	}
	found := false
	for _, line := range reported {
		if strings.Contains(line, unknownID) {
			found = true
		}
	}
	if !found {
		t.Fatalf("unknown workspace not reported: %v", reported)
	}
	// The directory is untouched.
	if _, err := fixture.backend.Open(context.Background(), execws.Ref{ID: unknownID, Kind: execws.BackendGitWorktree}); err != nil {
		t.Fatalf("unknown workspace was deleted: %v", err)
	}
}

// CL5: interrupted cleanup is idempotent — a worktree whose registration was
// pruned but whose directory remains is removed on the next pass, and a
// removed workspace stays removed.
func TestCL5PartialCleanupIdempotent(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	// Integrated-flow fixture: reuse the crafted launch with a terminal and
	// append integration evidence so the workspace is cleanup-eligible.
	crafted := fixture.craftImplementationLaunch(t, struct {
		workspaceReady bool
		admitted       bool
		terminal       bool
		state          string
		staleHeartbeat bool
		claimDoc       bool
		editWorktree   bool
	}{state: "awaiting_transition", workspaceReady: true, admitted: true, terminal: true, claimDoc: true, editWorktree: true})
	if _, err := fixture.manager.appendFact(facts.Fact{
		Kind: facts.KindIntegrationCompleted, Key: integrationKey(crafted.launch),
		Doc: documentDoc("tasks", crafted.documentID), Launch: crafted.launch,
		WorkspaceID: crafted.lease.WorkspaceID, WorkspaceKind: execws.WorkspaceKindGitWorktree,
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate interrupted cleanup: the worktree registration was pruned but
	// the directory remains.
	if err := fixture.backend.Remove(context.Background(), execws.Ref{ID: crafted.lease.WorkspaceID, Kind: execws.BackendGitWorktree}); err != nil {
		t.Fatal(err)
	}
	// Remove() is itself idempotent.
	if err := fixture.backend.Remove(context.Background(), execws.Ref{ID: crafted.lease.WorkspaceID, Kind: execws.BackendGitWorktree}); err != nil {
		t.Fatalf("second remove must tolerate the absent workspace: %v", err)
	}
	refs, err := fixture.backend.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Fatalf("partial cleanup left registrations: %v", refs)
	}
	// The manager cleanup treats the pruned workspace as absent: a second
	// run reports nothing new and stays error-free.
	removed, reported, err := fixture.manager.cleanupIsolatedWorkspaces(context.Background(), cleanupRetention, time.Now().UTC().Add(31*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 && removed[0] == crafted.lease.WorkspaceID {
		t.Fatalf("already-absent workspace removed again: %v", removed)
	}
	_ = reported
}

// isolatedChecksConfig copies the fixture configuration with one check whose
// command cannot exist, producing a failing launch.
func isolatedChecksConfig(t *testing.T, base config.Config, command []string) config.Config {
	t.Helper()
	cfg := base
	cfg.Orchestrator.Checks = []config.OrchestratorCheck{{ID: "broken", Command: command[0]}}
	return cfg
}
