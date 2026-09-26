package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/facts"
	"github.com/agent0ai/spynel/internal/workspace"
)

// documentAllFacts enumerates task evidence across documents for assertions
// that do not track one document identity.
func (m *Manager) documentAllFacts() ([]facts.Fact, error) {
	all, errs := m.Facts.AllFacts()
	if len(errs) > 0 {
		return nil, errs[0]
	}
	return all, nil
}

// RG1: default shared mode creates no worktrees, no canonical Git results,
// and no integration while preserving shared session keys.
func TestRG1DefaultSharedCreatesNoWorkspaces(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Orchestrator.EffectiveWorkspaceIsolation() != config.WorkspaceIsolationShared {
		t.Fatal("the default isolation mode must be shared")
	}
	if _, err := Create(cfg, "tasks", "shared mode probe", ""); err != nil {
		t.Fatal(err)
	}
	fake := newEmitCaptureHarness()
	manager := New(cfg, fake, extensions.Runner{})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases := mustLeases(t, manager)
	if len(leases) != 1 {
		t.Fatalf("leases = %#v", leases)
	}
	lease := leases[0]
	if lease.WorkspaceKind != execws.WorkspaceKindShared || lease.WorkspaceID != "" {
		t.Fatalf("shared lease became isolated: %#v", lease)
	}
	if strings.Contains(lease.SessionKey, lease.LaunchID) {
		t.Fatalf("shared session key carries a launch suffix: %q", lease.SessionKey)
	}
	// No execution-plane directories were created.
	if _, err := os.Stat(filepath.Join(root, ".spynel", "worktrees")); !os.IsNotExist(err) {
		t.Log(".spynel/worktrees exists from workspace init; verify no worktree content")
		entries, readErr := os.ReadDir(filepath.Join(root, ".spynel", "worktrees"))
		if readErr != nil || len(entries) != 0 {
			t.Fatalf("shared mode created worktrees: %v", entries)
		}
	}
	// Shared launches record additive evidence only: no workspace, capture,
	// check, review, or integration facts.
	evidence, err := manager.documentFacts("tasks", lease.DocumentID)
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range evidence {
		if fact.WorkspaceID != "" || fact.ResultSHA != "" || fact.Integration != "" {
			t.Fatalf("shared launch recorded isolated evidence: %+v", fact)
		}
		switch fact.Kind {
		case facts.KindLaunchCreated, facts.KindProviderAdmitted, facts.KindProviderTerminal:
		default:
			t.Fatalf("shared launch fact kind = %q", fact.Kind)
		}
	}
}

// RG3: goals get launch identities and shared-root workspaces only; goal
// planning and review never receive isolated workspaces.
func TestRG3GoalsGetLaunchesWithoutWorkspaces(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Orchestrator.WorkspaceIsolation = config.WorkspaceIsolationGitWorktree
	// The root must pass git preflight for task claims to try isolation; use
	// a real repository so the mode is genuinely live.
	runFixtureGit(t, root, "init", "-q", "--initial-branch=main", ".")
	runFixtureGit(t, root, "config", "user.email", "fixture@example.com")
	runFixtureGit(t, root, "config", "user.name", "Fixture")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("goal probe"), 0o600); err != nil {
		t.Fatal(err)
	}
	runFixtureGit(t, root, "add", "-A")
	runFixtureGit(t, root, "commit", "-qm", "base")
	if _, err := Create(cfg, "goals", "isolated goal probe", "## Success Criteria\n- [ ] done"); err != nil {
		t.Fatal(err)
	}
	fake := newEmitCaptureHarness()
	manager := New(cfg, fake, extensions.Runner{})
	backend, err := execws.NewLocalGit(root)
	if err != nil {
		t.Fatal(err)
	}
	manager.WorkspaceBackend = backend
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases := mustLeases(t, manager)
	if len(leases) != 1 {
		t.Fatalf("goal leases = %#v", leases)
	}
	lease := leases[0]
	if lease.Phase != phaseGoalPlanning {
		t.Fatalf("goal lease phase = %q", lease.Phase)
	}
	if lease.LaunchID == "" {
		t.Fatal("goal launches must still receive launch identities")
	}
	if lease.WorkspaceKind != execws.WorkspaceKindShared || lease.WorkspaceID != "" {
		t.Fatalf("goal planning became isolated: %#v", lease)
	}
	// No worktree was created for the goal.
	refs, err := backend.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Fatalf("goal work created worktrees: %v", refs)
	}
	// The goal's evidence carries the shared workspace kind.
	evidence, err := manager.documentFacts("goals", lease.DocumentID)
	if err != nil {
		t.Fatal(err)
	}
	created, ok := findFact(evidence, launchCreatedKey(lease.LaunchID))
	if !ok || created.WorkspaceKind != execws.WorkspaceKindShared || created.WorkspaceID != "" {
		t.Fatalf("goal launch evidence = %+v", created)
	}
}

// RG4: run-once settles through the post-terminal pipeline, not only the
// provider turn.
func TestRG4RunOnceWaitsThroughPipeline(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "verify", Command: "git", Args: []string{"rev-parse", "HEAD"}},
	})
	fixture.createTask("run once pipeline")
	if err := fixture.manager.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// RunOnce settled the implementation transition, which its reconciliation
	// could only accept after the finalized pipeline evidence existed: the
	// settle rounds waited through the pipeline, not just the provider turn.
	if leases := mustLeases(t, fixture.manager); len(leases) != 0 {
		t.Fatalf("run-once left %d unsettled leases", len(leases))
	}
	evidence, err := fixture.manager.documentAllFacts()
	if err != nil {
		t.Fatal(err)
	}
	var launch, result string
	for _, fact := range evidence {
		switch fact.Kind {
		case facts.KindResultCaptured:
			launch, result = fact.Launch, fact.ResultSHA
		}
	}
	if launch == "" || result == "" {
		t.Fatal("run-once settled without the captured result")
	}
	if _, ok := findFact(evidence, checkCompletedKey(launch, "verify", result)); !ok {
		t.Fatal("run-once settled without the check evidence")
	}
	if _, ok := findFact(evidence, reviewRequestedKey(launch)); !ok {
		t.Fatal("run-once did not reconcile the isolated implementation")
	}
}

func evidenceResultSHA(evidence []facts.Fact, launch string) string {
	for _, fact := range evidence {
		if fact.Kind == facts.KindResultCaptured && fact.Launch == launch {
			return fact.ResultSHA
		}
	}
	return ""
}

// RG7: the isolation mode applies live to new claims only.
func TestRG7IsolationModeAppliesLiveToNewClaims(t *testing.T) {
	fixture := newIsolatedFixture(t, nil)
	// The fixture starts in git-worktree mode; flip it to shared live and a
	// new task claim must run shared.
	shared := fixture.cfg
	shared.Orchestrator.WorkspaceIsolation = config.WorkspaceIsolationShared
	fixture.manager.ApplyRuntimeConfig(shared)
	taskPath := fixture.createTask("live mode switch")
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 {
		t.Fatalf("leases = %#v", leases)
	}
	if leases[0].WorkspaceKind != execws.WorkspaceKindShared || leases[0].WorkspaceID != "" {
		t.Fatalf("live shared mode still isolated the claim: %#v", leases[0])
	}
	_ = taskPath
	// Flipping back isolates the next claim.
	fixture.manager.ApplyRuntimeConfig(fixture.cfg)
	next := fixture.createTask("live mode switch back")
	fixture.scan()
	leases = mustLeases(t, fixture.manager)
	var isolated Lease
	for _, lease := range leases {
		if filepath.Base(lease.File) == filepath.Base(next) {
			isolated = lease
		}
	}
	if isolated.LaunchID == "" || isolated.WorkspaceKind != execws.WorkspaceKindGitWorktree {
		t.Fatalf("live git-worktree mode did not isolate the new claim: %#v", isolated)
	}
}
