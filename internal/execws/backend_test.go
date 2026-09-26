package execws

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// WS1: a prepared worktree is detached, locked, and checked out at the exact
// start SHA.
func TestWS1WorktreeDetachedLockedExactSHA(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	backend, err := NewLocalGit(root)
	if err != nil {
		t.Fatalf("NewLocalGit: %v", err)
	}
	if _, err := backend.Preflight(ctx); err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	id := "ws-" + strings.Repeat("a", 26)
	if err := backend.Prepare(ctx, Spec{ID: id, StartSHA: shas[0]}); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	path := backend.worktreeDir(id)
	var entry *worktreeEntry
	entries, err := backend.worktrees(ctx)
	if err != nil {
		t.Fatalf("worktrees: %v", err)
	}
	for index := range entries {
		if samePath(entries[index].Path, path) {
			entry = &entries[index]
		}
	}
	if entry == nil {
		t.Fatal("prepared worktree is not registered")
	}
	if !entry.Detached {
		t.Fatal("worktree must be detached")
	}
	if !entry.Locked {
		t.Fatal("worktree must be locked")
	}
	if entry.Head != shas[0] {
		t.Fatalf("worktree HEAD = %s, want %s", entry.Head, shas[0])
	}
	ws, err := backend.Open(ctx, Ref{ID: id, Kind: BackendGitWorktree})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := ws.VerifyAt(ctx, shas[0]); err != nil {
		t.Fatalf("VerifyAt: %v", err)
	}
}

// WS2: Prepare is idempotent for the same identity and start SHA and fails
// closed on conflicts.
func TestWS2IdempotentPrepareAndConflicts(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	backend, err := NewLocalGit(root)
	if err != nil {
		t.Fatalf("NewLocalGit: %v", err)
	}
	if _, err := backend.Preflight(ctx); err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	id := NewWorkspaceID()
	spec := Spec{ID: id, StartSHA: shas[0]}
	if err := backend.Prepare(ctx, spec); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := backend.Prepare(ctx, spec); err != nil {
		t.Fatalf("idempotent Prepare: %v", err)
	}
	if err := backend.Prepare(ctx, Spec{ID: id, StartSHA: shas[1]}); !errors.Is(err, ErrWorkspaceConflict) {
		t.Fatalf("different start SHA must conflict: %v", err)
	}
	foreignID := NewWorkspaceID()
	if err := backend.Prepare(ctx, Spec{ID: foreignID, StartSHA: shas[1]}); err != nil {
		t.Fatalf("Prepare second: %v", err)
	}
	// A dirty registered worktree at the right SHA is a conflict.
	dirty := NewWorkspaceID()
	if err := backend.Prepare(ctx, Spec{ID: dirty, StartSHA: shas[0]}); err != nil {
		t.Fatalf("Prepare dirty candidate: %v", err)
	}
	writeFile(t, filepath.Join(backend.worktreeDir(dirty), "uncommitted.txt"), "dirt")
	if err := backend.Prepare(ctx, Spec{ID: dirty, StartSHA: shas[0]}); !errors.Is(err, ErrWorkspaceConflict) {
		t.Fatalf("dirty worktree must conflict: %v", err)
	}
	// An unregistered directory at the workspace path conflicts.
	stray := NewWorkspaceID()
	writeFile(t, filepath.Join(backend.worktreeDir(stray), "stray.txt"), "x")
	if err := backend.Prepare(ctx, Spec{ID: stray, StartSHA: shas[0]}); !errors.Is(err, ErrWorkspaceConflict) {
		t.Fatalf("stray directory must conflict: %v", err)
	}
}

// WS3: two workspace identities get isolated directories and file systems.
func TestWS3DistinctWorkspacesAreIsolated(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	backend, ws1 := preparedWorkspace(t, root, shas[0])
	id1 := ws1.Ref().ID
	ws2 := func() Workspace {
		id := NewWorkspaceID()
		if err := backend.Prepare(ctx, Spec{ID: id, StartSHA: shas[0]}); err != nil {
			t.Fatalf("Prepare second: %v", err)
		}
		opened, err := backend.Open(ctx, Ref{ID: id, Kind: BackendGitWorktree})
		if err != nil {
			t.Fatalf("Open second: %v", err)
		}
		return opened
	}()
	if ws1.Ref().ID == ws2.Ref().ID {
		t.Fatal("two workspaces must not share one identity")
	}
	if samePath(ws1.ProviderView(), ws2.ProviderView()) {
		t.Fatal("two workspaces must not share one provider directory")
	}
	writeFile(t, filepath.Join(ws1.ProviderView(), "left.txt"), "left")
	writeFile(t, filepath.Join(ws2.ProviderView(), "right.txt"), "right")
	if _, err := os.Stat(filepath.Join(ws2.ProviderView(), "left.txt")); !os.IsNotExist(err) {
		t.Fatal("left workspace file leaked into the right workspace")
	}
	if _, err := os.Stat(filepath.Join(ws1.ProviderView(), "right.txt")); !os.IsNotExist(err) {
		t.Fatal("right workspace file leaked into the left workspace")
	}
	_ = id1
}

// WS4: preflight failures never fall back to shared mode; the reason is
// surfaced while the document stays unclaimed.
func TestWS4PreflightFailuresFailClosed(t *testing.T) {
	ctx := context.Background()

	t.Run("outside repository", func(t *testing.T) {
		backend, err := NewLocalGit(t.TempDir())
		if err != nil {
			t.Fatalf("NewLocalGit: %v", err)
		}
		_, err = backend.Preflight(ctx)
		var preflightErr *PreflightError
		if !errors.As(err, &preflightErr) || preflightErr.Reason != "root_outside_worktree" {
			t.Fatalf("err = %v, want root_outside_worktree", err)
		}
	})

	t.Run("bare repository", func(t *testing.T) {
		bare := t.TempDir()
		runGit(t, bare, "init", "-q", "--bare", ".")
		backend, err := NewLocalGit(bare)
		if err != nil {
			t.Fatalf("NewLocalGit: %v", err)
		}
		_, err = backend.Preflight(ctx)
		var preflightErr *PreflightError
		if !errors.As(err, &preflightErr) || preflightErr.Reason != "repo_bare" {
			t.Fatalf("err = %v, want repo_bare", err)
		}
	})

	t.Run("detached root head", func(t *testing.T) {
		root, _, shas := testRepo(t)
		runGit(t, root, "checkout", "-q", "--detach", shas[0])
		backend, err := NewLocalGit(root)
		if err != nil {
			t.Fatalf("NewLocalGit: %v", err)
		}
		_, err = backend.Preflight(ctx)
		var preflightErr *PreflightError
		if !errors.As(err, &preflightErr) || preflightErr.Reason != "root_detached" {
			t.Fatalf("err = %v, want root_detached", err)
		}
	})

	t.Run("tracked spynel state", func(t *testing.T) {
		root, _, _ := testRepo(t)
		writeFile(t, filepath.Join(root, ".spynel", "leak.txt"), "leak")
		runGit(t, root, "add", "-f", ".spynel")
		runGit(t, root, "commit", "-qm", "track state")
		backend, err := NewLocalGit(root)
		if err != nil {
			t.Fatalf("NewLocalGit: %v", err)
		}
		_, err = backend.Preflight(ctx)
		var preflightErr *PreflightError
		if !errors.As(err, &preflightErr) || preflightErr.Reason != "spynel_tracked" {
			t.Fatalf("err = %v, want spynel_tracked", err)
		}
	})

	t.Run("version gate", func(t *testing.T) {
		if err := supportedGitVersion("git version 2.24.9"); err == nil {
			t.Fatal("git 2.24 must be rejected")
		}
		if err := supportedGitVersion("git version 2.25.0"); err != nil {
			t.Fatalf("git 2.25 must be accepted: %v", err)
		}
		if err := supportedGitVersion("git version 2.39.2 (Apple Git-157)"); err != nil {
			t.Fatalf("Apple git must be accepted: %v", err)
		}
		if err := supportedGitVersion("nonsense"); err == nil {
			t.Fatal("unrecognized version output must be rejected")
		}
	})
}

// WS5: a workspace root below the repository top maps the provider directory
// into the new worktree.
func TestWS5RepoSubdirectoryProviderView(t *testing.T) {
	ctx := context.Background()
	root, _, _ := testRepo(t)
	writeFile(t, filepath.Join(root, "apps", "hoa", "main.go"), "package main")
	runGit(t, root, "add", "-A")
	runGit(t, root, "commit", "-qm", "add app")
	start := strings.TrimSpace(runGit(t, root, "rev-parse", "HEAD"))

	subRoot := filepath.Join(root, "apps", "hoa")
	backend, err := NewLocalGit(subRoot)
	if err != nil {
		t.Fatalf("NewLocalGit: %v", err)
	}
	report, err := backend.Preflight(ctx)
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if report.RootRel != "apps/hoa" {
		t.Fatalf("RootRel = %q, want apps/hoa", report.RootRel)
	}
	if report.RepoTop != root {
		t.Fatalf("RepoTop = %q, want %q", report.RepoTop, root)
	}
	id := NewWorkspaceID()
	if err := backend.Prepare(ctx, Spec{ID: id, StartSHA: start}); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	ws, err := backend.Open(ctx, Ref{ID: id, Kind: BackendGitWorktree})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := filepath.Join(backend.worktreeDir(id), "apps", "hoa")
	if ws.ProviderView() != want {
		t.Fatalf("ProviderView = %q, want %q", ws.ProviderView(), want)
	}
	if _, err := os.Stat(filepath.Join(ws.ProviderView(), "main.go")); err != nil {
		t.Fatalf("provider view must contain the subdirectory checkout: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backend.worktreeDir(id), "apps", "hoa", "main.go")); err != nil {
		t.Fatalf("worktree must contain the app tree: %v", err)
	}
}

// WS6: dangerous inherited Git environment never redirects Spynel Git.
func TestWS6DangerousGitEnvScrubbed(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	// GIT_DIR/GIT_INDEX_FILE/GIT_WORK_TREE pointing at hostile locations
	// would corrupt or redirect every runner operation if inherited.
	decoy := t.TempDir()
	writeFile(t, filepath.Join(decoy, "gitdir-marker"), "x")
	t.Setenv("GIT_DIR", decoy)
	t.Setenv("GIT_WORK_TREE", decoy)
	t.Setenv("GIT_INDEX_FILE", filepath.Join(decoy, "bogus-index"))
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "user.name")
	t.Setenv("GIT_CONFIG_VALUE_0", "Hostile")

	backend, ws := preparedWorkspace(t, root, shas[0])
	writeFile(t, filepath.Join(ws.ProviderView(), "scrubbed.txt"), "content")
	result, err := ws.Capture(ctx, captureRequest("ln-scrub", shas[0], "doc-scrub"))
	if err != nil {
		t.Fatalf("Capture with hostile env: %v", err)
	}
	if !result.Changed {
		t.Fatal("capture must see the workspace edit despite hostile env")
	}
	// The captured identity must not be the hostile one.
	body := runGit(t, backend.worktreeDir(ws.Ref().ID), "cat-file", "commit", result.ResultSHA)
	if strings.Contains(body, "Hostile") {
		t.Fatal("hostile GIT_CONFIG identity leaked into the canonical commit")
	}
	if strings.Contains(body, "decoy") {
		t.Fatal("decoy path leaked into the canonical commit")
	}
}

// WS7: repository hooks never execute during Spynel-owned Git operations.
func TestWS7HooksDisabled(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	ctx := context.Background()
	root, _, shas := testRepo(t)
	// post-checkout runs on `git worktree add`; a failing hook must not run.
	hooks := filepath.Join(root, ".git", "hooks")
	writeFile(t, filepath.Join(hooks, "post-checkout"), "#!/bin/sh\nexit 1\n")
	if err := os.Chmod(filepath.Join(hooks, "post-checkout"), 0o700); err != nil {
		t.Fatalf("chmod hook: %v", err)
	}
	// post-merge runs on `git merge --ff-only` during integration.
	writeFile(t, filepath.Join(hooks, "post-merge"), "#!/bin/sh\nexit 1\n")
	if err := os.Chmod(filepath.Join(hooks, "post-merge"), 0o700); err != nil {
		t.Fatalf("chmod hook: %v", err)
	}
	backend, ws := preparedWorkspace(t, root, shas[1])
	if err := ws.VerifyAt(ctx, shas[1]); err != nil {
		t.Fatalf("VerifyAt: %v", err)
	}
	// A failing post-checkout hook must not have blocked Prepare; now prove
	// capture and integration work with a failing post-merge hook installed.
	writeFile(t, filepath.Join(ws.ProviderView(), "hook-probe.txt"), "content")
	result, err := ws.Capture(ctx, captureRequest("ln-hooks", shas[1], "doc-hooks"))
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if !result.Changed {
		t.Fatal("capture must produce a change for the hook probe")
	}
	report, err := backend.Preflight(ctx)
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if _, err := backend.Integrate(ctx, Target{Ref: report.TargetRef, OldSHA: report.TargetOld}, result.ResultSHA); err != nil {
		t.Fatalf("Integrate with failing post-merge hook installed: %v", err)
	}
}

// F1: each isolated claim observes the target as it exists for that claim.
// Earlier preflight users (including cleanup) cannot freeze a process-wide
// base SHA, target ref, error, or caller context.
func TestPreflightRefreshesTargetAfterSameProcessIntegration(t *testing.T) {
	ctx := context.Background()
	root, targetRef, shas := testRepo(t)
	backend, err := NewLocalGit(root)
	if err != nil {
		t.Fatal(err)
	}

	// This first observation represents cleanup/shared code touching the
	// backend before the first isolated claim.
	first, err := backend.Preflight(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.TargetRef != targetRef || first.TargetOld != shas[1] {
		t.Fatalf("first preflight = %+v, want %s at %s", first, targetRef, shas[1])
	}

	id := NewWorkspaceID()
	if err := backend.Prepare(ctx, Spec{ID: id, StartSHA: first.TargetOld}); err != nil {
		t.Fatal(err)
	}
	ws, err := backend.Open(ctx, Ref{ID: id, Kind: BackendGitWorktree})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(ws.ProviderView(), "fresh-base.txt"), "task one")
	result, err := ws.Capture(ctx, captureRequest("ln-fresh-target", first.TargetOld, "doc-fresh-target"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Integrate(ctx, Target{Ref: first.TargetRef, OldSHA: first.TargetOld}, result.ResultSHA); err != nil {
		t.Fatal(err)
	}

	second, err := backend.Preflight(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.TargetOld != result.ResultSHA {
		t.Fatalf("second claim observed stale target %s, want integrated %s", second.TargetOld, result.ResultSHA)
	}
	if second.TargetOld == first.TargetOld {
		t.Fatal("second claim reused the first claim's base evidence")
	}
}

func TestWorkspaceIDShape(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 10000; i++ {
		id := NewWorkspaceID()
		if !strings.HasPrefix(id, "ws-") || len(id) != len("ws-")+26 {
			t.Fatalf("workspace id shape = %q", id)
		}
		if strings.ToLower(id) != id {
			t.Fatalf("workspace id must be lowercase: %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate workspace id %q", id)
		}
		seen[id] = true
	}
}
