package execws

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testRepo creates a temporary Git repository with deterministic identity and
// two commits on its initial branch. It returns the repository root, the
// branch ref, and the SHAs of the two commits (oldest first).
func testRepo(t *testing.T) (root, branch string, shas []string) {
	t.Helper()
	root = t.TempDir()
	gitInit(t, root)
	runGit(t, root, "commit", "--allow-empty", "-m", "base")
	base := runGit(t, root, "rev-parse", "HEAD")
	runGit(t, root, "commit", "--allow-empty", "-m", "second")
	second := runGit(t, root, "rev-parse", "HEAD")
	ref := strings.TrimSpace(runGit(t, root, "symbolic-ref", "HEAD"))
	return root, ref, []string{strings.TrimSpace(base), strings.TrimSpace(second)}
}

func gitInit(t *testing.T, dir string) {
	t.Helper()
	runGit(t, dir, "init", "-q", "--initial-branch=main", ".")
	runGit(t, dir, "config", "user.email", "test@example.com")
	runGit(t, dir, "config", "user.name", "Test")
	runGit(t, dir, "config", "commit.gpgsign", "false")
}

// runGit runs plain git for test fixture setup only; production code always
// goes through the scrubbed runner. The helper scrubs the same dangerous Git
// environment so tests stay valid under t.Setenv probes.
func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = scrubbedTestEnv(os.Environ())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func scrubbedTestEnv(environ []string) []string {
	scrubbed := make([]string, 0, len(environ))
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if scrubbedEnvName(name) {
			continue
		}
		scrubbed = append(scrubbed, entry)
	}
	return append(scrubbed, "GIT_TERMINAL_PROMPT=0")
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func preparedWorkspace(t *testing.T, root, startSHA string) (*LocalGit, Workspace) {
	t.Helper()
	ctx := context.Background()
	backend, err := NewLocalGit(root)
	if err != nil {
		t.Fatalf("NewLocalGit: %v", err)
	}
	if _, err := backend.Preflight(ctx); err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	id := NewWorkspaceID()
	if err := backend.Prepare(ctx, Spec{ID: id, StartSHA: startSHA}); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	ws, err := backend.Open(ctx, Ref{ID: id, Kind: BackendGitWorktree})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return backend, ws
}

func terminalTime() time.Time {
	return time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
}

func captureRequest(launch, base, doc string) CaptureRequest {
	return CaptureRequest{LaunchID: launch, BaseSHA: base, TaskDoc: doc, Title: "probe result", At: terminalTime()}
}
