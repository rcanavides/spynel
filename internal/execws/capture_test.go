package execws

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func captureProbe(t *testing.T, base string) (Workspace, CaptureRequest) {
	t.Helper()
	root, _, _ := testRepo(t)
	// Start every capture probe at the target HEAD so integration evidence is
	// consistent; the base commit is the second fixture commit.
	if base == "" {
		base = strings.TrimSpace(runGit(t, root, "rev-parse", "HEAD"))
	}
	_, ws := preparedWorkspace(t, root, base)
	return ws, captureRequest("ln-gt", base, "doc-gt")
}

// GT: a tracked edit is captured as the canonical result with parent base.
func TestGTCaptureTrackedEdit(t *testing.T) {
	ctx := context.Background()
	ws, req := captureProbe(t, "")
	writeFile(t, filepath.Join(ws.ProviderView(), "README.md"), "tracked edit")
	result, err := ws.Capture(ctx, req)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if !result.Changed || result.ResultSHA == req.BaseSHA {
		t.Fatalf("tracked edit must change the result: %+v", result)
	}
	parents := strings.Fields(runGit(t, filepath.Dir(filepath.Dir(ws.ProviderView())), "rev-list", "--parents", "-n", "1", result.ResultSHA))
	if len(parents) != 2 || parents[1] != req.BaseSHA {
		t.Fatalf("canonical commit parents = %v, want exactly %s", parents, req.BaseSHA)
	}
	if err := ws.VerifyAt(ctx, result.ResultSHA); err != nil {
		t.Fatalf("workspace must verify at the canonical result: %v", err)
	}
	body := runGit(t, filepath.Dir(filepath.Dir(ws.ProviderView())), "cat-file", "commit", result.ResultSHA)
	for _, trailer := range []string{"Spynel-Task: doc-gt", "Spynel-Launch: ln-gt", "Spynel-Base: " + req.BaseSHA} {
		if !strings.Contains(body, trailer) {
			t.Fatalf("canonical commit missing trailer %q in:\n%s", trailer, body)
		}
	}
	if len(result.ChangedPaths) != 1 || result.ChangedPaths[0] != "README.md" {
		t.Fatalf("changed paths = %v", result.ChangedPaths)
	}
}

// GT: an untracked nonignored file is captured; ignored files are excluded.
func TestGTCaptureUntrackedAndIgnoredFiles(t *testing.T) {
	ctx := context.Background()
	root, _, _ := testRepo(t)
	writeFile(t, filepath.Join(root, ".gitignore"), "ignored-dir/\n*.log\n")
	runGit(t, root, "add", "-A")
	runGit(t, root, "commit", "-qm", "ignore rules")
	base := strings.TrimSpace(runGit(t, root, "rev-parse", "HEAD"))
	_, ws := preparedWorkspace(t, root, base)
	req := captureRequest("ln-untracked", base, "doc-untracked")
	writeFile(t, filepath.Join(ws.ProviderView(), "fresh"), "new nonignored file")
	writeFile(t, filepath.Join(ws.ProviderView(), "ignored-dir", "nested.txt"), "ignored content")
	writeFile(t, filepath.Join(ws.ProviderView(), "debug.log"), "ignored log")
	result, err := ws.Capture(ctx, req)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if !result.Changed {
		t.Fatal("the nonignored untracked file must change the result")
	}
	git := ws.(*gitWorkspace)
	indexOut := func() string {
		return runGit(t, git.dir, "show", "--name-only", "--format=", result.ResultSHA)
	}()
	if strings.Contains(indexOut, "fresh") != true {
		t.Fatalf("captured result must include the nonignored file:\n%s", indexOut)
	}
	if strings.Contains(indexOut, "ignored-dir") || strings.Contains(indexOut, "debug.log") {
		t.Fatalf("ignored outputs must never enter the canonical result:\n%s", indexOut)
	}
	// Ignored outputs may remain in the workspace after capture.
	if _, err := os.Stat(filepath.Join(ws.ProviderView(), "ignored-dir", "nested.txt")); err != nil {
		t.Fatalf("ignored outputs may remain: %v", err)
	}
}

// GT: an agent commit is recorded as evidence but never becomes authority;
// the canonical commit still has parent base and the captured content.
func TestGTAgentCommitIsNotCanonical(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	backend, ws := preparedWorkspace(t, root, shas[1])
	req := captureRequest("ln-agent-commit", shas[1], "doc-agent")
	git := ws.(*gitWorkspace)

	writeFile(t, filepath.Join(git.dir, "agent-file.txt"), "agent work")
	runGit(t, git.dir, "add", "-A")
	runGit(t, git.dir, "commit", "-qm", "agent: my own commit")
	agentHead := strings.TrimSpace(runGit(t, git.dir, "rev-parse", "HEAD"))

	result, err := ws.Capture(ctx, req)
	if err != nil {
		t.Fatalf("Capture with agent commit: %v", err)
	}
	if !result.AgentHeadMoved {
		t.Fatal("agent head movement must be recorded")
	}
	if !result.Changed {
		t.Fatal("content differs from base, so the result must change")
	}
	if result.ResultSHA == agentHead {
		t.Fatal("the agent commit must not be adopted as the canonical result")
	}
	parents := strings.Fields(runGit(t, git.dir, "rev-list", "--parents", "-n", "1", result.ResultSHA))
	if len(parents) != 2 || parents[1] != shas[1] {
		t.Fatalf("canonical parent must stay the base: %v", parents)
	}
	if err := ws.VerifyAt(ctx, result.ResultSHA); err != nil {
		t.Fatalf("VerifyAt after agent commit capture: %v", err)
	}
	_ = backend
}

// GT: agent resets and checkouts are ignored as authority; content decides.
func TestGTAgentResetIgnoredAsAuthority(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	_, ws := preparedWorkspace(t, root, shas[1])
	req := captureRequest("ln-reset", shas[1], "doc-reset")
	git := ws.(*gitWorkspace)

	// The agent resets to the base commit, then edits content.
	runGit(t, git.dir, "reset", "-q", "--hard", shas[0])
	writeFile(t, filepath.Join(git.dir, "post-reset.txt"), "still real content")
	result, err := ws.Capture(ctx, req)
	if err != nil {
		t.Fatalf("Capture after agent reset: %v", err)
	}
	if !result.AgentHeadMoved {
		t.Fatal("reset moved the workspace head; evidence must record it")
	}
	if !result.Changed {
		t.Fatal("captured content differs from the launch base")
	}
	parents := strings.Fields(runGit(t, git.dir, "rev-list", "--parents", "-n", "1", result.ResultSHA))
	if len(parents) != 2 || parents[1] != req.BaseSHA {
		t.Fatalf("canonical parent must be the launch base despite the reset: %v", parents)
	}
}

// GT: no-change content produces changed=false and no result commit or ref.
func TestGTNoChangeCapture(t *testing.T) {
	ctx := context.Background()
	ws, req := captureProbe(t, "")
	result, err := ws.Capture(ctx, req)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if result.Changed {
		t.Fatalf("fresh worktree must capture as unchanged: %+v", result)
	}
	if result.ResultSHA != req.BaseSHA {
		t.Fatalf("unchanged result must report the base SHA, got %s", result.ResultSHA)
	}
	git := ws.(*gitWorkspace)
	if _, err := runGitChecked(t, git.dir, "rev-parse", "--verify", git.resultRef(req.LaunchID)); err == nil {
		t.Fatal("unchanged capture must not create a result ref")
	}
	if err := ws.VerifyAt(ctx, req.BaseSHA); err != nil {
		t.Fatalf("VerifyAt base after no-change capture: %v", err)
	}
}

// runGitChecked runs git and returns the error instead of failing the test.
func runGitChecked(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = scrubbedTestEnv(os.Environ())
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// GT: gitlinks never enter a canonical result.
func TestGTGitlinkRejected(t *testing.T) {
	ctx := context.Background()
	ws, req := captureProbe(t, "")
	git := ws.(*gitWorkspace)
	nested := filepath.Join(git.dir, "embedded-repo")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	runGit(t, nested, "init", "-q", ".")
	runGit(t, nested, "config", "user.email", "embedded@example.com")
	runGit(t, nested, "config", "user.name", "Embedded")
	writeFile(t, filepath.Join(nested, "inner.txt"), "inner")
	runGit(t, nested, "add", "-A")
	runGit(t, nested, "commit", "-qm", "inner")

	_, err := ws.Capture(ctx, req)
	var rejected *CaptureRejectedError
	if !errors.As(err, &rejected) || rejected.Reason == "" || !strings.Contains(rejected.Reason, "gitlink") {
		t.Fatalf("gitlink capture must be rejected: %v", err)
	}
	if !errors.Is(err, ErrCaptureRejected) {
		t.Fatalf("error must wrap ErrCaptureRejected: %v", err)
	}
}

// GT: forbidden .spynel paths never enter a canonical result.
func TestGTDotSpynelResultPathRejected(t *testing.T) {
	ctx := context.Background()
	ws, req := captureProbe(t, "")
	writeFile(t, filepath.Join(ws.ProviderView(), ".spynel", "evil-state.yaml"), "leak")
	_, err := ws.Capture(ctx, req)
	var rejected *CaptureRejectedError
	if !errors.As(err, &rejected) || !strings.Contains(rejected.Reason, ".spynel") {
		t.Fatalf(".spynel result path must be rejected: %v", err)
	}
	if !errors.Is(err, ErrCaptureRejected) {
		t.Fatalf("error must wrap ErrCaptureRejected: %v", err)
	}
}

// GT: the create-only result ref makes captures idempotent: the same launch,
// base, tree, identity, and terminal time reproduce the same SHA and adopt
// the existing ref; a conflicting ref value fails closed.
func TestGTResultRefRecoveryAndAdoption(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	_, ws := preparedWorkspace(t, root, shas[1])
	req := captureRequest("ln-retry", shas[1], "doc-retry")
	writeFile(t, filepath.Join(ws.ProviderView(), "stable.txt"), "deterministic content")
	first, err := ws.Capture(ctx, req)
	if err != nil {
		t.Fatalf("first Capture: %v", err)
	}

	// A fresh workspace for the same launch reproduces the same canonical SHA.
	_, ws2 := preparedWorkspace(t, root, shas[1])
	writeFile(t, filepath.Join(ws2.ProviderView(), "stable.txt"), "deterministic content")
	second, err := ws2.Capture(ctx, req)
	if err != nil {
		t.Fatalf("retry Capture: %v", err)
	}
	if second.ResultSHA != first.ResultSHA {
		t.Fatalf("retry must reproduce the same canonical SHA: %s vs %s", second.ResultSHA, first.ResultSHA)
	}
	if !second.Adopted {
		t.Fatal("retry must adopt the existing identical result ref")
	}

	// An equivalent canonical result (same tree, same base) is adoptable.
	git := ws.(*gitWorkspace)
	conflictReq := req
	conflictReq.LaunchID = "ln-conflict"
	writeFile(t, filepath.Join(git.dir, "other.txt"), "other content")
	conflict, err := ws.Capture(ctx, conflictReq)
	if err != nil {
		t.Fatalf("conflict probe capture: %v", err)
	}
	// Force a conflicting ref value at the first launch's ref.
	if _, err := runGitChecked(t, git.dir, "update-ref", git.resultRef(req.LaunchID), conflict.ResultSHA); err != nil {
		t.Fatalf("force conflicting ref: %v", err)
	}
	_, err = ws2.Capture(ctx, req)
	var rejected *CaptureRejectedError
	if !errors.As(err, &rejected) || !strings.Contains(rejected.Reason, "ref_conflict") {
		t.Fatalf("conflicting ref must fail closed: %v", err)
	}
}

// F5: when recovery computes a different commit with equivalent canonical
// content, the existing create-only result ref remains authoritative.
func TestGTEquivalentAdoptionReturnsExistingRefSHA(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	_, firstWorkspace := preparedWorkspace(t, root, shas[1])
	firstRequest := captureRequest("ln-equivalent-adoption", shas[1], "doc-equivalent")
	writeFile(t, filepath.Join(firstWorkspace.ProviderView(), "equivalent.txt"), "same tree")
	first, err := firstWorkspace.Capture(ctx, firstRequest)
	if err != nil {
		t.Fatal(err)
	}

	_, retryWorkspace := preparedWorkspace(t, root, shas[1])
	retryRequest := firstRequest
	retryRequest.At = retryRequest.At.Add(time.Second)
	writeFile(t, filepath.Join(retryWorkspace.ProviderView(), "equivalent.txt"), "same tree")
	gitWorkspace := retryWorkspace.(*gitWorkspace)
	name, email := gitWorkspace.gitIdentity(ctx)
	stamp := fmt.Sprintf("%d +0000", retryRequest.At.Unix())
	candidateOut, err := gitWorkspace.runner().runStdin(ctx, gitWorkspace.dir, []byte(canonicalCommitMessage(retryRequest)), []string{
		"GIT_AUTHOR_NAME=" + name, "GIT_AUTHOR_EMAIL=" + email,
		"GIT_COMMITTER_NAME=" + name, "GIT_COMMITTER_EMAIL=" + email,
		"GIT_AUTHOR_DATE=" + stamp, "GIT_COMMITTER_DATE=" + stamp,
	}, "commit-tree", first.TreeSHA, "-p", retryRequest.BaseSHA)
	if err != nil {
		t.Fatal(err)
	}
	candidate := strings.TrimSpace(string(candidateOut))
	if candidate == first.ResultSHA {
		t.Fatal("test setup did not produce distinct equivalent commit metadata")
	}

	// Restore the uncaptured edit after the candidate probe and exercise the
	// public capture path, which must adopt and return the existing ref SHA.
	writeFile(t, filepath.Join(retryWorkspace.ProviderView(), "equivalent.txt"), "same tree")
	retry, err := retryWorkspace.Capture(ctx, retryRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Adopted {
		t.Fatal("equivalent recapture did not report adoption")
	}
	if retry.ResultSHA != first.ResultSHA {
		t.Fatalf("capture returned candidate %s, want authoritative ref %s", retry.ResultSHA, first.ResultSHA)
	}
	if ref := strings.TrimSpace(runGit(t, root, "rev-parse", gitWorkspace.resultRef(firstRequest.LaunchID))); ref != retry.ResultSHA {
		t.Fatalf("result ref %s differs from returned result %s", ref, retry.ResultSHA)
	}
	if err := retryWorkspace.VerifyAt(ctx, first.ResultSHA); err != nil {
		t.Fatalf("VerifyAt must use authoritative ref SHA: %v", err)
	}
}

// GT: VerifyAt rejects both a moved HEAD and nonignored source mutations.
func TestGTVerifyAtMutationRejection(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	_, ws := preparedWorkspace(t, root, shas[1])
	writeFile(t, filepath.Join(ws.ProviderView(), "edit.txt"), "captured")
	req := captureRequest("ln-verify", shas[1], "doc-verify")
	result, err := ws.Capture(ctx, req)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	// Ignored outputs may remain without breaking verification.
	writeFile(t, filepath.Join(ws.ProviderView(), "build", "out.log"), "ignored only when .gitignore matches; untracked here but not part of R")
	// Untracked files DO break clean verification because they are not part
	// of R; remove the probe file again.
	if err := os.RemoveAll(filepath.Join(ws.ProviderView(), "build")); err != nil {
		t.Fatalf("cleanup probe: %v", err)
	}
	if err := ws.VerifyAt(ctx, result.ResultSHA); err != nil {
		t.Fatalf("VerifyAt must pass right after capture: %v", err)
	}
	writeFile(t, filepath.Join(ws.ProviderView(), "edit.txt"), "mutated after capture")
	if err := ws.VerifyAt(ctx, result.ResultSHA); !errors.Is(err, ErrVerifyFailed) {
		t.Fatalf("mutation must fail VerifyAt: %v", err)
	}
	// Restore content; a moved HEAD must also fail verification.
	writeFile(t, filepath.Join(ws.ProviderView(), "edit.txt"), "captured")
	runGit(t, ws.(*gitWorkspace).dir, "reset", "-q", "--hard", shas[1])
	// reset --hard leaves the workspace at base with clean content; verifying
	// against the result must fail because the head moved.
	if err := ws.VerifyAt(ctx, result.ResultSHA); err == nil {
		t.Fatal("moved head must fail VerifyAt")
	}
}

// GT: commit signing configured in the repository cannot break canonical
// commits; Spynel disables signing for its own Git operations.
func TestGTCommitSigningDisabled(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	runGit(t, root, "config", "commit.gpgsign", "true")
	_, ws := preparedWorkspace(t, root, shas[1])
	writeFile(t, filepath.Join(ws.ProviderView(), "signed-env.txt"), "content")
	result, err := ws.Capture(ctx, captureRequest("ln-signing", shas[1], "doc-signing"))
	if err != nil {
		t.Fatalf("Capture with repo commit.gpgsign=true: %v", err)
	}
	if !result.Changed {
		t.Fatal("capture must produce a change")
	}
}

// GT: a merge in progress rejects the capture.
func TestGTMergeInProgressRejected(t *testing.T) {
	ctx := context.Background()
	root, _, shas := testRepo(t)
	_, ws := preparedWorkspace(t, root, shas[1])
	git := ws.(*gitWorkspace)
	// Create a real divergence and start a merge that stops on conflict.
	runGit(t, root, "checkout", "-q", "-b", "diverged")
	writeFile(t, filepath.Join(root, "conflict.txt"), "branch side")
	runGit(t, root, "add", "-A")
	runGit(t, root, "commit", "-qm", "diverged")
	runGit(t, root, "checkout", "-q", "main")
	writeFile(t, filepath.Join(root, "conflict.txt"), "main side")
	runGit(t, root, "add", "-A")
	runGit(t, root, "commit", "-qm", "main side")
	if _, err := runGitChecked(t, git.dir, "merge", "--no-commit", "--no-ff", "diverged"); err == nil {
		t.Skip("merge auto-resolved; marker probe needs a real conflict")
	}
	if _, err := os.Stat(filepath.Join(root, ".git", "worktrees", git.ref.ID, "MERGE_HEAD")); err != nil {
		t.Fatalf("expected merge state marker: %v", err)
	}
	_, err := ws.Capture(ctx, captureRequest("ln-merge", shas[1], "doc-merge"))
	var rejected *CaptureRejectedError
	if !errors.As(err, &rejected) || rejected.Reason != "merge_in_progress" {
		t.Fatalf("merge in progress must reject capture: %v", err)
	}
	// Abort so later cleanup stays possible.
	_, _ = runGitChecked(t, git.dir, "merge", "--abort")
}
