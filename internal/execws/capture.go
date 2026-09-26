package execws

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// gitWorkspace is one prepared detached Git worktree bound to its backend.
type gitWorkspace struct {
	backend     *LocalGit
	ref         Ref
	dir         string
	providerDir string
}

func (w *gitWorkspace) Ref() Ref             { return w.ref }
func (w *gitWorkspace) ProviderView() string { return w.providerDir }

func (w *gitWorkspace) runner() gitRunner { return w.backend.runner() }

// resultRef is the per-launch create-only canonical result reference.
func (w *gitWorkspace) resultRef(launchID string) string {
	return "refs/spynel/launches/" + launchID + "/result"
}

// mergeStateMarkers are the Git metadata files whose presence proves an
// unfinished merge/cherry-pick/rebase/revert/bisect. Working content captured
// mid-operation is not a valid result.
var mergeStateMarkers = []string{
	"MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "BISECT_LOG",
	"rebase-merge", "rebase-apply",
}

// rejectMergeState fails closed while any sequencer or bisect operation is in
// progress inside the workspace.
func (w *gitWorkspace) rejectMergeState(ctx context.Context) error {
	git := w.runner()
	gitDirOut, err := git.run(ctx, w.dir, nil, "rev-parse", "--git-dir")
	if err != nil {
		return err
	}
	gitDir := strings.TrimSpace(string(gitDirOut.stdout))
	if !filepath.IsAbs(gitDir) {
		gitDir = filepath.Join(w.dir, gitDir)
	}
	for _, marker := range mergeStateMarkers {
		if _, err := os.Lstat(filepath.Join(gitDir, marker)); err == nil {
			return &CaptureRejectedError{Reason: "merge_in_progress"}
		}
	}
	return nil
}

// VerifyAt proves the workspace HEAD is exactly sha and the nonignored
// working tree is clean relative to it. Ignored build outputs may remain.
func (w *gitWorkspace) VerifyAt(ctx context.Context, sha string) error {
	git := w.runner()
	head, err := git.run(ctx, w.dir, nil, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(head.stdout)) != sha {
		return &VerifyError{Reason: fmt.Sprintf("head is %s, want %s", shortSHA(strings.TrimSpace(string(head.stdout))), shortSHA(sha))}
	}
	if err := w.backend.assertCleanWorktree(ctx, w.dir); err != nil {
		return &VerifyError{Reason: "nonignored source changed"}
	}
	return nil
}

// captureIndexRejects scans one complete temporary index for gitlink entries
// and forbidden .spynel result paths inside the capture scope.
func (w *gitWorkspace) captureIndexRejects(ctx context.Context, indexFile string, scopePrefix string) error {
	git := w.runner()
	env := []string{"GIT_INDEX_FILE=" + indexFile}
	out, err := git.run(ctx, w.dir, env, "ls-files", "-s", "-z")
	if err != nil {
		return err
	}
	for _, record := range bytes.Split(out.stdout, []byte{0}) {
		if len(record) == 0 {
			continue
		}
		parts := bytes.SplitN(record, []byte("\t"), 2)
		if len(parts) != 2 {
			continue
		}
		path := string(parts[1])
		if scopePrefix != "" && !strings.HasPrefix(path, scopePrefix+"/") {
			continue
		}
		mode := string(bytes.Fields(parts[0])[0])
		if mode == "160000" {
			return &CaptureRejectedError{Reason: fmt.Sprintf("gitlink at %s; submodules cannot enter canonical results", path)}
		}
		if forbiddenSpynelPath(path, scopePrefix) {
			return &CaptureRejectedError{Reason: fmt.Sprintf("forbidden .spynel result path at %s", path)}
		}
	}
	return nil
}

// forbiddenSpynelPath reports whether a captured repo path lies inside the
// workspace state directory for this capture scope.
func forbiddenSpynelPath(repoPath, scopePrefix string) bool {
	rel := repoPath
	if scopePrefix != "" {
		if !strings.HasPrefix(repoPath, scopePrefix+"/") {
			return false
		}
		rel = strings.TrimPrefix(repoPath, scopePrefix+"/")
	}
	return rel == ".spynel" || strings.HasPrefix(rel, ".spynel/")
}

// Capture turns the provider's working content into the canonical result of
// one launch. The agent's Git HEAD and index are never authority: content is
// captured through a Spynel-controlled temporary index and committed with
// parent base_sha, so agent commits, resets, or checkouts do not reject
// otherwise valid work.
func (w *gitWorkspace) Capture(ctx context.Context, req CaptureRequest) (CaptureResult, error) {
	if req.LaunchID == "" || req.BaseSHA == "" {
		return CaptureResult{}, fmt.Errorf("execws: capture requires a launch id and base sha")
	}
	if err := w.rejectMergeState(ctx); err != nil {
		return CaptureResult{}, err
	}
	git := w.runner()

	// Agent head movement is recorded as evidence, never as authority.
	headOut, err := git.run(ctx, w.dir, nil, "rev-parse", "HEAD")
	if err != nil {
		return CaptureResult{}, err
	}
	agentHead := strings.TrimSpace(string(headOut.stdout))
	agentHeadMoved := agentHead != req.BaseSHA

	launchDir := w.backend.launchDir(req.LaunchID)
	if err := os.MkdirAll(launchDir, 0o700); err != nil {
		return CaptureResult{}, err
	}
	indexFile := filepath.Join(launchDir, "capture.index")
	_ = os.Remove(indexFile)

	env := []string{"GIT_INDEX_FILE=" + indexFile}
	if _, err := git.run(ctx, w.dir, env, "read-tree", req.BaseSHA); err != nil {
		return CaptureResult{}, err
	}
	// The pathspec "." is relative to the provider view, so a workspace root
	// below the repository top scopes the capture to that subtree.
	if _, err := git.run(ctx, w.providerDir, env, "add", "-A", "--", "."); err != nil {
		return CaptureResult{}, err
	}
	scopePrefix := w.captureScopePrefix()
	if err := w.captureIndexRejects(ctx, indexFile, scopePrefix); err != nil {
		return CaptureResult{}, err
	}
	treeOut, err := git.run(ctx, w.dir, env, "write-tree")
	if err != nil {
		return CaptureResult{}, err
	}
	treeSHA := strings.TrimSpace(string(treeOut.stdout))

	baseTreeOut, err := git.run(ctx, w.dir, nil, "rev-parse", req.BaseSHA+"^{tree}")
	if err != nil {
		return CaptureResult{}, err
	}
	baseTree := strings.TrimSpace(string(baseTreeOut.stdout))

	result := CaptureResult{TreeSHA: treeSHA, AgentHeadMoved: agentHeadMoved, ResultSHA: req.BaseSHA}
	if treeSHA == baseTree {
		result.Changed = false
		// No result commit or ref for unchanged content; the workspace moves
		// back to the base so verification sees a clean authoritative state.
		if _, err := git.run(ctx, w.dir, nil, "reset", "-q", req.BaseSHA); err != nil {
			return CaptureResult{}, err
		}
		if err := w.VerifyAt(ctx, req.BaseSHA); err != nil {
			return CaptureResult{}, err
		}
		return result, nil
	}
	result.Changed = true

	resultSHA, adopted, err := w.commitCanonical(ctx, req, treeSHA)
	if err != nil {
		return CaptureResult{}, err
	}
	result.ResultSHA = resultSHA
	result.Adopted = adopted
	if paths, err := w.changedPaths(ctx, req.BaseSHA, resultSHA); err == nil {
		result.ChangedPaths = paths
	}
	// Move the workspace to the canonical result (mixed reset: index adopts
	// R while the working content already matches it) and verify.
	if _, err := git.run(ctx, w.dir, nil, "reset", "-q", resultSHA); err != nil {
		return CaptureResult{}, err
	}
	if err := w.VerifyAt(ctx, resultSHA); err != nil {
		return CaptureResult{}, err
	}
	return result, nil
}

// captureScopePrefix is the repository path prefix of the provider view; ""
// means the provider sees the whole worktree.
func (w *gitWorkspace) captureScopePrefix() string {
	rel, err := filepath.Rel(w.dir, w.providerDir)
	if err != nil || rel == "." {
		return ""
	}
	return filepath.ToSlash(rel)
}

// commitCanonical creates the deterministic canonical commit from the captured
// tree and publishes the launch's create-only result ref.
func (w *gitWorkspace) commitCanonical(ctx context.Context, req CaptureRequest, treeSHA string) (string, bool, error) {
	git := w.runner()
	name, email := w.gitIdentity(ctx)
	stamp := fmt.Sprintf("%d +0000", req.At.Unix())
	identity := []string{
		"GIT_AUTHOR_NAME=" + name, "GIT_AUTHOR_EMAIL=" + email,
		"GIT_COMMITTER_NAME=" + name, "GIT_COMMITTER_EMAIL=" + email,
		"GIT_AUTHOR_DATE=" + stamp, "GIT_COMMITTER_DATE=" + stamp,
	}
	message := canonicalCommitMessage(req)
	out, err := git.runStdin(ctx, w.dir, []byte(message), identity, "commit-tree", treeSHA, "-p", req.BaseSHA)
	if err != nil {
		return "", false, err
	}
	resultSHA := strings.TrimSpace(string(out))
	if resultSHA == "" {
		return "", false, fmt.Errorf("git commit-tree produced no commit id")
	}
	authoritativeSHA, adopted, err := w.publishResultRef(ctx, req.LaunchID, resultSHA, treeSHA, req.BaseSHA)
	if err != nil {
		return "", false, err
	}
	return authoritativeSHA, adopted, nil
}

// canonicalCommitMessage renders the bounded title and durable trailers.
func canonicalCommitMessage(req CaptureRequest) string {
	title := strings.TrimSpace(req.Title)
	title = strings.ReplaceAll(title, "\r", " ")
	if index := strings.IndexByte(title, '\n'); index >= 0 {
		title = strings.TrimSpace(title[:index])
	}
	if len(title) > 100 {
		title = title[:100]
	}
	if title == "" {
		title = "task result"
	}
	return fmt.Sprintf("spynel: %s\n\nSpynel-Task: %s\nSpynel-Launch: %s\nSpynel-Base: %s\n", title, req.TaskDoc, req.LaunchID, req.BaseSHA)
}

// gitIdentity resolves the repository's configured identity, falling back to
// a deterministic Spynel identity so retries reproduce the same commit.
func (w *gitWorkspace) gitIdentity(ctx context.Context) (string, string) {
	git := w.runner()
	nameOut, nameErr := git.run(ctx, w.dir, nil, "config", "--get", "user.name")
	emailOut, emailErr := git.run(ctx, w.dir, nil, "config", "--get", "user.email")
	name := strings.TrimSpace(string(nameOut.stdout))
	email := strings.TrimSpace(string(emailOut.stdout))
	if nameErr != nil || emailErr != nil || name == "" || email == "" {
		return "Spynel", "spynel@localhost"
	}
	return name, email
}

// publishResultRef moves the launch's create-only result ref to resultSHA.
// An existing identical ref is adopted; an existing equivalent canonical
// result (same tree, same base parent) is adopted as the canonical result.
// Any other value fails closed with a ref conflict.
func (w *gitWorkspace) publishResultRef(ctx context.Context, launchID, resultSHA, treeSHA, baseSHA string) (string, bool, error) {
	git := w.runner()
	ref := w.resultRef(launchID)
	zero := strings.Repeat("0", w.backend.objectFormatLength(ctx))
	if _, err := git.run(ctx, w.dir, nil, "update-ref", ref, resultSHA, zero); err == nil {
		return resultSHA, false, nil
	}
	existingOut, err := git.run(ctx, w.dir, nil, "rev-parse", "--verify", ref)
	if err != nil {
		return "", false, &CaptureRejectedError{Reason: "ref_conflict: cannot read existing result ref"}
	}
	existing := strings.TrimSpace(string(existingOut.stdout))
	if existing == resultSHA {
		return existing, true, nil
	}
	if w.sameCanonicalResult(ctx, existing, treeSHA, baseSHA) {
		return existing, true, nil
	}
	return "", false, &CaptureRejectedError{Reason: fmt.Sprintf("ref_conflict: refs/spynel/launches/%s/result is %s, want %s", launchID, shortSHA(existing), shortSHA(resultSHA))}
}

// sameCanonicalResult independently validates an existing ref value as the
// same canonical result: identical tree content with the same base parent.
func (w *gitWorkspace) sameCanonicalResult(ctx context.Context, existing, treeSHA, baseSHA string) bool {
	git := w.runner()
	treeOut, err := git.run(ctx, w.dir, nil, "rev-parse", existing+"^{tree}")
	if err != nil || strings.TrimSpace(string(treeOut.stdout)) != treeSHA {
		return false
	}
	parentsOut, err := git.run(ctx, w.dir, nil, "rev-list", "--parents", "-n", "1", existing)
	if err != nil {
		return false
	}
	fields := strings.Fields(string(parentsOut.stdout))
	if len(fields) != 2 || fields[1] != baseSHA {
		return false
	}
	return true
}

func (w *gitWorkspace) changedPaths(ctx context.Context, baseSHA, resultSHA string) ([]string, error) {
	git := w.runner()
	out, err := git.run(ctx, w.dir, nil, "diff-tree", "--no-commit-id", "--name-only", "-r", "-z", baseSHA, resultSHA)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, path := range bytes.Split(out.stdout, []byte{0}) {
		if len(path) == 0 {
			continue
		}
		paths = append(paths, string(path))
		if len(paths) >= 256 {
			break
		}
	}
	return paths, nil
}

func boundedTail(data []byte, limit int) string {
	if len(data) > limit {
		data = data[len(data)-limit:]
	}
	return strings.TrimSpace(string(data))
}

// objectFormatLength resolves the repository object id length (40 for SHA-1,
// 64 for SHA-256) for create-only zero-id CAS writes.
func (b *LocalGit) objectFormatLength(ctx context.Context) int {
	out, err := b.runner().run(ctx, b.root, nil, "rev-parse", "--show-object-format")
	if err != nil {
		return 40
	}
	format := strings.TrimSpace(string(out.stdout))
	if format == "sha256" {
		return 64
	}
	return 40
}
