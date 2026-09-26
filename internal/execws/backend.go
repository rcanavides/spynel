package execws

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// LocalGit implements the git-worktree backend against one workspace root,
// which may be the repository top or a subdirectory of it.
type LocalGit struct {
	root string

	// process-local integration serialization; the cross-process lock lives at
	// .spynel/runtime/locks/integration.lock.
	integrateMu sync.Mutex
}

// NewLocalGit binds one backend to an absolute workspace root. The root must
// lie inside a non-bare Git working tree; Preflight verifies that before any
// isolated claim uses it.
func NewLocalGit(root string) (*LocalGit, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return &LocalGit{root: absolute}, nil
}

func (b *LocalGit) Kind() string { return BackendGitWorktree }

func (b *LocalGit) stateDir(parts ...string) string {
	return filepath.Join(append([]string{b.root, ".spynel"}, parts...)...)
}

func (b *LocalGit) worktreeDir(id string) string { return b.stateDir("worktrees", id) }
func (b *LocalGit) launchDir(id string) string   { return b.stateDir("runtime", "launches", id) }
func (b *LocalGit) emptyHooksPath() string       { return b.stateDir("runtime", "empty-hooks") }
func (b *LocalGit) integrationLockPath() string {
	return b.stateDir("runtime", "locks", "integration.lock")
}

func (b *LocalGit) runner() gitRunner {
	return gitRunner{hooksPath: b.emptyHooksPath()}
}

// ensureRuntimeDirectories prepares the execution-plane state directories.
func (b *LocalGit) ensureRuntimeDirectories() error {
	for _, dir := range []string{
		b.stateDir("worktrees"),
		b.stateDir("runtime", "launches"),
		b.stateDir("runtime", "locks"),
		b.emptyHooksPath(),
	} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	return nil
}

// minimumGitMajor/minor pins the worktree features Spynel relies on
// (--detach --lock --reason on worktree add).
const (
	minimumGitMajor = 2
	minimumGitMinor = 25
)

// Preflight proves this root can host isolated implementation and review
// launches and captures the exact repository facts the claim freezes. It never
// falls back to shared mode: a failed preflight leaves the work eligible and
// unclaimed with the reason surfaced.
func (b *LocalGit) Preflight(ctx context.Context) (PreflightReport, error) {
	return b.preflight(ctx)
}

func (b *LocalGit) preflight(ctx context.Context) (PreflightReport, error) {
	report := PreflightReport{}
	if _, err := exec.LookPath("git"); err != nil {
		return report, &PreflightError{Reason: "git_missing", Detail: err.Error()}
	}
	git := b.runner()
	version, err := git.run(ctx, b.root, nil, "version")
	if err != nil {
		return report, &PreflightError{Reason: "git_missing", Detail: "git version: " + err.Error()}
	}
	if err := supportedGitVersion(strings.TrimSpace(string(version.stdout))); err != nil {
		return report, &PreflightError{Reason: "git_version", Detail: err.Error()}
	}
	inside, err := git.run(ctx, b.root, nil, "rev-parse", "--is-inside-work-tree")
	bare, bareErr := git.run(ctx, b.root, nil, "rev-parse", "--is-bare-repository")
	if bareErr == nil && strings.TrimSpace(string(bare.stdout)) == "true" {
		return report, &PreflightError{Reason: "repo_bare"}
	}
	if err != nil || strings.TrimSpace(string(inside.stdout)) != "true" {
		return report, &PreflightError{Reason: "root_outside_worktree"}
	}
	branch, err := git.run(ctx, b.root, nil, "symbolic-ref", "-q", "HEAD")
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(branch.stdout)), "refs/heads/") {
		return report, &PreflightError{Reason: "root_detached"}
	}
	if b.spynelTracked(ctx, git) {
		return report, &PreflightError{Reason: "spynel_tracked"}
	}
	top, err := git.run(ctx, b.root, nil, "rev-parse", "--show-toplevel")
	if err != nil {
		return report, &PreflightError{Reason: "root_outside_worktree", Detail: err.Error()}
	}
	head, err := git.run(ctx, b.root, nil, "rev-parse", "HEAD")
	if err != nil {
		return report, &PreflightError{Reason: "root_outside_worktree", Detail: err.Error()}
	}
	repoTop := strings.TrimSpace(string(top.stdout))
	report.RepoTop = repoTop
	report.RootRel = relativeInside(b.root, repoTop)
	report.TargetRef = strings.TrimSpace(string(branch.stdout))
	report.TargetOld = strings.TrimSpace(string(head.stdout))
	if err := b.ensureRuntimeDirectories(); err != nil {
		return PreflightReport{}, err
	}
	return report, nil
}

// spynelTracked reports whether the workspace state directory is tracked in
// the root checkout, which would leak live state into every result.
func (b *LocalGit) spynelTracked(ctx context.Context, git gitRunner) bool {
	index, err := git.run(ctx, b.root, nil, "ls-files", "--", ".spynel")
	if err == nil && len(strings.TrimSpace(string(index.stdout))) > 0 {
		return true
	}
	committed, err := git.run(ctx, b.root, nil, "ls-tree", "-r", "--name-only", "HEAD", "--", ".spynel")
	return err == nil && len(strings.TrimSpace(string(committed.stdout))) > 0
}

func supportedGitVersion(version string) error {
	// Formats: "git version 2.39.2" and "git version 2.39.2 (Apple Git-157)".
	fields := strings.Fields(version)
	if len(fields) < 3 || fields[0] != "git" || fields[1] != "version" {
		return fmt.Errorf("unrecognized git version output %q", version)
	}
	parts := strings.SplitN(fields[2], ".", 3)
	if len(parts) < 2 {
		return fmt.Errorf("unrecognized git version %q", version)
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	if majorErr != nil || minorErr != nil {
		return fmt.Errorf("unrecognized git version %q", version)
	}
	if major < minimumGitMajor || (major == minimumGitMajor && minor < minimumGitMinor) {
		return fmt.Errorf("git %d.%d is older than the required %d.%d", major, minor, minimumGitMajor, minimumGitMinor)
	}
	return nil
}

// relativeInside returns the slash path of root relative to top; "." when
// equal. root must live inside top.
func relativeInside(root, top string) string {
	rel, err := filepath.Rel(top, root)
	if err != nil || rel == "." {
		return "."
	}
	return filepath.ToSlash(rel)
}

// worktreeEntry is one parsed block of `git worktree list --porcelain`.
type worktreeEntry struct {
	Path     string
	Head     string
	Branch   string
	Bare     bool
	Detached bool
	Locked   bool
}

func (b *LocalGit) worktrees(ctx context.Context) ([]worktreeEntry, error) {
	git := b.runner()
	out, err := git.run(ctx, b.root, nil, "worktree", "list", "--porcelain")
	if err != nil {
		return nil, err
	}
	return parseWorktreeList(string(out.stdout)), nil
}

func parseWorktreeList(data string) []worktreeEntry {
	var entries []worktreeEntry
	var current *worktreeEntry
	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case strings.HasPrefix(line, "worktree "):
			if current != nil {
				entries = append(entries, *current)
			}
			current = &worktreeEntry{Path: strings.TrimPrefix(line, "worktree ")}
		case current == nil:
			continue
		case strings.HasPrefix(line, "HEAD "):
			current.Head = strings.TrimPrefix(line, "HEAD ")
		case strings.HasPrefix(line, "branch "):
			current.Branch = strings.TrimPrefix(line, "branch ")
		case line == "bare":
			current.Bare = true
		case line == "detached":
			current.Detached = true
		case strings.HasPrefix(line, "locked"):
			current.Locked = true
		}
	}
	if current != nil {
		entries = append(entries, *current)
	}
	return entries
}

// Prepare creates the detached locked worktree for one spec, reusing it when
// the same workspace identity is already registered clean at the same start
// SHA. Conflicting state fails closed with ErrWorkspaceConflict.
func (b *LocalGit) Prepare(ctx context.Context, spec Spec) error {
	if spec.ID == "" || spec.StartSHA == "" {
		return fmt.Errorf("execws: prepare requires a workspace id and start sha")
	}
	path := b.worktreeDir(spec.ID)
	git := b.runner()
	entries, err := b.worktrees(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !samePath(entry.Path, path) {
			continue
		}
		if entry.Head != spec.StartSHA {
			return &ConflictError{Reason: fmt.Sprintf("workspace %s is registered at %s, want %s", spec.ID, shortSHA(entry.Head), shortSHA(spec.StartSHA))}
		}
		if err := b.assertCleanWorktree(ctx, path); err != nil {
			return &ConflictError{Reason: fmt.Sprintf("workspace %s is registered but not clean: %v", spec.ID, err)}
		}
		return nil
	}
	if info, err := os.Lstat(path); err == nil {
		_ = info
		return &ConflictError{Reason: fmt.Sprintf("workspace path %s exists without a registered worktree", path)}
	} else if !os.IsNotExist(err) {
		return err
	}
	if _, err := git.run(ctx, b.root, nil, "worktree", "add", "--detach", "--lock", "--reason", "spynel "+spec.ID, path, spec.StartSHA); err != nil {
		return err
	}
	created, err := git.run(ctx, path, nil, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(created.stdout)) != spec.StartSHA {
		return &ConflictError{Reason: "prepared worktree checked out the wrong start commit"}
	}
	return nil
}

func (b *LocalGit) assertCleanWorktree(ctx context.Context, path string) error {
	status, err := b.runner().run(ctx, path, nil, "status", "--porcelain")
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(status.stdout))) > 0 {
		return fmt.Errorf("nonignored working tree changes present")
	}
	return nil
}

// Open binds one existing workspace. The caller must have prepared it.
func (b *LocalGit) Open(ctx context.Context, ref Ref) (Workspace, error) {
	if ref.Kind != "" && ref.Kind != BackendGitWorktree {
		return nil, fmt.Errorf("execws: unknown workspace kind %q", ref.Kind)
	}
	path := b.worktreeDir(ref.ID)
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrWorkspaceGone, path)
		}
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: %s is not a directory", ErrWorkspaceGone, path)
	}
	rootRel, err := b.relativeRoot(ctx)
	if err != nil {
		return nil, err
	}
	provider := path
	if rootRel != "." {
		provider = filepath.Join(path, filepath.FromSlash(rootRel))
	}
	return &gitWorkspace{backend: b, ref: Ref{ID: ref.ID, Kind: BackendGitWorktree}, dir: path, providerDir: provider}, nil
}

// relativeRoot resolves the workspace root's position inside the repository,
// computing it on first use when Preflight did not already publish it.
func (b *LocalGit) relativeRoot(ctx context.Context) (string, error) {
	report, err := b.Preflight(ctx)
	if err != nil {
		// A backend whose preflight fails cannot open isolated workspaces.
		return "", err
	}
	return report.RootRel, nil
}

// Remove deletes one owned worktree idempotently, tolerating partially
// completed previous cleanup. It never touches directories outside the exact
// owned workspace path.
func (b *LocalGit) Remove(ctx context.Context, ref Ref) error {
	if ref.Kind != "" && ref.Kind != BackendGitWorktree {
		return fmt.Errorf("execws: unknown workspace kind %q", ref.Kind)
	}
	path := b.worktreeDir(ref.ID)
	git := b.runner()
	// A stale lock must not block removing our own workspace.
	_, _ = git.run(ctx, b.root, nil, "worktree", "unlock", path)
	if _, err := git.run(ctx, b.root, nil, "worktree", "remove", "--force", path); err != nil {
		entries, listErr := b.worktrees(ctx)
		if listErr != nil {
			return listErr
		}
		registered := false
		for _, entry := range entries {
			if samePath(entry.Path, path) {
				registered = true
				break
			}
		}
		if registered {
			return err
		}
		// Not registered: tolerate (already removed or interrupted cleanup).
	}
	if _, err := git.run(ctx, b.root, nil, "worktree", "prune"); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("execws: workspace path %s is not a directory", path)
		}
		// The exact owned path remains after prune: interrupted cleanup left
		// an unregistered directory. Only this identity's path is removed.
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

// List reports every worktree registered below .spynel/worktrees.
func (b *LocalGit) List(ctx context.Context) ([]Ref, error) {
	entries, err := b.worktrees(ctx)
	if err != nil {
		return nil, err
	}
	base := b.stateDir("worktrees")
	var refs []Ref
	for _, entry := range entries {
		rel, relErr := filepath.Rel(base, entry.Path)
		if relErr != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			continue
		}
		refs = append(refs, Ref{ID: filepath.ToSlash(rel), Kind: BackendGitWorktree})
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].ID < refs[j].ID })
	return refs, nil
}

func (b *LocalGit) Integrator() Integrator { return b }

func samePath(a, b string) bool {
	if a == b {
		return true
	}
	absA, errA := filepath.Abs(a)
	absB, errB := filepath.Abs(b)
	return errA == nil && errB == nil && filepath.Clean(absA) == filepath.Clean(absB)
}

func shortSHA(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}
