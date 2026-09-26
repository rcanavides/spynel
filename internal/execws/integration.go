package execws

import (
	"context"
	"fmt"
	"strings"
)

// Integrate moves the repository target ref to one approved result SHA.
//
// Serialization: one integration at a time per repository through the
// cross-process lock at .spynel/runtime/locks/integration.lock plus the
// backend's process-local mutex.
//
// Safety: the target's current value must still equal the captured
// target_old, target_old must be an ancestor of the result (ff-only), and the
// mechanism adapts to where the target is checked out:
//
//   - root checkout: working-tree-aware `git merge --ff-only`, verified to
//     land exactly at R; a failure means user edits would be overwritten and
//     blocks retryably without touching them;
//   - checked out in any other worktree: ErrIntegrationBlocked
//     {checked_out_elsewhere};
//   - not checked out: CAS `git update-ref <ref> <R> <old>`.
//
// A target already at R is observed without a second movement. Never rebase,
// never force, never merge diverged history, never create merge commits.
func (b *LocalGit) Integrate(ctx context.Context, target Target, resultSHA string) (IntegrationResult, error) {
	if target.Ref == "" || target.OldSHA == "" || resultSHA == "" {
		return IntegrationResult{}, fmt.Errorf("execws: integrate requires target ref, old sha, and result sha")
	}
	b.integrateMu.Lock()
	defer b.integrateMu.Unlock()
	unlock, err := lockFile(b.integrationLockPath())
	if err != nil {
		return IntegrationResult{}, err
	}
	defer unlock()

	git := b.runner()
	currentOut, err := git.run(ctx, b.root, nil, "rev-parse", target.Ref)
	if err != nil {
		return IntegrationResult{}, err
	}
	current := strings.TrimSpace(string(currentOut.stdout))
	if current == resultSHA {
		return IntegrationResult{NewSHA: current, Mechanism: IntegrationObserved}, nil
	}
	if current != target.OldSHA {
		return IntegrationResult{}, &IntegrationAdvancedError{Reason: fmt.Sprintf("target %s is %s, expected %s", target.Ref, shortSHA(current), shortSHA(target.OldSHA))}
	}
	if _, err := git.run(ctx, b.root, nil, "merge-base", "--is-ancestor", target.OldSHA, resultSHA); err != nil {
		return IntegrationResult{}, &IntegrationAdvancedError{Reason: fmt.Sprintf("result %s is not a fast-forward of %s", shortSHA(resultSHA), shortSHA(target.OldSHA))}
	}

	checkouts, err := b.worktrees(ctx)
	if err != nil {
		return IntegrationResult{}, err
	}
	report, err := b.Preflight(ctx)
	if err != nil {
		return IntegrationResult{}, err
	}
	rootCheckout := false
	for _, entry := range checkouts {
		if entry.Branch != target.Ref {
			continue
		}
		if samePath(entry.Path, report.RepoTop) {
			rootCheckout = true
			continue
		}
		return IntegrationResult{}, &IntegrationBlockedError{Reason: "checked_out_elsewhere"}
	}

	if rootCheckout {
		rootBranchOut, err := git.run(ctx, b.root, nil, "symbolic-ref", "-q", "HEAD")
		if err != nil || strings.TrimSpace(string(rootBranchOut.stdout)) != target.Ref {
			return IntegrationResult{}, &IntegrationBlockedError{Reason: "target_dirty"}
		}
		// Working-tree-aware ff update: merge --ff-only refuses to overwrite
		// user edits and refuses diverged history without any reset.
		if _, err := git.run(ctx, b.root, nil, "merge", "--ff-only", resultSHA); err != nil {
			return IntegrationResult{}, &IntegrationBlockedError{Reason: "target_dirty"}
		}
		movedOut, err := git.run(ctx, b.root, nil, "rev-parse", target.Ref)
		if err != nil {
			return IntegrationResult{}, err
		}
		moved := strings.TrimSpace(string(movedOut.stdout))
		if moved != resultSHA {
			return IntegrationResult{}, &IntegrationBlockedError{Reason: "target_dirty"}
		}
		return IntegrationResult{NewSHA: moved, Mechanism: IntegrationFFWorkingTree}, nil
	}
	// testBeforeUpdateRef is nil in production; in-package tests use it to
	// move the target between the pre-check and the CAS write, proving the
	// CAS guard rejects a ref that moved inside the lock window.
	if testBeforeUpdateRef != nil {
		testBeforeUpdateRef()
	}
	if _, err := git.run(ctx, b.root, nil, "update-ref", target.Ref, resultSHA, target.OldSHA); err != nil {
		return IntegrationResult{}, &IntegrationAdvancedError{Reason: "concurrent target movement: update-ref CAS rejected"}
	}
	return IntegrationResult{NewSHA: resultSHA, Mechanism: IntegrationCASUpdateRef}, nil
}

// testBeforeUpdateRef is a nil production seam used only by in-package tests.
var testBeforeUpdateRef func()
