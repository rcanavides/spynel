package execws

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// integrationFixture creates a repository whose root checkout is on the target
// branch at base, plus a captured result R based on base.
func integrationFixture(t *testing.T) (backend *LocalGit, root, base, resultSHA string, target Target) {
	t.Helper()
	ctx := context.Background()
	root, _, shas := testRepo(t)
	backend, err := NewLocalGit(root)
	if err != nil {
		t.Fatalf("NewLocalGit: %v", err)
	}
	report, err := backend.Preflight(ctx)
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if report.TargetOld != shas[1] {
		t.Fatalf("fixture target = %s, want the newest commit", report.TargetOld)
	}
	target = Target{Ref: report.TargetRef, OldSHA: report.TargetOld}
	_, ws := preparedWorkspace(t, root, shas[1])
	writeFile(t, filepath.Join(ws.ProviderView(), "feature.txt"), "integrated feature")
	result, err := ws.Capture(ctx, captureRequest("ln-integration", shas[1], "doc-integration"))
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if !result.Changed {
		t.Fatal("integration fixture must capture a change")
	}
	return backend, root, shas[1], result.ResultSHA, target
}

// IN1: a valid ff integration moves the target to exactly R through the
// working-tree-aware root mechanism.
func TestIN1ValidFFIntegration(t *testing.T) {
	ctx := context.Background()
	backend, root, base, resultSHA, target := integrationFixture(t)
	got, err := backend.Integrate(ctx, target, resultSHA)
	if err != nil {
		t.Fatalf("Integrate: %v", err)
	}
	if got.NewSHA != resultSHA || got.Mechanism != IntegrationFFWorkingTree {
		t.Fatalf("integration result = %+v", got)
	}
	current := strings.TrimSpace(runGit(t, root, "rev-parse", target.Ref))
	if current != resultSHA {
		t.Fatalf("target = %s, want %s", current, resultSHA)
	}
	// The root working tree advanced with the branch and .spynel survived.
	if _, err := os.Stat(filepath.Join(root, ".spynel", "worktrees")); err != nil {
		t.Fatalf("workspace state directory must survive integration: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "feature.txt")); err != nil || string(data) != "integrated feature" {
		t.Fatalf("root working tree must contain the integrated feature: %v", err)
	}
	// IN8: no merge commit ever exists.
	merges := strings.TrimSpace(runGit(t, root, "rev-list", "--merges", base+".."+resultSHA))
	if merges != "" {
		t.Fatalf("integration must never create merge commits: %s", merges)
	}
}

// IN2: a target that advanced past the captured old SHA is rejected without
// rebase, merge, or force; the result ref and target survive untouched.
func TestIN2TargetAdvancedRejected(t *testing.T) {
	ctx := context.Background()
	backend, root, base, resultSHA, target := integrationFixture(t)
	// Advance the target from A to C while the result is based on A.
	runGit(t, root, "commit", "--allow-empty", "-qm", "competing advance")
	_, err := backend.Integrate(ctx, target, resultSHA)
	var advanced *IntegrationAdvancedError
	if !errors.As(err, &advanced) {
		t.Fatalf("advanced target must reject integration: %v", err)
	}
	if !errors.Is(err, ErrIntegrationAdvanced) {
		t.Fatalf("error must wrap ErrIntegrationAdvanced: %v", err)
	}
	current := strings.TrimSpace(runGit(t, root, "rev-parse", target.Ref))
	if current == resultSHA || current == base {
		t.Fatalf("rejected integration must leave the advanced target untouched: %s", current)
	}
	// A diverged result (not a descendant of the old target) is rejected too.
	diverged, _, _ := testRepo(t)
	_ = diverged
	// Build a result whose parent is NOT the target old: capture from an older
	// base in a fresh workspace.
	_, ws := preparedWorkspace(t, root, base)
	writeFile(t, filepath.Join(ws.ProviderView(), "divergent.txt"), "sibling content")
	sibling, err := ws.Capture(ctx, captureRequest("ln-sibling", base, "doc-sibling"))
	if err != nil {
		t.Fatalf("sibling capture: %v", err)
	}
	if !sibling.Changed {
		t.Fatal("sibling capture must change")
	}
	_, err = backend.Integrate(ctx, Target{Ref: target.Ref, OldSHA: strings.TrimSpace(runGit(t, root, "rev-parse", target.Ref))}, sibling.ResultSHA)
	if !errors.As(err, &advanced) {
		t.Fatalf("non-ff result must reject integration: %v", err)
	}
}

// IN3: the repository-global integration lock excludes cross-process holders
// and serializes concurrent in-process integrations.
func TestIN3IntegrationLockExcludesOtherProcess(t *testing.T) {
	if os.Getenv("SPYNEL_EXECWS_LOCK_CHILD") != "" {
		execwsLockChildMain(os.Getenv("SPYNEL_EXECWS_LOCK_DIR"))
		return
	}
	backend, _, _, _, _ := integrationFixture(t)
	if err := backend.ensureRuntimeDirectories(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	child := exec.Command(os.Args[0], "-test.run", "^"+t.Name()+"$", "-test.count=1")
	child.Env = append(os.Environ(),
		"SPYNEL_EXECWS_LOCK_CHILD=1",
		"SPYNEL_EXECWS_LOCK_DIR="+backend.integrationLockPath())
	stdin, err := child.StdinPipe()
	if err != nil {
		t.Fatalf("child stdin: %v", err)
	}
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatalf("child stdout: %v", err)
	}
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	reader := bufio.NewReader(stdout)
	if line, err := reader.ReadString('\n'); err != nil || line != "locked\n" {
		t.Fatalf("child ready = %q err=%v", line, err)
	}
	if _, held, err := tryLockFile(backend.integrationLockPath()); err != nil {
		t.Fatalf("try lock: %v", err)
	} else if held {
		t.Fatal("integration lock must exclude other processes")
	}
	if _, err := stdin.Write([]byte("release\n")); err != nil {
		t.Fatalf("signal child: %v", err)
	}
	if err := child.Wait(); err != nil {
		t.Fatalf("child exit: %v", err)
	}
}

// IN3: two concurrent integrations serialize; exactly one target movement
// happens and the loser observes the advanced target instead of clobbering.
func TestIN3ConcurrentIntegrationsSerialize(t *testing.T) {
	ctx := context.Background()
	backend, root, base, _, target := integrationFixture(t)
	results := make([]string, 2)
	for index := range results {
		_, ws := preparedWorkspace(t, root, base)
		writeFile(t, filepath.Join(ws.ProviderView(), fmt.Sprintf("feature-%d.txt", index)), "content")
		captured, err := ws.Capture(ctx, captureRequest(fmt.Sprintf("ln-race-%d", index), base, "doc-race"))
		if err != nil {
			t.Fatalf("capture %d: %v", index, err)
		}
		results[index] = captured.ResultSHA
	}
	var wg sync.WaitGroup
	outcomes := make(chan string, 2)
	for _, result := range results {
		wg.Add(1)
		go func(result string) {
			defer wg.Done()
			_, err := backend.Integrate(ctx, target, result)
			var advanced *IntegrationAdvancedError
			switch {
			case err == nil:
				outcomes <- "integrated"
			case errors.As(err, &advanced):
				outcomes <- "advanced"
			default:
				outcomes <- "error: " + err.Error()
			}
		}(result)
	}
	wg.Wait()
	close(outcomes)
	integrated, advanced, failed := 0, 0, 0
	for outcome := range outcomes {
		switch {
		case outcome == "integrated":
			integrated++
		case outcome == "advanced":
			advanced++
		default:
			failed++
			t.Logf("unexpected outcome: %s", outcome)
		}
	}
	if integrated != 1 || advanced != 1 || failed != 0 {
		t.Fatalf("outcomes = integrated:%d advanced:%d failed:%d, want 1/1/0", integrated, advanced, failed)
	}
	current := strings.TrimSpace(runGit(t, root, "rev-parse", target.Ref))
	if current != results[0] && current != results[1] {
		t.Fatalf("target = %s, want one of the two results", current)
	}
	merges := strings.TrimSpace(runGit(t, root, "rev-list", "--merges", base+".."+current))
	if merges != "" {
		t.Fatalf("no merge commits may exist: %s", merges)
	}
}

func execwsLockChildMain(lockPath string) {
	unlock, err := lockFile(lockPath)
	if err != nil {
		os.Exit(3)
	}
	os.Stdout.WriteString("locked\n")
	reader := bufio.NewReader(os.Stdin)
	if _, err := reader.ReadString('\n'); err != nil {
		unlock()
		os.Exit(4)
	}
	unlock()
	os.Exit(0)
}

// IN4: a dirty root checkout blocks integration without resetting user edits;
// a later retry after the user resolved their edits succeeds.
func TestIN4DirtyRootBlocksThenRetries(t *testing.T) {
	ctx := context.Background()
	backend, root, base, _, target := integrationFixture(t)
	// Capture a result that modifies a tracked file, so a root checkout with
	// user edits to that same file cannot fast-forward without overwriting.
	_, ws := preparedWorkspace(t, root, base)
	writeFile(t, filepath.Join(ws.ProviderView(), "README.md"), "captured README change")
	colliding, err := ws.Capture(ctx, captureRequest("ln-dirty", base, "doc-dirty"))
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	// The user edits the same tracked file in the root checkout.
	writeFile(t, filepath.Join(root, "README.md"), "user edit in progress")
	_, err = backend.Integrate(ctx, target, colliding.ResultSHA)
	var blocked *IntegrationBlockedError
	if !errors.As(err, &blocked) || blocked.Reason != "target_dirty" {
		t.Fatalf("dirty root must block: %v", err)
	}
	if !errors.Is(err, ErrIntegrationBlocked) {
		t.Fatalf("error must wrap ErrIntegrationBlocked: %v", err)
	}
	// The user edit survived untouched.
	if data, _ := os.ReadFile(filepath.Join(root, "README.md")); string(data) != "user edit in progress" {
		t.Fatal("integration must never reset user changes")
	}
	current := strings.TrimSpace(runGit(t, root, "rev-parse", target.Ref))
	if current == colliding.ResultSHA {
		t.Fatal("blocked integration must not move the target")
	}
	// A later retry after the user committed their edit succeeds, because the
	// captured evidence moves to the new target value.
	runGit(t, root, "add", "README.md")
	runGit(t, root, "commit", "-qm", "user work")
	newTarget := Target{Ref: target.Ref, OldSHA: strings.TrimSpace(runGit(t, root, "rev-parse", target.Ref))}
	_, ws2 := preparedWorkspace(t, root, newTarget.OldSHA)
	writeFile(t, filepath.Join(ws2.ProviderView(), "retry.txt"), "retry content")
	retry, err := ws2.Capture(ctx, captureRequest("ln-retry", newTarget.OldSHA, "doc-retry"))
	if err != nil {
		t.Fatalf("retry capture: %v", err)
	}
	got, err := backend.Integrate(ctx, newTarget, retry.ResultSHA)
	if err != nil {
		t.Fatalf("retry Integrate: %v", err)
	}
	if got.NewSHA != retry.ResultSHA {
		t.Fatalf("retry result = %+v", got)
	}
	if data, _ := os.ReadFile(filepath.Join(root, "README.md")); string(data) != "user edit in progress" {
		t.Fatal("integration must never rewrite the user's file content")
	}
}

// IN5: a target not checked out anywhere integrates through CAS update-ref.
func TestIN5TargetNotCheckedOutUsesCAS(t *testing.T) {
	ctx := context.Background()
	backend, root, base, _, _ := integrationFixture(t)
	// Create a branch that no worktree checks out.
	runGit(t, root, "branch", "release-line", base)
	_, ws := preparedWorkspace(t, root, base)
	writeFile(t, filepath.Join(ws.ProviderView(), "release.txt"), "release content")
	result, err := ws.Capture(ctx, captureRequest("ln-cas", base, "doc-cas"))
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	got, err := backend.Integrate(ctx, Target{Ref: "refs/heads/release-line", OldSHA: base}, result.ResultSHA)
	if err != nil {
		t.Fatalf("CAS Integrate: %v", err)
	}
	if got.Mechanism != IntegrationCASUpdateRef || got.NewSHA != result.ResultSHA {
		t.Fatalf("CAS result = %+v", got)
	}
	current := strings.TrimSpace(runGit(t, root, "rev-parse", "refs/heads/release-line"))
	if current != result.ResultSHA {
		t.Fatalf("CAS target = %s, want %s", current, result.ResultSHA)
	}
	// The root checkout stayed on its own branch the whole time.
	rootBranch := strings.TrimSpace(runGit(t, root, "symbolic-ref", "HEAD"))
	if rootBranch == "refs/heads/release-line" {
		t.Fatal("CAS must never move the root checkout")
	}
}

// IN6: a target already at R is observed without a second movement.
func TestIN6AlreadyMovedRefObservedWithoutSecondMovement(t *testing.T) {
	ctx := context.Background()
	backend, root, _, resultSHA, target := integrationFixture(t)
	if _, err := backend.Integrate(ctx, target, resultSHA); err != nil {
		t.Fatalf("first Integrate: %v", err)
	}
	reflogBefore := runGit(t, root, "reflog", "show", "--format=%h", target.Ref)
	got, err := backend.Integrate(ctx, target, resultSHA)
	if err != nil {
		t.Fatalf("second Integrate: %v", err)
	}
	if got.Mechanism != IntegrationObserved || got.NewSHA != resultSHA {
		t.Fatalf("observed result = %+v", got)
	}
	reflogAfter := runGit(t, root, "reflog", "show", "--format=%h", target.Ref)
	if reflogBefore != reflogAfter {
		t.Fatalf("observe must not move the ref twice:\n%s\n%s", reflogBefore, reflogAfter)
	}
}

// IN7: integration refuses incomplete evidence instead of guessing.
func TestIN7MissingEvidenceRefuses(t *testing.T) {
	ctx := context.Background()
	backend, _, _, resultSHA, target := integrationFixture(t)
	if _, err := backend.Integrate(ctx, Target{Ref: "", OldSHA: target.OldSHA}, resultSHA); err == nil {
		t.Fatal("empty target ref must refuse")
	}
	if _, err := backend.Integrate(ctx, Target{Ref: target.Ref, OldSHA: ""}, resultSHA); err == nil {
		t.Fatal("empty old sha must refuse")
	}
	if _, err := backend.Integrate(ctx, target, ""); err == nil {
		t.Fatal("empty result sha must refuse")
	}
}

// IN9: a target checked out in another worktree blocks integration.
func TestIN9CheckedOutElsewhereBlocks(t *testing.T) {
	ctx := context.Background()
	backend, root, base, _, _ := integrationFixture(t)
	// The target branch here is one that only a side worktree checks out.
	runGit(t, root, "branch", "side-branch", base)
	elsewhere := filepath.Join(t.TempDir(), "elsewhere")
	runGit(t, root, "worktree", "add", elsewhere, "side-branch")
	// Build a fast-forwardable result for the side branch.
	_, ws := preparedWorkspace(t, root, base)
	writeFile(t, filepath.Join(ws.ProviderView(), "side.txt"), "side content")
	result, err := ws.Capture(ctx, captureRequest("ln-side", base, "doc-side"))
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	_, err = backend.Integrate(ctx, Target{Ref: "refs/heads/side-branch", OldSHA: base}, result.ResultSHA)
	var blocked *IntegrationBlockedError
	if !errors.As(err, &blocked) || blocked.Reason != "checked_out_elsewhere" {
		t.Fatalf("checked-out-elsewhere must block: %v", err)
	}
	if !errors.Is(err, ErrIntegrationBlocked) {
		t.Fatalf("error must wrap ErrIntegrationBlocked: %v", err)
	}
	current := strings.TrimSpace(runGit(t, root, "rev-parse", "refs/heads/side-branch"))
	if current != base {
		t.Fatalf("blocked integration must not move the target: %s", current)
	}
}

// IN5 CAS guard: a target that moves between the pre-check and the ref write
// must be rejected by the update-ref CAS instead of silently overwritten.
func TestIN5CASRejectsRefMovedInsideWindow(t *testing.T) {
	ctx := context.Background()
	backend, root, base, _, _ := integrationFixture(t)
	// Build an unchecked-out branch to exercise the CAS path.
	runGit(t, root, "branch", "cas-probe", base)
	// Use a real result based on the branch.
	_, ws := preparedWorkspace(t, root, base)
	writeFile(t, filepath.Join(ws.ProviderView(), "cas.txt"), "cas content")
	result, err := ws.Capture(ctx, captureRequest("ln-cas-window", base, "doc-cas"))
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	// Move the target directly inside the lock window: the CAS must reject
	// the stale old value instead of overwriting the movement.
	runGit(t, root, "commit", "--allow-empty", "-m", "in-window movement")
	newCommit := strings.TrimSpace(runGit(t, root, "rev-parse", "HEAD"))
	previous := testBeforeUpdateRef
	testBeforeUpdateRef = func() {
		if _, err := runGitChecked(t, root, "update-ref", "refs/heads/cas-probe", newCommit); err != nil {
			t.Fatalf("seed in-window movement: %v", err)
		}
	}
	defer func() { testBeforeUpdateRef = previous }()
	_, err = backend.Integrate(ctx, Target{Ref: "refs/heads/cas-probe", OldSHA: base}, result.ResultSHA)
	var advanced *IntegrationAdvancedError
	if !errors.As(err, &advanced) {
		t.Fatalf("ref moved inside the window: err = %v, want ErrIntegrationAdvanced (CAS must reject)", err)
	}
	current := strings.TrimSpace(runGit(t, root, "rev-parse", "refs/heads/cas-probe"))
	if current == result.ResultSHA {
		t.Fatal("the CAS-less write overwrote an in-window target movement")
	}
}
