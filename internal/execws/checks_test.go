package execws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// checkProbe prepares a workspace, captures one edit, and returns the
// workspace plus the canonical result SHA.
func checkProbe(t *testing.T, edit string) (Workspace, string, CaptureRequest) {
	t.Helper()
	ctx := context.Background()
	root, _, shas := testRepo(t)
	_, ws := preparedWorkspace(t, root, shas[1])
	req := captureRequest("ln-checks", shas[1], "doc-checks")
	if edit != "" {
		writeFile(t, filepath.Join(ws.ProviderView(), "probe.txt"), edit)
		result, err := ws.Capture(ctx, req)
		if err != nil {
			t.Fatalf("Capture: %v", err)
		}
		if !result.Changed {
			t.Fatal("check probe must capture a change")
		}
		return ws, result.ResultSHA, req
	}
	return ws, shas[1], req
}

func checkDef(id, command string, args ...string) CheckDef {
	return CheckDef{ID: id, Command: command, Args: args, Timeout: 30 * time.Second}
}

// CK1: a check runs exactly at R; a workspace that is not at R is mutated
// before the check even starts.
func TestCK1CheckRunsAtExactResult(t *testing.T) {
	ctx := context.Background()
	ws, resultSHA, req := checkProbe(t, "content")
	def := checkDef("probe-ok", "true")
	got, err := ws.RunCheck(ctx, CheckRequest{LaunchID: req.LaunchID, Def: def, ResultSHA: resultSHA})
	if err != nil {
		t.Fatalf("RunCheck: %v", err)
	}
	if got.Outcome != CheckOutcomePassed || got.ExitCode != 0 {
		t.Fatalf("check result = %+v", got)
	}
	if got.ResultSHA != resultSHA || got.ID != def.ID {
		t.Fatalf("evidence binding = %+v", got)
	}
	// A wrong result SHA fails as mutation evidence, never silently passes.
	if _, err := ws.RunCheck(ctx, CheckRequest{LaunchID: req.LaunchID, Def: def, ResultSHA: resultSHA + "0"}); !errors.Is(err, ErrVerifyFailed) {
		t.Fatalf("wrong result SHA must fail verify: %v", err)
	}
}

// CK2: a nonzero exit is a failed outcome with the exit code recorded.
func TestCK2ExitFailureRecordsFailedOutcome(t *testing.T) {
	ctx := context.Background()
	ws, resultSHA, req := checkProbe(t, "content")
	got, err := ws.RunCheck(ctx, CheckRequest{LaunchID: req.LaunchID, Def: checkDef("probe-fail", "false"), ResultSHA: resultSHA})
	if err != nil {
		t.Fatalf("RunCheck: %v", err)
	}
	if got.Outcome != CheckOutcomeFailed || got.ExitCode != 1 {
		t.Fatalf("check result = %+v", got)
	}
}

// CK4: the retained output is a bounded tail while the digest covers all
// streamed bytes.
func TestCK4BoundedTailAndFullDigest(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses printf-heavy shell probes")
	}
	ctx := context.Background()
	ws, resultSHA, req := checkProbe(t, "content")
	// Produce 512 KiB of deterministic output: more than the 128 KiB tail.
	def := checkDef("probe-output", "sh", "-c", `i=0; while [ $i -lt 512 ]; do printf 'abcdefgh%.4d\n' $i; i=$((i+1)); done`)
	// each line is 12 bytes (+"%4d\n" 4+1...); use a byte-counted generator instead:
	def = CheckDef{ID: "probe-output", Command: "head", Args: []string{"-c", "524288", "/dev/zero"}, Timeout: 30 * time.Second}
	got, err := ws.RunCheck(ctx, CheckRequest{LaunchID: req.LaunchID, Def: def, ResultSHA: resultSHA})
	if err != nil {
		t.Fatalf("RunCheck: %v", err)
	}
	if got.Outcome != CheckOutcomePassed {
		t.Fatalf("check outcome = %+v", got)
	}
	if got.OutputBytes != 524288 {
		t.Fatalf("output bytes = %d, want 524288", got.OutputBytes)
	}
	if !got.Truncated {
		t.Fatal("output beyond the tail bound must be marked truncated")
	}
	retained, err := os.ReadFile(got.OutputPath)
	if err != nil {
		t.Fatalf("read retained tail: %v", err)
	}
	if len(retained) != checkTailBound {
		t.Fatalf("retained tail = %d bytes, want %d", len(retained), checkTailBound)
	}
	digest := sha256.Sum256(make([]byte, 524288))
	if got.OutputSHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("output digest covers the full stream: %s", got.OutputSHA256)
	}
	if !got.OutputKept {
		t.Fatal("output tail must be retained")
	}
}

// CK5: a nonignored source mutation performed by a check itself yields
// outcome=mutated, the failure that stops all remaining checks.
func TestCK5SourceMutationFailsMutated(t *testing.T) {
	ctx := context.Background()
	ws, resultSHA, req := checkProbe(t, "content")
	probe := filepath.Join(ws.ProviderView(), "probe.txt")
	got, err := ws.RunCheck(ctx, CheckRequest{
		LaunchID:  req.LaunchID,
		Def:       CheckDef{ID: "probe-mutating", Command: "sh", Args: []string{"-c", "printf mutated > " + quoteShell(probe)}, Timeout: 30 * time.Second},
		ResultSHA: resultSHA,
	})
	if got.Outcome != CheckOutcomeMutated {
		t.Fatalf("mutation outcome = %+v err=%v", got, err)
	}
	if !errors.Is(err, ErrVerifyFailed) {
		t.Fatalf("mutation must surface a verify failure: %v", err)
	}
}

func quoteShell(path string) string { return "'" + strings.ReplaceAll(path, "'", `'\''`) + "'" }

// CK6: ignored outputs a check produces do not fail verification.
func TestCK6IgnoredCheckOutputsAccepted(t *testing.T) {
	ctx := context.Background()
	root, _, _ := testRepo(t)
	writeFile(t, filepath.Join(root, ".gitignore"), "*.log\n")
	runGit(t, root, "add", "-A")
	runGit(t, root, "commit", "-qm", "ignore logs")
	_, ws := preparedWorkspace(t, root, strings.TrimSpace(runGit(t, root, "rev-parse", "HEAD")))
	req := captureRequest("ln-ignored", strings.TrimSpace(runGit(t, root, "rev-parse", "HEAD")), "doc-ignored")
	writeFile(t, filepath.Join(ws.ProviderView(), "source.txt"), "captured")
	result, err := ws.Capture(ctx, req)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	got, err := ws.RunCheck(ctx, CheckRequest{
		LaunchID:  req.LaunchID,
		Def:       CheckDef{ID: "probe-writer", Command: "sh", Args: []string{"-c", "echo noise > check-output.log"}, Timeout: 30 * time.Second},
		ResultSHA: result.ResultSHA,
	})
	if err != nil || got.Outcome != CheckOutcomePassed {
		t.Fatalf("ignored check output must pass: %+v err=%v", got, err)
	}
}

// CK9: check evidence binds the exact definition digest; a changed definition
// changes the digest deterministically.
func TestCK9DefinitionDigestBinding(t *testing.T) {
	base := checkDef("build", "make", "check")
	if CheckDefDigest(base) != CheckDefDigest(checkDef("build", "make", "check")) {
		t.Fatal("identical definitions must produce identical digests")
	}
	variations := []CheckDef{
		checkDef("build", "make", "check", "-j2"),
		checkDef("build", "make", "test"),
		checkDef("test", "make", "check"),
		{ID: "build", Command: "make", Args: []string{"check"}, Timeout: time.Minute},
	}
	baseDigest := CheckDefDigest(base)
	for _, variation := range variations {
		if CheckDefDigest(variation) == baseDigest {
			t.Fatalf("definition variation %+v must change the digest", variation)
		}
	}
	// The set digest is order-insensitive across processes.
	setA := []CheckDef{base, checkDef("lint", "golangci-lint", "run")}
	setB := []CheckDef{checkDef("lint", "golangci-lint", "run"), base}
	if CheckSetDigest(setA) != CheckSetDigest(setB) {
		t.Fatal("set digest must be deterministic regardless of configured order")
	}
	if CheckSetDigest(nil) == "" {
		t.Fatal("an empty check set still has a stable digest")
	}
}

// CK10: a check command that cannot start records a failed outcome with no
// process side effects; the runner fails closed instead of hanging.
func TestCK10MissingCommandFailsClosed(t *testing.T) {
	ctx := context.Background()
	ws, resultSHA, req := checkProbe(t, "content")
	got, err := ws.RunCheck(ctx, CheckRequest{
		LaunchID:  req.LaunchID,
		Def:       CheckDef{ID: "probe-missing", Command: filepath.Join(t.TempDir(), "definitely-not-a-command-9f2c"), Timeout: 5 * time.Second},
		ResultSHA: resultSHA,
	})
	if got.Outcome != CheckOutcomeFailed || got.ExitCode != -1 {
		t.Fatalf("missing command outcome = %+v err=%v, want failed exit -1", got, err)
	}
}
