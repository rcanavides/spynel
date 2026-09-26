package execws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// checkTailBound is the retained bounded tail target for one check's combined
// output. The full output is hashed while streaming, so unbounded process
// output cannot exhaust memory or disk.
const checkTailBound = 128 * 1024

// tailBuffer keeps the last limit bytes written to it.
type tailBuffer struct {
	limit int
	data  []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.data = append(t.data, p...)
	if excess := len(t.data) - t.limit; excess > 0 {
		t.data = t.data[excess:]
	}
	return len(p), nil
}

// RunCheck executes one system-owned check inside the workspace at an exact
// result SHA. The workspace is verified at R before and after the check; a
// mutated nonignored source fails the check with outcome=mutated so callers
// stop the remaining checks.
func (w *gitWorkspace) RunCheck(ctx context.Context, req CheckRequest) (CheckResult, error) {
	if req.LaunchID == "" || req.ResultSHA == "" || req.Def.ID == "" || req.Def.Command == "" {
		return CheckResult{}, fmt.Errorf("execws: check requires launch id, result sha, and definition")
	}
	result := CheckResult{
		ID:        req.Def.ID,
		DefDigest: CheckDefDigest(req.Def),
		ResultSHA: req.ResultSHA,
	}
	if err := w.VerifyAt(ctx, req.ResultSHA); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Cancellation proves nothing about mutation; fail closed as an
			// ordinary failed check without mutated evidence.
			result.Outcome = CheckOutcomeFailed
			result.ExitCode = -1
			return result, ctxErr
		}
		result.Outcome = CheckOutcomeMutated
		return result, err
	}

	checkDir := filepath.Join(w.backend.launchDir(req.LaunchID), "checks")
	if err := os.MkdirAll(checkDir, 0o700); err != nil {
		return result, err
	}
	result.OutputPath = filepath.Join(checkDir, req.Def.ID+".log")

	started := time.Now()
	exitCode, total, digest, tail, runErr := w.runCheckProcess(ctx, req)
	result.DurationMS = time.Since(started).Milliseconds()
	result.OutputBytes = total
	result.OutputSHA256 = digest
	result.Truncated = total > int64(len(tail))
	if err := os.WriteFile(result.OutputPath, tail, 0o600); err != nil {
		return result, fmt.Errorf("execws: retain check output: %w", err)
	}
	result.OutputKept = true

	// A source mutation detected after the check outranks the check's own
	// outcome and fails the whole check set.
	if err := w.VerifyAt(ctx, req.ResultSHA); err != nil {
		result.Outcome = CheckOutcomeMutated
		return result, err
	}
	switch {
	case runErr == nil && exitCode == 0:
		result.Outcome = CheckOutcomePassed
	case runErr != nil && isProcessKilled(runErr):
		// Timeout or cancellation killed the process group.
		result.Outcome = CheckOutcomeFailed
		result.ExitCode = -1
	default:
		result.Outcome = CheckOutcomeFailed
		result.ExitCode = exitCode
	}
	return result, nil
}

// runCheckProcess starts the check command with no shell and streams combined
// output through the digest and tail. On Unix the command leads its own
// process group so timeout or cancellation kills the whole tree.
func (w *gitWorkspace) runCheckProcess(ctx context.Context, req CheckRequest) (exitCode int, total int64, digest string, tail []byte, err error) {
	runCtx, cancel := context.WithTimeout(ctx, req.Def.Timeout)
	defer cancel()
	cmd := newCheckCommand(runCtx, req.Def.Command, req.Def.Args)
	cmd.Dir = w.providerDir
	hasher := sha256.New()
	tailWriter := &tailBuffer{limit: checkTailBound}
	counter := &countingWriter{next: io.MultiWriter(hasher, tailWriter)}
	cmd.Stdout = counter
	cmd.Stderr = counter
	err = cmd.Start()
	if err != nil {
		return -1, 0, "", nil, err
	}
	err = cmd.Wait()
	exitCode = exitCodeFrom(err)
	return exitCode, counter.count, hex.EncodeToString(hasher.Sum(nil)), tailWriter.data, err
}

type countingWriter struct {
	next  io.Writer
	count int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.count += int64(len(p))
	return c.next.Write(p)
}

func exitCodeFrom(err error) int {
	if err == nil {
		return 0
	}
	if exitErr, ok := err.(interface{ ExitCode() int }); ok {
		if code := exitErr.ExitCode(); code >= 0 {
			return code
		}
	}
	return -1
}
