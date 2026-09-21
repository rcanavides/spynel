package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The tests in this file exercise the providerProcess lifecycle contract:
// one Wait owner, immutable exit classification, bounded explicit stop
// escalation, reader-owned stdout, and repeated-lifecycle hygiene. Unix-only
// group-signal evidence lives in provider_process_unix_test.go.

// startProcessFixture launches a provider lifecycle fixture as its own
// process-group leader and returns the owning providerProcess plus the
// fixture evidence log path.
func startProcessFixture(t *testing.T, mode string) (*providerProcess, string) {
	t.Helper()
	command, root, logPath := portableHarnessFixture(t, mode)
	proc, err := startProviderProcess(processSpec{Path: command, Dir: root})
	if err != nil {
		t.Fatal(err)
	}
	return proc, logPath
}

type drainedOutput struct {
	lines []string
	err   error
}

// drainStdout reads provider stdout to genuine EOF and closes the read side
// exactly once, mirroring the adapter reader contract. The drained result is
// published only after the close so close-count assertions synchronize with
// the reader goroutine, and a non-nil err means the drain never reached a
// genuine EOF.
func drainStdout(t *testing.T, proc *providerProcess) <-chan drainedOutput {
	t.Helper()
	results := make(chan drainedOutput, 1)
	go func() {
		var received []string
		scanner := bufio.NewScanner(proc.Stdout())
		for scanner.Scan() {
			received = append(received, scanner.Text())
		}
		proc.CloseStdout()
		results <- drainedOutput{lines: received, err: scanner.Err()}
	}()
	return results
}

func awaitDrained(t *testing.T, drained <-chan drainedOutput) []string {
	t.Helper()
	output := <-drained
	if output.err != nil {
		t.Fatalf("stdout drain ended in %v instead of a genuine EOF", output.err)
	}
	return output.lines
}

// pollFixtureRecords waits until the fixture evidence log contains at least
// one record of the given kind. The bounded poll is a wait-for-condition loop;
// its timeout is only a failure guard.
func pollFixtureRecords(t *testing.T, path, kind string, guard time.Duration) []fixtureRecord {
	t.Helper()
	deadline := time.Now().Add(guard)
	for {
		var matched []fixtureRecord
		if data, err := os.ReadFile(path); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.TrimSpace(line) == "" {
					continue
				}
				var record fixtureRecord
				if json.Unmarshal([]byte(line), &record) == nil && record.Kind == kind {
					matched = append(matched, record)
				}
			}
		}
		if len(matched) > 0 {
			return matched
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture evidence log %s never recorded %q within %s", path, kind, guard)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// openFDCount reports this process's open descriptor count on Linux, where
// /proc/self/fd is available, for descriptor-leak assertions.
func openFDCount() (int, bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

// L1: A process that exits by itself publishes Done, then an immutable Result
// with requested=false that preserves the exact exit error.
func TestProviderNaturalExitPublishesImmutableResult(t *testing.T) {
	proc, _ := startProcessFixture(t, "proc-exit-nonzero")
	select {
	case <-proc.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("natural fixture exit did not publish Done")
	}
	exit := proc.Result()
	if exit.requested {
		t.Fatal("natural exit was classified as requested")
	}
	if exit.err == nil || exit.err.Error() != "exit status 7" {
		t.Fatalf("natural exit result = %v, want exit status 7", exit.err)
	}
}

// L2: A provider that exits on cooperative stdin EOF stops cleanly with no
// signal escalation.
func TestProviderStopCooperativeEOFNeedsNoSignals(t *testing.T) {
	proc, logPath := startProcessFixture(t, "proc-cooperative")
	if err := proc.Stop(context.Background()); err != nil {
		t.Fatalf("cooperative stop = %v", err)
	}
	exit := proc.Result()
	if !exit.requested {
		t.Fatal("cooperative stop exit was not classified as requested")
	}
	if exit.err != nil {
		t.Fatalf("cooperative stop exit error = %v", exit.err)
	}
	for _, record := range pollFixtureRecords(t, logPath, "exit", 5*time.Second) {
		if record.Text != "eof" {
			t.Fatalf("cooperative fixture exited for %q instead of stdin EOF", record.Text)
		}
	}
	if runtime.GOOS != "windows" {
		for _, record := range readFixtureRecords(t, logPath) {
			if record.Kind == "signal" {
				t.Fatalf("cooperative stop escalated to signal %q", record.Text)
			}
		}
	}
}

// L5: Repeated and concurrent Stop callers share exactly one escalation
// sequence and observe identical results.
func TestProviderConcurrentStopRunsOneEscalation(t *testing.T) {
	proc, logPath := startProcessFixture(t, "proc-ignore-eof")
	const callers = 8
	results := make(chan error, callers)
	begin := make(chan struct{})
	for i := 0; i < callers; i++ {
		go func() {
			<-begin
			results <- proc.Stop(context.Background())
		}()
	}
	close(begin)
	for i := 0; i < callers; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent stop = %v", err)
		}
	}
	exit := proc.Result()
	if !exit.requested {
		t.Fatal("concurrent stop exit was not classified as requested")
	}
	if runtime.GOOS != "windows" {
		signals := pollFixtureRecords(t, logPath, "signal", 5*time.Second)
		if len(signals) != 1 {
			t.Fatalf("escalation delivered %d stop signals, want exactly 1", len(signals))
		}
	}
}

// L6: The sole Wait owner publishes exactly one exit snapshot. Concurrent
// observers read the identical immutable result, and a duplicate Wait anywhere
// would surface a reaped-child error instead of the preserved exit status.
func TestProviderSingleWaitOwnerResultSnapshot(t *testing.T) {
	proc, _ := startProcessFixture(t, "proc-exit-nonzero")
	<-proc.Done()
	const observers = 16
	exits := make(chan processExit, observers)
	for i := 0; i < observers; i++ {
		go func() {
			exits <- proc.Result()
		}()
	}
	first := <-exits
	if first.err == nil || first.err.Error() != "exit status 7" {
		t.Fatalf("exit snapshot = %v, want exit status 7", first.err)
	}
	if first.requested {
		t.Fatal("natural exit snapshot claimed requested")
	}
	for i := 1; i < observers; i++ {
		exit := <-exits
		if exit.requested != first.requested {
			t.Fatalf("observer %d read requested=%t, want %t", i, exit.requested, first.requested)
		}
		if (exit.err == nil) != (first.err == nil) || (exit.err != nil && exit.err.Error() != first.err.Error()) {
			t.Fatalf("observer %d read %v, want %v", i, exit.err, first.err)
		}
	}
}

// L8: A stop-caused exit is classified requested and surfaces no failure.
func TestProviderRequestedExitClassification(t *testing.T) {
	proc, _ := startProcessFixture(t, "proc-cooperative")
	if err := proc.Stop(context.Background()); err != nil {
		t.Fatalf("requested stop = %v", err)
	}
	exit := proc.Result()
	if !exit.requested {
		t.Fatal("stop-caused exit was not classified as requested")
	}
	if exit.err != nil {
		t.Fatalf("stop-caused exit error = %v", exit.err)
	}
}

// L9: Without any Stop, a spontaneous nonzero exit keeps its failure visible.
func TestProviderSpontaneousFailureStaysVisible(t *testing.T) {
	proc, _ := startProcessFixture(t, "proc-exit-nonzero")
	exit := proc.Result()
	if exit.requested {
		t.Fatal("spontaneous exit was classified as requested")
	}
	if exit.err == nil {
		t.Fatal("spontaneous failure was lost")
	}
	if again := proc.Result(); again.requested || again.err == nil {
		t.Fatalf("re-read snapshot = %#v, want the same spontaneous failure", again)
	}
}

// L10: A cancelled stop context can never turn the stop into an indefinite
// wait: grace waiting is skipped while TERM, KILL, and the final sweep still
// run, and the stop never reports a false kill timeout.
func TestProviderCancelledStopIsBounded(t *testing.T) {
	proc, _ := startProcessFixture(t, "proc-hold")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := proc.Stop(ctx)
	if err != nil && !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("cancelled stop = %v, want nil or only the cancellation", err)
	}
	if err != nil && strings.Contains(err.Error(), "did not exit after SIGKILL") {
		t.Fatal("cancelled stop reported a false kill timeout")
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("cancelled stop took %s; grace waiting was not skipped", elapsed)
	}
	// Cleanup still ran: the uncooperative process must actually be dead.
	exit := proc.Result()
	if !exit.requested {
		t.Fatal("cancelled cleanup did not classify its own kill as requested")
	}
}

// L12: Repeated create/read/stop cycles must not leak lifecycle state or race
// under the race detector.
func TestProviderRepeatedLifecycleCycles(t *testing.T) {
	for cycle := 0; cycle < 10; cycle++ {
		proc, _ := startProcessFixture(t, "proc-cooperative")
		drained := drainStdout(t, proc)
		if err := proc.Stop(context.Background()); err != nil {
			t.Fatalf("cycle %d stop = %v", cycle, err)
		}
		if exit := proc.Result(); !exit.requested || exit.err != nil {
			t.Fatalf("cycle %d exit = %#v", cycle, exit)
		}
		if lines := awaitDrained(t, drained); len(lines) != 0 {
			t.Fatalf("cycle %d saw unexpected stdout output: %v", cycle, lines)
		}
		if proc.stdoutCloseCount() != 1 {
			t.Fatalf("cycle %d closed stdout %d times, want exactly 1", cycle, proc.stdoutCloseCount())
		}
	}
}

// L13: A Stop that begins after the process already exited can never
// reclassify the published natural-exit snapshot as requested.
func TestProviderStopAfterExitCannotRewriteRequestedHistory(t *testing.T) {
	proc, _ := startProcessFixture(t, "proc-exit-nonzero")
	<-proc.Done()
	before := proc.Result()
	if before.requested {
		t.Fatal("natural exit snapshot already claimed requested")
	}
	if err := proc.Stop(context.Background()); err != nil {
		t.Fatalf("stop after natural exit = %v", err)
	}
	after := proc.Result()
	if after.requested {
		t.Fatal("stop after exit rewrote the requested classification")
	}
	if after.err == nil || after.err.Error() != before.err.Error() {
		t.Fatalf("stop after exit changed the exit result: %v then %v", before.err, after.err)
	}
}

// A crafted providerProcess over a real fixture group whose exit publication
// never arrives models the one bounded stop failure reachable without a
// production hook: the process survives the whole escalation, so Stop must
// report "did not exit after SIGKILL". Adapters must surface that report —
// ACP and Codex through Close's error return, Pi and Claude through their
// configured stderr diagnostics — instead of discarding it.
func TestProviderStopFailureSurfacesThroughAdapterCleanup(t *testing.T) {
	real, logPath := startProcessFixture(t, "proc-hold")
	t.Cleanup(func() { _ = real.Stop(context.Background()) })
	// The crafted lifecycle shares the real fixture's process group; wait for
	// the fixture's signal handlers so the staged escalation observes real
	// ignored-TERM behavior rather than a pre-registration default kill.
	pollFixtureRecords(t, logPath, "hold-ready", 10*time.Second)
	stalled := &providerProcess{cmd: real.cmd, done: make(chan struct{})}

	acp := &ACP{proc: stalled, pending: map[string]chan acpResponse{}, promptTurns: map[string]*acpTurn{}, active: map[string]*acpTurn{}}
	acpErr := acp.Close()
	if acpErr == nil || !strings.Contains(acpErr.Error(), "did not exit after SIGKILL") {
		t.Fatalf("ACP.Close() = %v, want the surfaced provider stop failure", acpErr)
	}
	if !acp.closed {
		t.Fatal("ACP.Close returned before marking the adapter closed")
	}

	codex := &Codex{proc: stalled}
	codexErr := codex.Close()
	if codexErr == nil || !strings.Contains(codexErr.Error(), "did not exit after SIGKILL") {
		t.Fatalf("Codex.Close() = %v, want the surfaced provider stop failure", codexErr)
	}
	if !codex.closed {
		t.Fatal("Codex.Close returned before marking the adapter closed")
	}

	var piDiagnostics bytes.Buffer
	(&piProcess{proc: stalled, owner: &Pi{config: HarnessConfig{Stderr: &piDiagnostics}}}).close()
	if !strings.Contains(piDiagnostics.String(), "stop Pi RPC process: provider process") || !strings.Contains(piDiagnostics.String(), "did not exit after SIGKILL") {
		t.Fatalf("Pi stop diagnostics = %q, want the stop failure logged", piDiagnostics.String())
	}

	var claudeDiagnostics bytes.Buffer
	_, claudeCancel := context.WithCancel(context.Background())
	claude := &Claude{config: HarnessConfig{Stderr: &claudeDiagnostics}, active: map[string]*claudeTurn{"chat": {proc: stalled, cancel: claudeCancel}}}
	if err := claude.Close(); err != nil {
		t.Fatalf("Claude.Close() = %v, want nil with diagnostics only", err)
	}
	if !strings.Contains(claudeDiagnostics.String(), "stop Claude Code process: provider process") || !strings.Contains(claudeDiagnostics.String(), "did not exit after SIGKILL") {
		t.Fatalf("Claude stop diagnostics = %q, want the stop failure logged", claudeDiagnostics.String())
	}
}

// L15: The adapter-style reader owns stdout across repeated cycles — it
// drains to genuine EOF, closes exactly once, Stop never closes it, and
// failed launches leave no descriptor leaks.
// drains to genuine EOF, closes exactly once, Stop never closes it, and
// failed launches leave no descriptor leaks.
func TestProviderStdoutOwnershipAndDescriptorHygiene(t *testing.T) {
	baseline, canCountFDs := openFDCount()
	for cycle := 0; cycle < 5; cycle++ {
		proc, _ := startProcessFixture(t, "proc-cooperative")
		drained := drainStdout(t, proc)
		if err := proc.Stop(context.Background()); err != nil {
			t.Fatalf("cycle %d stop = %v", cycle, err)
		}
		awaitDrained(t, drained)
		if proc.stdoutCloseCount() != 1 {
			t.Fatalf("cycle %d closed stdout %d times, want exactly 1", cycle, proc.stdoutCloseCount())
		}
	}
	if canCountFDs {
		// Growth is the leak signal. Async fixture cleanup from earlier
		// tests can legitimately reduce the process-wide count after the
		// baseline capture, so a smaller total is not a failure.
		if after, _ := openFDCount(); after > baseline {
			t.Fatalf("stdout lifecycle cycles leaked descriptors: %d became %d", baseline, after)
		}
	}
	// A launch that fails after creating its pipes must close every descriptor
	// it created; no providerProcess is returned, so none can leak.
	if _, err := startProviderProcess(processSpec{Path: filepath.Join(t.TempDir(), "does not exist")}); err == nil {
		t.Fatal("expected the failed launch to report an error")
	}
	if canCountFDs {
		if after, _ := openFDCount(); after > baseline {
			t.Fatalf("failed launch leaked descriptors: %d became %d", baseline, after)
		}
	}
}
