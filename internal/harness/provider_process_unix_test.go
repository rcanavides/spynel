//go:build !windows

package harness

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Unix-only lifecycle tests: group-signal escalation evidence, descendant
// sweeps, and cancelled-stop cleanup behavior that depend on Unix signals.

func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// awaitProcessDeath polls process liveness until the pid is gone. The bounded
// poll is a wait-for-condition loop; its timeout is only a failure guard.
func awaitProcessDeath(pid int, guard time.Duration) bool {
	deadline := time.Now().Add(guard)
	for {
		if !processAlive(pid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func descendantFixturePID(t *testing.T, records []fixtureRecord) int {
	t.Helper()
	if len(records) == 0 {
		t.Fatal("descendant fixture recorded no pid")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(records[0].Text))
	if err != nil {
		t.Fatalf("descendant fixture pid %q: %v", records[0].Text, err)
	}
	return pid
}

// L3: A provider that ignores cooperative stdin EOF but honors TERM is
// stopped by group termination without any KILL.
func TestProviderStopEscalatesThroughTermWithoutKill(t *testing.T) {
	proc, logPath := startProcessFixture(t, "proc-ignore-eof")
	if err := proc.Stop(context.Background()); err != nil {
		t.Fatalf("TERM stop = %v", err)
	}
	signals := pollFixtureRecords(t, logPath, "signal", 5*time.Second)
	if len(signals) != 1 {
		t.Fatalf("TERM escalation evidence = %d records, want exactly 1", len(signals))
	}
	exit := proc.Result()
	if !exit.requested {
		t.Fatal("TERM stop exit was not classified as requested")
	}
	// A graceful TERM exit produces a clean wait result; a KILL would surface
	// "signal: killed" instead.
	if exit.err != nil {
		t.Fatalf("TERM stop did not end the process gracefully: %v", exit.err)
	}
}

// L4: A provider that ignores both EOF and TERM is stopped by group KILL
// within the escalation bound, and the stopped leader leaves no zombie.
func TestProviderStopEscalatesThroughKillWhenTermIgnored(t *testing.T) {
	proc, logPath := startProcessFixture(t, "proc-hold")
	if err := proc.Stop(context.Background()); err != nil {
		t.Fatalf("KILL stop = %v", err)
	}
	signals := pollFixtureRecords(t, logPath, "signal", 5*time.Second)
	if len(signals) == 0 {
		t.Fatal("TERM stage did not run before the KILL")
	}
	exit := proc.Result()
	if !exit.requested {
		t.Fatal("kill-caused exit was not classified as requested")
	}
	if exit.err == nil || !strings.Contains(exit.err.Error(), "signal: killed") {
		t.Fatalf("stop did not end the process via SIGKILL: %v", exit.err)
	}
	if processAlive(proc.cmd.Process.Pid) {
		t.Fatal("stopped process was not reaped by the one Wait owner")
	}
}

// L7: A wrapper leader that exits naturally cannot leave its descendant
// alive: the wait owner's group sweep owns that, and a Stop issued afterward
// stays safe and idempotent over the already-exited group.
func TestProviderStopAfterNaturalLeaderExitKillsSurvivingDescendant(t *testing.T) {
	proc, logPath := startProcessFixture(t, "proc-spawn-child")
	drained := drainStdout(t, proc)
	ready := pollFixtureRecords(t, logPath, "descendant-ready", 10*time.Second)
	descendantPID := descendantFixturePID(t, ready)
	select {
	case <-proc.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("leader fixture did not exit")
	}
	if !awaitProcessDeath(descendantPID, 10*time.Second) {
		t.Fatal("descendant survived its leader's natural exit before any Stop")
	}
	if err := proc.Stop(context.Background()); err != nil {
		t.Fatalf("stop after natural exit = %v", err)
	}
	exit := proc.Result()
	if exit.requested {
		t.Fatal("stop after natural exit reclassified the leader exit as requested")
	}
	if exit.err != nil {
		t.Fatalf("unexpected leader exit error: %v", exit.err)
	}
	if lines := awaitDrained(t, drained); len(lines) == 0 {
		t.Fatal("reader lost buffered descendant output during the sweep")
	}
}

// F1's platform contract: interruption is a real provider disturbance, never
// a silent no-op, and it never escalates into a stop. On Unix that is group
// SIGINT; on Windows the helper terminates the direct provider process
// instead (compile-verified; runtime Windows coverage runs on the Windows
// CI leg).
func TestProviderInterruptDeliversSignalWithoutStopping(t *testing.T) {
	proc, logPath := startProcessFixture(t, "proc-hold")
	defer func() { _ = proc.Stop(context.Background()) }()
	// Wait for the fixture's signal handlers before interrupting: a signal
	// that races ahead of handler registration would terminate the fixture
	// through the default disposition instead of disturbing it.
	pollFixtureRecords(t, logPath, "hold-ready", 5*time.Second)
	if err := proc.Interrupt(); err != nil {
		t.Fatalf("interrupt = %v", err)
	}
	signals := pollFixtureRecords(t, logPath, "signal", 5*time.Second)
	if len(signals) == 0 || signals[0].Text != syscall.SIGINT.String() {
		t.Fatalf("interrupt delivered %v, want a group SIGINT record", signals)
	}
	// Interruption must not terminate the provider: only Stop owns that.
	if !processAlive(proc.cmd.Process.Pid) {
		t.Fatal("interrupt terminated the provider instead of disturbing it")
	}
}

// L14: A stop context cancelled mid-escalation must skip only grace waiting:
// TERM had already run, KILL still runs, the final sweep still runs, and the
// stop returns promptly without a false "did not exit after SIGKILL" report.
func TestProviderCancelledStopStillEscalatesAndSweeps(t *testing.T) {
	proc, logPath := startProcessFixture(t, "proc-hold")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, 1)
	go func() {
		results <- proc.Stop(ctx)
	}()
	// Wait until TERM has actually been delivered and recorded, then cancel:
	// the remaining grace waits must be skipped, never the cleanup itself.
	pollFixtureRecords(t, logPath, "signal", 10*time.Second)
	cancel()
	select {
	case err := <-results:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled stop = %v, want nil or only the cancellation", err)
		}
		if err != nil && strings.Contains(err.Error(), "did not exit after SIGKILL") {
			t.Fatal("cancelled stop reported a false kill timeout")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("cancelled stop did not return promptly after cancellation")
	}
	// The uncooperative process still died: KILL and the sweep ran.
	exit := proc.Result()
	if !exit.requested {
		t.Fatal("cancelled cleanup did not classify its own kill as requested")
	}
	if exit.err == nil || !strings.Contains(exit.err.Error(), "signal: killed") {
		t.Fatalf("cancelled cleanup did not KILL the provider: %v", exit.err)
	}
}

// L16: A natural leader exit alone — with no Stop at all — must sweep the
// process group so the descendant dies, while the stdout reader first drains
// the descendant's buffered late output and then reaches a genuine EOF.
func TestProviderNaturalLeaderExitSweepsDescendantsAndDrainsStdout(t *testing.T) {
	proc, logPath := startProcessFixture(t, "proc-spawn-child")
	drained := drainStdout(t, proc)
	ready := pollFixtureRecords(t, logPath, "descendant-ready", 10*time.Second)
	descendantPID := descendantFixturePID(t, ready)
	select {
	case <-proc.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("leader fixture did not exit naturally")
	}
	if !awaitProcessDeath(descendantPID, 10*time.Second) {
		t.Fatal("natural leader exit did not sweep the surviving descendant")
	}
	// Stop only for descriptor hygiene after the sweep evidence is complete.
	t.Cleanup(func() {
		_ = proc.Stop(context.Background())
	})
	lines := awaitDrained(t, drained)
	if !containsString(lines, "descendant-buffered-output") {
		t.Fatalf("buffered descendant output was truncated by the sweep: %v", lines)
	}
	exit := proc.Result()
	if exit.requested {
		t.Fatal("natural sweep exit was classified as requested")
	}
	if exit.err != nil {
		t.Fatalf("natural sweep corrupted the leader exit result: %v", exit.err)
	}
}
