//go:build !windows

package harness

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// runProcessLifecycleFixture implements the provider process lifecycle fixture
// modes. Each mode models one wrapper or provider shutdown behavior —
// cooperative, TERM-honoring, or fully uncooperative — and records durable
// signal and exit evidence to the fixture log. Ordering is established by
// files and readiness records only; every bounded poll below is a
// wait-for-condition loop whose timeout is a failure guard, never a sleep that
// tests rely on for ordering.
func runProcessLifecycleFixture(mode string) int {
	switch mode {
	case "proc-cooperative":
		return runProcCooperativeFixture()
	case "proc-ignore-eof":
		return runProcIgnoreEOFFixture()
	case "proc-hold":
		return runProcHoldFixture()
	case "proc-spawn-child":
		return runProcSpawnChildFixture()
	case "proc-descendant":
		return runProcDescendantFixture()
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown unix process fixture mode %q\n", mode)
		return 2
	}
}

// runProcCooperativeFixture is a well-behaved provider: it exits on
// cooperative stdin EOF. If TERM arrives first, it records that evidence
// before exiting, so a cooperative stop can prove no escalation happened.
func runProcCooperativeFixture() int {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	defer signal.Stop(signals)
	stdinDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		close(stdinDone)
	}()
	for {
		select {
		case <-stdinDone:
			appendFixtureLog(map[string]any{"kind": "exit", "text": "eof"})
			return 0
		case sig := <-signals:
			appendFixtureLog(map[string]any{"kind": "signal", "text": sig.String()})
			return 0
		case <-time.After(time.Hour):
		}
	}
}

// runProcIgnoreEOFFixture ignores cooperative stdin EOF and keeps running
// until group termination arrives, which it records before exiting cleanly.
func runProcIgnoreEOFFixture() int {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
	}()
	for {
		select {
		case sig := <-signals:
			appendFixtureLog(map[string]any{"kind": "signal", "text": sig.String()})
			return 0
		case <-time.After(time.Hour):
		}
	}
}

// runProcHoldFixture ignores cooperative stdin EOF, TERM, and INT. It records
// every signal it receives and never exits on its own; only the uncatchable
// group KILL can end it. The ready record is written only after the signal
// handlers are registered, so tests can synchronize before signalling.
func runProcHoldFixture() int {
	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	appendFixtureLog(map[string]any{"kind": "hold-ready"})
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
	}()
	for {
		select {
		case sig := <-signals:
			appendFixtureLog(map[string]any{"kind": "signal", "text": sig.String()})
		case <-time.After(time.Hour):
		}
	}
}

// runProcSpawnChildFixture is a wrapper leader that starts a descendant
// holding the provider stdout pipe, waits for durable evidence the descendant
// is ready, and then exits naturally without waiting for it.
func runProcSpawnChildFixture() int {
	descendant := exec.Command(mustExecutable())
	descendant.Stdout = os.Stdout
	descendant.Stderr = os.Stderr
	descendant.Env = append(os.Environ(), fixtureModeEnv+"=proc-descendant")
	if err := descendant.Start(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "process fixture descendant failed to start: %v\n", err)
		return 3
	}
	logPath := os.Getenv(fixtureLogEnv)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if data, err := os.ReadFile(logPath); err == nil && strings.Contains(string(data), "descendant-ready") {
			break
		}
		if time.Now().After(deadline) {
			_, _ = fmt.Fprintln(os.Stderr, "process fixture descendant never became ready")
			return 4
		}
		time.Sleep(5 * time.Millisecond)
	}
	appendFixtureLog(map[string]any{"kind": "leader-exit"})
	return 0
}

// runProcDescendantFixture is a provider child that outlives its leader. Its
// stdout line is written while the leader still lives so the bytes are already
// buffered when the leader exits; it then holds the pipe open until the
// process-group sweep kills it.
func runProcDescendantFixture() int {
	_, _ = fmt.Fprintln(os.Stdout, "descendant-buffered-output")
	appendFixtureLog(map[string]any{"kind": "descendant-ready", "text": strconv.Itoa(os.Getpid())})
	signals := make(chan os.Signal, 4)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	for {
		select {
		case sig := <-signals:
			appendFixtureLog(map[string]any{"kind": "descendant-signal", "text": sig.String()})
			return 0
		case <-time.After(time.Hour):
		}
	}
}

func mustExecutable() string {
	executable, err := os.Executable()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "process fixture cannot resolve its executable: %v\n", err)
		os.Exit(5)
	}
	return executable
}
