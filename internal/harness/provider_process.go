package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// Stop escalation and wait bounds are fixed internal lifecycle constants.
// C8.2 defines how one provider process is stopped; multi-provider ordering
// and total shutdown budgets belong to a later milestone.
const (
	stdinGrace = 3 * time.Second
	termGrace  = 2 * time.Second
	killGrace  = 2 * time.Second
	waitDelay  = 2 * time.Second
)

// processExit is the immutable result published by the one Wait owner. The
// requested flag is snapshotted at wait completion, so a Stop that begins
// after process exit can never reclassify a spontaneous exit as a requested
// shutdown.
type processExit struct {
	err       error
	requested bool
}

// stageOutcome distinguishes process exit from grace expiry and caller
// cancellation while waiting for a stop stage.
type stageOutcome int

const (
	stageExited stageOutcome = iota
	stageTimeout
	stageCancelled
)

// processSpec describes one provider OS process launch.
type processSpec struct {
	Path   string
	Args   []string
	Dir    string
	Env    []string
	Stderr io.Writer
}

// providerProcess owns one provider OS process tree: exactly one goroutine
// owns cmd.Wait, stop is explicit and bounded, and the process group is
// swept at lifecycle end so wrapper descendants cannot survive the leader.
// It owns OS process lifecycle only; protocol and session concepts stay in
// the adapters.
type providerProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *os.File

	done chan struct{}
	exit processExit

	requested atomic.Bool

	stopOnce sync.Once
	stopErr  error

	sweepOnce sync.Once
	sweepErr  error

	stdoutOnce   sync.Once
	stdoutCloses int
}

// startProviderProcess launches spec as a dedicated process-group leader with
// Spynel-owned stdin and stdout pipes. The child keeps its own stdout writer,
// so provider exit cannot close or truncate the read side while buffered
// output is still draining.
func startProviderProcess(spec processSpec) (*providerProcess, error) {
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	stdinRead, stdinWrite, err := os.Pipe()
	if err != nil {
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		return nil, err
	}
	cmd := exec.Command(spec.Path, spec.Args...)
	cmd.Dir = spec.Dir
	cmd.Env = spec.Env
	cmd.Stdin = stdinRead
	cmd.Stdout = stdoutWrite
	if spec.Stderr != nil {
		cmd.Stderr = spec.Stderr
	}
	cmd.WaitDelay = waitDelay
	configureProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		_ = stdinRead.Close()
		_ = stdinWrite.Close()
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		return nil, err
	}
	// The child owns its inherited endpoints. Drop the parent copies so the
	// reader observes EOF from the provider tree alone and cooperative stdin
	// EOF reaches the provider directly.
	_ = stdinRead.Close()
	_ = stdoutWrite.Close()
	process := &providerProcess{
		cmd:    cmd,
		stdin:  stdinWrite,
		stdout: stdoutRead,
		done:   make(chan struct{}),
	}
	go process.waitLoop()
	return process, nil
}

// waitLoop is the sole owner of cmd.Wait. It publishes the immutable exit
// snapshot, closes done, and only then sweeps the process group so
// descendants cannot survive a natural leader exit.
func (p *providerProcess) waitLoop() {
	err := p.cmd.Wait()
	p.exit = processExit{
		err:       err,
		requested: p.requested.Load(),
	}
	close(p.done)
	_ = p.sweepGroup()
}

// Done reports the process-exit notification channel.
func (p *providerProcess) Done() <-chan struct{} { return p.done }

// Result returns the immutable exit snapshot. It observes Done first, so it
// never re-reads live requested state and cannot rewrite history.
func (p *providerProcess) Result() processExit {
	<-p.done
	return p.exit
}

// exited reports process exit without waiting.
func (p *providerProcess) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// Stdin exposes the provider stdin writer for adapter protocol writes.
func (p *providerProcess) Stdin() io.WriteCloser { return p.stdin }

// Stdout exposes the provider stdout read end. The adapter reader owns it and
// must CloseStdout exactly once after draining.
func (p *providerProcess) Stdout() *os.File { return p.stdout }

// CloseStdout closes the read end exactly once. Only the reader closes it:
// neither Stop nor the wait owner may close stdout, or buffered provider
// output could be truncated while it drains.
func (p *providerProcess) CloseStdout() {
	p.stdoutOnce.Do(func() {
		p.stdoutCloses++
		_ = p.stdout.Close()
	})
}

// stdoutCloseCount reports how many times CloseStdout took effect. It exists
// for lifecycle tests inside this package.
func (p *providerProcess) stdoutCloseCount() int { return p.stdoutCloses }

// Stop terminates the provider process tree through one explicit bounded
// escalation: cooperative stdin EOF, group termination, group kill, and a
// final group sweep. It is idempotent; repeated and concurrent callers share
// the one sequence's result. Cancellation skips grace waiting but never
// skips safety cleanup, and signal errors never abandon later stages.
func (p *providerProcess) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() {
		// Mark requested before any stop action so every exit this sequence
		// causes classifies as requested. A Stop beginning after exit cannot
		// rewrite the already-published snapshot.
		p.requested.Store(true)
		var errs []error
		collect := func(err error) {
			if err != nil {
				errs = append(errs, err)
			}
		}
		cancelled := false
		if p.stdin != nil {
			_ = p.stdin.Close()
		}
		switch p.waitStage(ctx, stdinGrace) {
		case stageExited:
			collect(p.sweepGroup())
			p.stopErr = errors.Join(errs...)
			return
		case stageCancelled:
			cancelled = true
		case stageTimeout:
		}
		collect(terminateProcessGroup(p.cmd))
		if !cancelled {
			switch p.waitStage(ctx, termGrace) {
			case stageExited:
				collect(p.sweepGroup())
				p.stopErr = errors.Join(errs...)
				return
			case stageCancelled:
				cancelled = true
			case stageTimeout:
			}
		}
		collect(killProcessGroup(p.cmd))
		killElapsed := false
		if !cancelled {
			switch p.waitStage(ctx, killGrace) {
			case stageExited:
			case stageCancelled:
				cancelled = true
			case stageTimeout:
				killElapsed = true
			}
		}
		// The final sweep is unconditional: signalling problems never leave
		// descendants alive.
		collect(p.sweepGroup())
		if killElapsed && !p.exited() {
			errs = append(errs, fmt.Errorf("provider process %d did not exit after SIGKILL", p.cmd.Process.Pid))
		}
		if cancelled && !p.exited() {
			errs = append(errs, ctx.Err())
		}
		p.stopErr = errors.Join(errs...)
	})
	return p.stopErr
}

// waitStage waits for process exit, grace expiry, or caller cancellation.
func (p *providerProcess) waitStage(ctx context.Context, grace time.Duration) stageOutcome {
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-p.done:
		return stageExited
	case <-ctx.Done():
		return stageCancelled
	case <-timer.C:
		return stageTimeout
	}
}

// sweepGroup performs the one final kill of the provider process group. Both
// the wait owner and Stop may request it safely.
func (p *providerProcess) sweepGroup() error {
	p.sweepOnce.Do(func() {
		p.sweepErr = killProcessGroup(p.cmd)
	})
	return p.sweepErr
}

// Interrupt requests in-turn interruption: it delivers group SIGINT on
// Unix and the platform's direct-process fallback on Windows. It remains an
// interrupt operation, never a full Stop.
func (p *providerProcess) Interrupt() error {
	return interruptProcessGroup(p.cmd)
}
