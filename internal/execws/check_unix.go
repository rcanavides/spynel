//go:build !windows

package execws

import (
	"context"
	"errors"
	"os/exec"
	"syscall"
	"time"
)

// newCheckCommand starts the check in its own process group so a timeout or
// cancellation can kill the entire descendant tree, not only the leader.
func newCheckCommand(ctx context.Context, command string, args []string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			// Negative pid addresses the whole process group.
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

// isProcessKilled reports whether a check failure came from the process being
// killed (timeout or cancellation) rather than exiting on its own.
func isProcessKilled(err error) bool {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.Sys().(syscall.WaitStatus).Signaled()
	}
	return false
}
