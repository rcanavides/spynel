//go:build windows

package execws

import (
	"context"
	"os/exec"
)

// newCheckCommand uses the accepted direct-process kill on Windows. Job
// Objects are deliberately out of scope for C9-F.
func newCheckCommand(ctx context.Context, command string, args []string) *exec.Cmd {
	return exec.CommandContext(ctx, command, args...)
}

// isProcessKilled reports whether a check failure came from the process being
// killed (timeout or cancellation) rather than exiting on its own. Windows
// direct-kill termination is accepted as an ordinary failed exit; the outcome
// is identical.
func isProcessKilled(err error) bool {
	return false
}
