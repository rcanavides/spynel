//go:build windows

package harness

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// Windows full descendant cleanup is deliberately NOT implemented in C8.2:
// there is no Job Object subsystem, so descendants of a provider process are
// NOT guaranteed to be cleaned up when the direct process stops. Escalation
// falls back to direct process termination, which is the same effective
// lifecycle the previous context-owned kills provided.

// configureProcessGroup at least isolates the provider in its own Windows
// process group.
func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// interruptProcessGroup preserves the effective pre-C8.2 Windows behavior.
// Windows has no deliverable in-process SIGINT, so provider interruption
// falls back to terminating the direct provider process through the same
// direct-kill escalation the stop path uses. This is not a true SIGINT, and
// descendants remain outside C8.2 ownership on Windows.
func interruptProcessGroup(cmd *exec.Cmd) error {
	return killDirectProcess(cmd)
}

// terminateProcessGroup escalates through the direct process kill fallback.
func terminateProcessGroup(cmd *exec.Cmd) error {
	return killDirectProcess(cmd)
}

// killProcessGroup has no group sweep on Windows; direct escalation above is
// the C8.2 fallback.
func killProcessGroup(_ *exec.Cmd) error {
	return nil
}

// killDirectProcess terminates the provider's direct OS process. An already
// finished process is success.
func killDirectProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
