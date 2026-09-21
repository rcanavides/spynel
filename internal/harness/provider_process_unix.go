//go:build !windows

package harness

import (
	"errors"
	"os/exec"
	"syscall"
)

// configureProcessGroup makes every provider process a dedicated process
// group leader so its whole OS process tree can be signalled and swept
// together, and a wrapper cannot shield descendants merely by exiting first.
func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// signalProcessGroup signals the provider's whole process group. A group
// that is already gone is success, not failure.
func signalProcessGroup(cmd *exec.Cmd, signal syscall.Signal) error {
	if cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// interruptProcessGroup delivers SIGINT to the provider group for in-turn
// interruption.
func interruptProcessGroup(cmd *exec.Cmd) error {
	return signalProcessGroup(cmd, syscall.SIGINT)
}

// terminateProcessGroup delivers the cooperative-stop SIGTERM to the provider
// group.
func terminateProcessGroup(cmd *exec.Cmd) error {
	return signalProcessGroup(cmd, syscall.SIGTERM)
}

// killProcessGroup SIGKILLs the provider group, including descendants that
// outlived their wrapper leader.
func killProcessGroup(cmd *exec.Cmd) error {
	return signalProcessGroup(cmd, syscall.SIGKILL)
}
