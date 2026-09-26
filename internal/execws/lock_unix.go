//go:build !windows

package execws

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// lockFile takes the exclusive cross-process lock at path and returns its
// releaser. It serializes repository-global integration across processes.
func lockFile(path string) (func(), error) {
	if err := os.MkdirAll(dirOf(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open integration lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock integration: %w", err)
	}
	return func() {
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		_ = file.Close()
	}, nil
}

func dirOf(path string) string {
	index := len(path)
	for index > 0 && !os.IsPathSeparator(path[index-1]) {
		index--
	}
	if index == 0 {
		return "."
	}
	return path[:index-1]
}

// tryLockFile attempts one nonblocking exclusive lock; tests use it to prove
// the integration lock excludes other descriptors and processes.
func tryLockFile(path string) (func(), bool, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, fmt.Errorf("open integration lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("try lock integration: %w", err)
	}
	return func() {
		_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
		_ = file.Close()
	}, true, nil
}
