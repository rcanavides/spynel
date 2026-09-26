//go:build windows

package facts

import (
	"testing"
)

// FJ2 (Windows surface): the journal lock file takes and releases an
// exclusive byte-range lock. Cross-process exclusion is enforced by
// LockFileEx; a second immediate attempt on a held lock fails.
func TestFJ2LockTakeAndRelease(t *testing.T) {
	journal := Open(t.TempDir())
	if err := journal.ensureDirectories(); err != nil {
		t.Fatalf("ensure directories: %v", err)
	}
	unlock, err := lockFile(journal.lockPath())
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if _, held, err := tryLockFile(journal.lockPath()); err != nil {
		t.Fatalf("try lock: %v", err)
	} else if held {
		t.Fatal("a second handle must not take the held lock")
	}
	unlock()
	secondUnlock, held, err := tryLockFile(journal.lockPath())
	if err != nil {
		t.Fatalf("try lock after release: %v", err)
	}
	if !held {
		t.Fatal("lock must be free after unlock")
	}
	secondUnlock()
}
