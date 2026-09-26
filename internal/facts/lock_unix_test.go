//go:build !windows

package facts

import (
	"bufio"
	"os"
	"os/exec"
	"testing"
)

// FJ2: the journal lock excludes other processes. The test re-executes itself
// as a helper child that holds the lock; the parent proves exclusion with
// nonblocking lock attempts before and after the child releases.
func TestFJ2CrossProcessLockExcludesOtherProcess(t *testing.T) {
	if dir := os.Getenv("SPYNEL_FACTS_LOCK_CHILD"); dir != "" {
		factsLockChildMain(dir)
		return
	}
	journal := Open(t.TempDir())
	if err := journal.ensureDirectories(); err != nil {
		t.Fatalf("ensure directories: %v", err)
	}
	child := exec.Command(os.Args[0], "-test.run", "^"+t.Name()+"$", "-test.count=1")
	child.Env = append(os.Environ(), "SPYNEL_FACTS_LOCK_CHILD="+journal.dir)
	stdin, err := child.StdinPipe()
	if err != nil {
		t.Fatalf("child stdin: %v", err)
	}
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatalf("child stdout: %v", err)
	}
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	// The child signals only after it actually holds the lock, so the parent's
	// exclusion probe never races acquisition.
	reader := bufio.NewReader(stdout)
	if line, err := reader.ReadString('\n'); err != nil || line != "locked\n" {
		t.Fatalf("child ready signal = %q err=%v, want %q", line, err, "locked\n")
	}
	if unlock, held, err := tryLockFile(journal.lockPath()); err != nil {
		t.Fatalf("try lock while child holds it: %v", err)
	} else if held {
		unlock()
		t.Fatal("cross-process lock was not exclusive")
	}
	if _, err := stdin.Write([]byte("release\n")); err != nil {
		t.Fatalf("signal child: %v", err)
	}
	if err := child.Wait(); err != nil {
		t.Fatalf("child exit: %v", err)
	}
	// After release the lock is available again.
	unlock, held, err := tryLockFile(journal.lockPath())
	if err != nil {
		t.Fatalf("try lock after release: %v", err)
	}
	if !held {
		t.Fatal("lock must be available after the child released it")
	}
	unlock()
	// A real append still works through the journal.
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "k", Doc: testDoc(DocKindTask, "locked"), Launch: "ln"}); err != nil {
		t.Fatalf("Append after cross-process lock exercise: %v", err)
	}
}

// factsLockChildMain acquires the lock, signals readiness, and holds it until
// the parent responds on stdin.
func factsLockChildMain(dir string) {
	journal := Open(dir)
	unlock, err := lockFile(journal.lockPath())
	if err != nil {
		os.Exit(3)
	}
	os.Stdout.WriteString("locked\n")
	reader := bufio.NewReader(os.Stdin)
	if _, err := reader.ReadString('\n'); err != nil {
		unlock()
		os.Exit(4)
	}
	unlock()
	os.Exit(0)
}

// The in-process mutex plus flock keeps a second descriptor out while held.
func TestFJ2InProcessLockExclusion(t *testing.T) {
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
		t.Fatal("a second descriptor must not take the held lock")
	}
	unlock()
	_, held, err := tryLockFile(journal.lockPath())
	if err != nil || !held {
		t.Fatalf("lock must be free after unlock: held=%v err=%v", held, err)
	}
}
