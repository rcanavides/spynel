//go:build !windows

package execws

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// CK3: a timeout kills the whole descendant process group; cancellation does
// the same. Timeouts are failure guards, never correctness proof.
func TestCK3TimeoutKillsProcessGroup(t *testing.T) {
	ctx := context.Background()
	ws, resultSHA, req := checkProbe(t, "content")
	// The check spawns a child that outlives the leader; both must die. The
	// pid file lives outside the workspace so the instrumentation itself does
	// not count as a source mutation.
	pidFile := filepath.Join(t.TempDir(), "group-child.pid")
	script := "sleep 30 & echo $! > " + quoteShell(pidFile) + "; wait"
	got, err := ws.RunCheck(ctx, CheckRequest{
		LaunchID:  req.LaunchID,
		Def:       CheckDef{ID: "probe-timeout", Command: "sh", Args: []string{"-c", script}, Timeout: 500 * time.Millisecond},
		ResultSHA: resultSHA,
	})
	if err != nil {
		t.Fatalf("RunCheck: %v", err)
	}
	if got.Outcome != CheckOutcomeFailed {
		t.Fatalf("timeout outcome = %+v", got)
	}
	// The recorded child pid must no longer be alive: the group was killed.
	pidData, readErr := os.ReadFile(pidFile)
	if readErr != nil {
		t.Fatalf("read child pid: %v", readErr)
	}
	var pidValue int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(pidData)), "%d", &pidValue); err != nil {
		t.Fatalf("parse child pid: %v", err)
	}
	if err := syscall.Kill(pidValue, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant %d survived the group kill: %v", pidValue, err)
	}
	// A context cancelled before the check even starts still records a failed
	// outcome without leaking any process.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	got, err = ws.RunCheck(cancelled, CheckRequest{
		LaunchID:  req.LaunchID,
		Def:       CheckDef{ID: "probe-cancel", Command: "sleep", Args: []string{"30"}, Timeout: time.Minute},
		ResultSHA: resultSHA,
	})
	if err == nil || got.Outcome != CheckOutcomeFailed {
		t.Fatalf("cancellation outcome = %+v err=%v", got, err)
	}
}
