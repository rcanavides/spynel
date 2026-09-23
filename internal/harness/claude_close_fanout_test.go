package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

// The Claude close fanout test uses fixture processes that never finish their
// turns naturally. Each held process records when the cooperative stop stage
// reaches it; a concurrent Close must reach every process's bounded stop
// together, while a serial one could not reach the second process until the
// first stop finished its full escalation grace.

// stopEntriedCount reads the shared fixture log and counts how many held
// provider processes have already entered their cooperative stop stage.
func stopEntriedCount(t *testing.T, logPath string) int {
	t.Helper()
	count := 0
	for _, record := range readFixtureRecords(t, logPath) {
		if record.Kind == "stop-entered" {
			count++
		}
	}
	return count
}

// awaitAllStopsEntered fails the test unless every held process records its
// cooperative stop stage within the grace window, which a serial stop loop
// can never satisfy: its second stop only begins after the first bounded
// escalation has fully expired.
func awaitAllStopsEntered(t *testing.T, logPath string, want int, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		count := stopEntriedCount(t, logPath)
		if count >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d held %s processes entered their bounded stops; process stops did not fan out concurrently", count, want, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// awaitTurnInactive waits for the turn's own reader goroutine to observe the
// stopped process and settle the admission; Close waits for the bounded Stops,
// not for the terminal event delivery that follows them.
func awaitTurnInactive(t *testing.T, what string, active func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for active() {
		if time.Now().After(deadline) {
			t.Fatalf("%s remained active after Close", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestClaudeCloseStopsActiveTurnProcessesConcurrently(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "claude-hold")
	claude, err := NewClaude(HarnessConfig{Command: command, Cwd: root, ApprovalPolicy: "never"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := claude.Start(ctx); err != nil {
		t.Fatal(err)
	}
	const turns = 3
	for index := 0; index < turns; index++ {
		key := fmt.Sprintf("hold-%d", index)
		if _, steered, err := claude.Send(ctx, key, "held work", func(core.Event) {}); err != nil || steered {
			t.Fatalf("held Send(%q) = steered %t, %v", key, steered, err)
		}
		if !claude.IsActive(key) {
			t.Fatalf("held turn %q was not active before Close", key)
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- claude.Close() }()
	awaitAllStopsEntered(t, logPath, turns, "Claude")
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Claude.Close never finished the bounded stop fanout")
	}
	for index := 0; index < turns; index++ {
		key := fmt.Sprintf("hold-%d", index)
		awaitTurnInactive(t, "held turn "+key, func() bool { return claude.IsActive(key) })
	}
	if err := claude.Close(); err != nil {
		t.Fatal(err)
	}
}
