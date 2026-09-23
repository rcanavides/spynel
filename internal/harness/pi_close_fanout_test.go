package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

// The Pi close fanout test holds one provider process per session key open.
// Close must run every per-session bounded stop concurrently: all held
// processes record their cooperative stop stage together, which a serial stop
// loop can never reach within the grace window.
func TestPiCloseStopsSessionProcessesConcurrently(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "pi-hold")
	pi, err := NewPi(HarnessConfig{Command: command, Cwd: root, SessionsFile: fmt.Sprintf("%s/sessions.json", root)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pi.Start(ctx); err != nil {
		t.Fatal(err)
	}
	const sessions = 3
	for index := 0; index < sessions; index++ {
		key := fmt.Sprintf("hold-%d", index)
		if _, steered, err := pi.Send(ctx, key, "held work", func(core.Event) {}); err != nil || steered {
			t.Fatalf("held Pi send(%q) = steered %t, %v", key, steered, err)
		}
		if !pi.IsActive(key) {
			t.Fatalf("held Pi session %q was not active before Close", key)
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- pi.Close() }()
	awaitAllStopsEntered(t, logPath, sessions, "Pi")
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Pi.Close never finished the bounded stop fanout")
	}
	for index := 0; index < sessions; index++ {
		key := fmt.Sprintf("hold-%d", index)
		awaitTurnInactive(t, "held Pi session "+key, func() bool { return pi.IsActive(key) })
	}
	if err := pi.Close(); err != nil {
		t.Fatal(err)
	}
}
