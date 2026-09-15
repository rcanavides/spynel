package harness

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

func TestACPCapturesLateFinalChunkAfterPromptResult(t *testing.T) {
	command, root, _ := portableHarnessFixture(t, "acp-late-final-chunk")

	adapter, err := NewACP(HarnessConfig{
		Command:      command,
		Cwd:          root,
		SessionsFile: filepath.Join(root, "sessions.json"),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := adapter.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	done := make(chan core.Event, 1)

	if _, _, err := adapter.Send(ctx, "late-final", "test", func(event core.Event) {
		if event.Done {
			done <- event
		}
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-done:
		if event.Kind != core.EventFinal ||
			event.Text != "LATE_FINAL_OK" ||
			event.FinalText == nil ||
			*event.FinalText != "LATE_FINAL_OK" {
			t.Fatalf("late ACP final = %#v", event)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for late ACP final chunk")
	}
}
