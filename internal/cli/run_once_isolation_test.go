package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/orchestrator"
	"github.com/agent0ai/spynel/internal/workspace"
)

type signaledRunOnceHarness struct {
	*heldCLIHarness
	entered chan string
}

func (h *signaledRunOnceHarness) Send(ctx context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	thread, steered, err := h.heldCLIHarness.Send(ctx, key, prompt, emit)
	h.entered <- key
	return thread, steered, err
}

func TestRunOnceWaitsForDispatchedWorkAfterScanError(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orchestrator.Create(cfg, "tasks", "valid queued work", ""); err != nil {
		t.Fatal(err)
	}
	fragment := filepath.Join(cfg.StatePath("tasks", "review"), "malformed.md")
	if err := os.WriteFile(fragment, []byte("malformed fragment\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lease := orchestrator.Lease{ID: "bad-run-once", Route: "tasks", File: filepath.Join(cfg.StatePath("tasks", "reviewing"), "malformed.md"),
		SessionKey: "bad-run-once", State: "awaiting_transition", Phase: "task_review", StartedAt: time.Now().UTC(), HeartbeatAt: time.Now().UTC()}
	data, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cfg.StatePath("runtime", "leases"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.StatePath("runtime", "leases", lease.ID+".json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	target := &signaledRunOnceHarness{heldCLIHarness: newHeldCLIHarness(), entered: make(chan string, 1)}
	manager := orchestrator.New(cfg, target, extensions.Runner{})
	done := make(chan error, 1)
	go func() { done <- manager.RunOnce(context.Background()) }()
	var key string
	select {
	case key = <-target.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("queued work was not dispatched")
	}
	select {
	case err := <-done:
		t.Fatalf("run once returned while work remained active: %v", err)
	default:
	}
	target.finish(key, "done")
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), lease.ID) {
			t.Fatalf("run once lost scan error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run once did not finish after provider completion")
	}
}
