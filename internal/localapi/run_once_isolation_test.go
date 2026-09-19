package localapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/app"
	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/orchestrator"
	"github.com/agent0ai/spynel/internal/workspace"
)

type gatedRunOnceHarness struct {
	*apiHarness
	entered chan struct{}
	release chan struct{}
}

func (h *gatedRunOnceHarness) Send(_ context.Context, key, _ string, emit core.Emit) (string, bool, error) {
	close(h.entered)
	<-h.release
	emit(core.Event{Kind: core.EventFinal, Text: "done", Done: true})
	return "thread-" + key, false, nil
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
	target := &gatedRunOnceHarness{apiHarness: newAPIHarness(), entered: make(chan struct{}), release: make(chan struct{})}
	service := app.New(cfg, target)
	defer service.Close()
	server := &Server{Service: service}
	response := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/v1/run-once", nil)
	done := make(chan struct{})
	go func() { server.runOnce(response, request); close(done) }()
	select {
	case <-target.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("queued work was not dispatched")
	}
	select {
	case <-done:
		t.Fatal("run once returned while dispatched work remained active")
	default:
	}
	close(target.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run once did not finish after provider completion")
	}
	if response.Code == 204 || !strings.Contains(response.Body.String(), lease.ID) {
		t.Fatalf("run once lost scan error: status=%d, body=%s", response.Code, response.Body.String())
	}
}
