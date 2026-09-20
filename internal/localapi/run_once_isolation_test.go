package localapi

import (
	"context"
	"encoding/json"
	"net/http"
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

type settlingRunOnceHarness struct {
	*apiHarness
	settle func()
}

func (h *settlingRunOnceHarness) Send(_ context.Context, key, _ string, emit core.Emit) (string, bool, error) {
	h.settle()
	emit(core.Event{Kind: core.EventFinal, Text: "done", ThreadID: "thread-" + key, Done: true})
	return "thread-" + key, false, nil
}

// J16: the run-once endpoint settles the durable consequences of the work it
// dispatched and returns 204, with all settlement owned by the shared
// orchestrator RunOnce rather than endpoint logic.
func TestRunOnceEndpointSettlesTransitionAndReturnsNoContent(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	taskPath, err := orchestrator.CreateWithOptions(cfg, "tasks", "settle over the local API", "", orchestrator.CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Dir(filepath.Dir(taskPath))
	working := filepath.Join(base, "working", filepath.Base(taskPath))
	done := filepath.Join(base, "done", filepath.Base(taskPath))
	target := &settlingRunOnceHarness{apiHarness: newAPIHarness(), settle: func() {
		document, readErr := orchestrator.ReadDocument(working)
		if readErr != nil {
			t.Errorf("read working task: %v", readErr)
			return
		}
		now := time.Now().UTC().Truncate(time.Second)
		document.FrontMatter["status"] = "done"
		document.FrontMatter["updated_at"] = now.Format(time.RFC3339)
		document.FrontMatter["completion_summary"] = map[string]any{
			"verdict": "completed", "outcome": "Collected the requested status.",
			"evidence":     "Local status output only.",
			"uncertainty":  "Remote health remains uncertain.",
			"completed_at": now.Format(time.RFC3339),
		}
		if writeErr := orchestrator.WriteDocument(working, document); writeErr != nil {
			t.Errorf("write settled task: %v", writeErr)
			return
		}
		if renameErr := os.Rename(working, done); renameErr != nil {
			t.Errorf("move settled task: %v", renameErr)
		}
	}}
	service := app.New(cfg, target)
	defer service.Close()
	finished := 0
	service.Orchestrator.JobStarted = func(orchestrator.Lease, string, time.Time, int, int) (int, error) {
		return 7, nil
	}
	service.Orchestrator.JobFinished = func(int) { finished++ }
	server := &Server{Service: service}
	response := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/v1/run-once", nil)
	server.runOnce(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("run once status = %d, body %s", response.Code, response.Body.String())
	}
	settled, err := orchestrator.ReadDocument(done)
	if err != nil || settled.FrontMatter["status"] != "done" {
		t.Fatalf("settled task = %v at %s, err %v", settled.FrontMatter["status"], done, err)
	}
	entries, err := os.ReadDir(cfg.StatePath("runtime", "leases"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("leases after settled run once = %d entries, err %v", len(entries), err)
	}
	if finished != 1 {
		t.Fatalf("JobFinished calls = %d, want exactly 1", finished)
	}
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
