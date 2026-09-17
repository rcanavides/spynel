package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/workspace"
)

type availabilityHarness struct {
	*fakeHarness
	start func() error
}

func (h *availabilityHarness) Start(context.Context) error {
	if h.start != nil {
		return h.start()
	}
	return nil
}

func TestUnavailableAndFencedProvidersDoNotClaimWorkflow(t *testing.T) {
	for _, phase := range []string{phaseTaskImplementation, phaseTaskReview} {
		for _, fenced := range []bool{false, true} {
			t.Run(phase+map[bool]string{false: "/unavailable", true: "/fenced"}[fenced], func(t *testing.T) {
				root := t.TempDir()
				if err := workspace.Init(root, false); err != nil {
					t.Fatal(err)
				}
				cfg, err := config.Load(config.PathForRoot(root))
				if err != nil {
					t.Fatal(err)
				}
				// Multiple queued documents exercise the scan loop: one
				// iteration's reservation must not accumulate with the next.
				tasks := make([]string, 0, 3)
				for i := 1; i <= 3; i++ {
					task, err := Create(cfg, "tasks", fmt.Sprintf("availability guard %d", i), "")
					if err != nil {
						t.Fatal(err)
					}
					tasks = append(tasks, task)
				}
				role := harness.RoleDeveloper
				source, claimed := "todo", "working"
				if phase == phaseTaskReview {
					role = harness.RoleReviewer
					source, claimed = "review", "reviewing"
					for i, task := range tasks {
						target := cfg.StatePath("tasks", source, filepath.Base(task))
						doc, e := ReadDocument(task)
						if e != nil {
							t.Fatal(e)
						}
						doc.FrontMatter["status"] = "review"
						if e = WriteDocument(task, doc); e != nil {
							t.Fatal(e)
						}
						if e = os.Rename(task, target); e != nil {
							t.Fatal(e)
						}
						tasks[i] = target
					}
				}
				registry := harness.NewRegistry()
				chat := newFakeRecipient()
				provider := &availabilityHarness{fakeHarness: newFakeRecipient()}
				if !fenced {
					provider.start = func() error { return errors.New("provider unavailable") }
				}
				registry.Register("chat", func(harness.HarnessConfig) (harness.Harness, error) { return chat, nil })
				registry.Register("routed", func(harness.HarnessConfig) (harness.Harness, error) { return provider, nil })
				spec := harness.RuntimeSpec{Providers: map[harness.ProviderID]harness.HarnessConfig{"chat": {Name: "chat"}, "routed": {Name: "routed"}}, Roles: map[harness.Role]harness.ProviderID{harness.RoleChat: "chat", role: "routed"}}
				runtime, e := harness.NewRuntimeSpec(registry, spec)
				if e != nil {
					t.Fatal(e)
				}
				defer runtime.Close()
				if e = runtime.Start(context.Background()); e != nil {
					t.Fatal(e)
				}
				var settle func()
				if fenced {
					entered, release := make(chan struct{}), make(chan struct{})
					done := make(chan error, 1)
					registry.Register("routed", func(harness.HarnessConfig) (harness.Harness, error) {
						return &availabilityHarness{fakeHarness: newFakeRecipient(), start: func() error { close(entered); <-release; return nil }}, nil
					})
					next := spec.Providers["routed"]
					next.Sandbox = "read-only"
					spec.Providers["routed"] = next
					go func() { done <- runtime.Reconcile(context.Background(), spec) }()
					<-entered
					settle = func() {
						close(release)
						if e := <-done; e != nil {
							t.Error(e)
						}
					}
					defer settle()
				}
				manager := New(cfg, runtime.AcquireRole(harness.RoleChat), extensions.Runner{})
				manager.HarnessRouter = runtime
				if e = manager.scanPhaseQueue(context.Background(), workflowRoutes()[0], cfg.StatePath("tasks", source), cfg.StatePath("tasks", claimed), phase); e != nil {
					t.Fatal(e)
				}
				manager.Wait()
				for _, task := range tasks {
					if _, e = os.Stat(task); e != nil {
						t.Fatal("unavailable provider claimed task: ", e)
					}
				}
				leases, e := manager.loadLeases()
				if e != nil || len(leases) != 0 {
					t.Fatalf("unavailable provider wrote leases: %v %v", leases, e)
				}
				if provider.calls != 0 || chat.calls != 0 {
					t.Fatal("unavailable role dispatched or fell back")
				}
				// Existing recovery also waits without changing durable counters or state.
				lease := Lease{ID: "existing", Route: "tasks", OwnerID: "prior", SessionKey: "recover", Phase: phase, RecoveryCount: 3, State: "processing", File: tasks[0]}
				if e = manager.saveLease(lease); e != nil {
					t.Fatal(e)
				}
				manager.dispatch(context.Background(), workflowRoutes()[0], lease, true)
				manager.Wait()
				if e = manager.recoverStale(context.Background()); e != nil {
					t.Fatal(e)
				}
				after, e := manager.loadLease(lease.ID)
				if e != nil || after.RecoveryCount != 3 || after.State != "processing" || after.LastError != "" || after.OwnerID != "prior" || !after.HeartbeatAt.IsZero() {
					t.Fatalf("structural absence became recovery failure: %+v %v", after, e)
				}
			})
		}
	}
}

type midTurnDeathHarness struct {
	*fakeHarness
}

func (h *midTurnDeathHarness) Send(context.Context, string, string, core.Emit) (string, bool, error) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	return "", false, fmt.Errorf("%w: harness supervisor is closed", harness.ErrProviderUnavailable)
}

func TestMidTurnProviderDeathRecordsDurableLeaseError(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	task, err := Create(cfg, "tasks", "mid-turn provider death", "")
	if err != nil {
		t.Fatal(err)
	}
	working := cfg.StatePath("tasks", "working", filepath.Base(task))
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["status"] = "working"
	if err = WriteDocument(task, document); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(task, working); err != nil {
		t.Fatal(err)
	}
	registry := harness.NewRegistry()
	chat := newFakeRecipient()
	provider := &midTurnDeathHarness{fakeHarness: newFakeRecipient()}
	registry.Register("chat", func(harness.HarnessConfig) (harness.Harness, error) { return chat, nil })
	registry.Register("routed", func(harness.HarnessConfig) (harness.Harness, error) { return provider, nil })
	spec := harness.RuntimeSpec{Providers: map[harness.ProviderID]harness.HarnessConfig{"chat": {Name: "chat"}, "routed": {Name: "routed"}}, Roles: map[harness.Role]harness.ProviderID{harness.RoleChat: "chat", harness.RoleDeveloper: "routed"}}
	runtime, err := harness.NewRuntimeSpec(registry, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if err = runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager := New(cfg, runtime.AcquireRole(harness.RoleChat), extensions.Runner{})
	manager.HarnessRouter = runtime
	lease := Lease{ID: "mid-turn", Route: "tasks", OwnerID: "death", SessionKey: "tasks-mid-turn", Phase: phaseTaskImplementation, RecoveryCount: 0, State: "processing", File: working}
	if err = manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	manager.dispatch(context.Background(), workflowRoutes()[0], lease, false)
	manager.Wait()
	after, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != "error" {
		t.Fatalf("mid-turn provider death lease state=%s", after.State)
	}
	if after.LastError == "" {
		t.Fatal("mid-turn provider death left no durable lease error")
	}
}
