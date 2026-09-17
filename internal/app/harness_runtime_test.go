package app

import (
	"context"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/orchestrator"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/workspace"
)

type lifecycleServiceHarness struct {
	*serviceHarness
	starts atomic.Int32
	closes atomic.Int32
}

func (h *lifecycleServiceHarness) Start(context.Context) error { h.starts.Add(1); return nil }
func (h *lifecycleServiceHarness) Close() error                { h.closes.Add(1); return nil }

func TestServiceSingleProviderRuntimeOwnsLifecycleAndSettings(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "acp"
	cfg.Harness.ACPCommand, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}
	registry := harness.NewRegistry()
	var targets []*lifecycleServiceHarness
	registry.Register("acp", func(c harness.HarnessConfig) (harness.Harness, error) {
		if c.SessionsFile != cfg.HarnessSessionsPath("acp") {
			t.Errorf("sessions file=%s", c.SessionsFile)
		}
		h := &lifecycleServiceHarness{serviceHarness: newServiceHarness()}
		targets = append(targets, h)
		return h, nil
	})
	providers := harness.NewRuntime(registry, harness.HarnessConfig{Name: "acp", SessionsFile: cfg.HarnessSessionsPath("acp")})
	service := NewWithHarnessRuntime(cfg, providers, NewRuntime())
	defer service.Close()
	if service.Orchestrator.HarnessRouter != providers {
		t.Fatal("orchestrator router is not Runtime")
	}
	if service.Harness != providers.AcquireRole(harness.RoleChat) || service.Orchestrator.Harness != service.Harness {
		t.Fatal("service and orchestrator do not share runtime target")
	}
	if _, ok := service.Harness.(interface{ Close() error }); ok {
		t.Fatal("application target exposes lifecycle")
	}
	// Configuration before Start must not construct or start a provider.
	if _, err := service.ApplySettings(map[string]string{"harness.sandbox": "read-only"}); err != nil {
		t.Fatal(err)
	}
	if len(targets) != 0 {
		t.Fatal("pre-start configuration constructed provider")
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 || targets[0].starts.Load() != 1 {
		t.Fatal("duplicate primary provider")
	}
	if _, err := service.ApplySettings(map[string]string{"harness.sandbox": "danger-full-access"}); err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].closes.Load() != 1 || targets[1].starts.Load() != 1 {
		t.Fatal("structural replacement lifecycle incorrect")
	}
	loaded, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Harness.Sandbox != "danger-full-access" || providers.HarnessConfig().Sandbox != loaded.Harness.Sandbox {
		t.Fatal("settings and runtime differ")
	}
	// The server-term stop path followed by service cleanup must retire once.
	if err := service.ClosePrimaryHarness(); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if err := providers.Close(); err != nil {
		t.Fatal(err)
	}
	if targets[1].closes.Load() != 1 {
		t.Fatalf("provider closed %d times", targets[1].closes.Load())
	}
}

func TestRuntimeJobsAttributeExactAdmittedProvider(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "agent-zero"
	registry := harness.NewRegistry()
	spec := harness.RuntimeSpec{Providers: map[harness.ProviderID]harness.HarnessConfig{}, Roles: map[harness.Role]harness.ProviderID{harness.RoleChat: "agent-zero", harness.RoleDeveloper: "codex", harness.RoleReviewer: "claude-code"}}
	for _, id := range []harness.ProviderID{"agent-zero", "codex", "claude-code"} {
		target := newHeldServiceHarness()
		registry.Register(string(id), func(harness.HarnessConfig) (harness.Harness, error) { return target, nil })
		spec.Providers[id] = harness.HarnessConfig{Name: string(id)}
	}
	providers, err := harness.NewRuntimeSpec(registry, spec)
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithHarnessRuntime(cfg, providers, NewRuntime())
	defer service.Close()
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		role        harness.Role
		phase, want string
	}{{harness.RoleDeveloper, "task_implementation", "codex"}, {harness.RoleReviewer, "task_review", "claude-code"}, {harness.RoleNotification, "notification", "agent-zero"}, {harness.RoleHeartbeat, "semantic_heartbeat", "agent-zero"}} {
		key := "job:" + test.phase
		target := providers.AcquireRole(test.role)
		_, release, e := harness.ReserveExecution(target, key)
		if e != nil {
			t.Fatal(e)
		}
		// Remap between admission and job creation. The reservation is authoritative.
		saved := spec.Roles[test.role]
		spec.Roles[test.role] = "agent-zero"
		if e = providers.Reconcile(context.Background(), spec); e != nil {
			t.Fatal(e)
		}
		id, e := service.Orchestrator.JobStarted(orchestrator.Lease{SessionKey: key, Phase: test.phase}, "job", time.Time{}, 1, 0)
		if e != nil {
			t.Fatal(e)
		}
		job, ok := service.Runtime.Job(id)
		if !ok || job.Provider != test.want {
			t.Fatalf("phase %s provider=%q want=%q", test.phase, job.Provider, test.want)
		}
		if _, _, e = target.Send(context.Background(), key, "prompt", nil); e != nil {
			t.Fatal(e)
		}
		release()
		if got := harness.ExecutionProvider(target, key); string(got) != job.Provider {
			t.Fatalf("job=%s actual=%s", job.Provider, got)
		}
		if saved == "" {
			delete(spec.Roles, test.role)
		} else {
			spec.Roles[test.role] = saved
		}
		if e = providers.Reconcile(context.Background(), spec); e != nil {
			t.Fatal(e)
		}
	}
	message := core.Message{Channel: "cli", Conversation: "provider", Text: "hello"}
	if err = service.Handle(context.Background(), message, func(core.Event) {}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, job := range service.Runtime.Jobs() {
		if job.Kind == "conversation" {
			found = true
			if job.Provider != "agent-zero" {
				t.Fatal("chat attributed to ", job.Provider)
			}
		}
	}
	if !found {
		t.Fatal("chat job missing")
	}
}
