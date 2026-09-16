package app

import (
	"context"
	"os"
	"sync/atomic"
	"testing"

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
