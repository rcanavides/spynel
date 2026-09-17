package app

import (
	"context"
	"testing"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/harness"
)

type lifecycleHarness struct {
	starts int
	closes int
}

func (h *lifecycleHarness) Start(context.Context) error {
	h.starts++
	return nil
}

func (h *lifecycleHarness) Send(
	context.Context,
	string,
	string,
	core.Emit,
) (string, bool, error) {
	return "", false, nil
}

func (*lifecycleHarness) Interrupt(context.Context, string) (bool, error) {
	return false, nil
}

func (*lifecycleHarness) ResetSession(string) error {
	return nil
}

func (*lifecycleHarness) ThreadID(string) string {
	return ""
}

func (*lifecycleHarness) IsActive(string) bool {
	return false
}

func (h *lifecycleHarness) Close() error {
	h.closes++
	return nil
}

func TestServiceOwnsRoutedHarnessLifecycle(t *testing.T) {
	cfg := config.Default()
	cfg.Root = t.TempDir()

	primary := &lifecycleHarness{}
	developer := &lifecycleHarness{}

	registry := harness.NewRegistry()
	registry.Register("chat", func(harness.HarnessConfig) (harness.Harness, error) { return primary, nil })
	registry.Register("developer", func(harness.HarnessConfig) (harness.Harness, error) { return developer, nil })
	providers, err := harness.NewRuntimeSpec(registry, harness.RuntimeSpec{Providers: map[harness.ProviderID]harness.HarnessConfig{"chat": {Name: "chat"}, "developer": {Name: "developer"}}, Roles: map[harness.Role]harness.ProviderID{harness.RoleChat: "chat", harness.RoleDeveloper: "developer"}})
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithHarnessRuntime(cfg, providers, NewRuntime())
	if service.Orchestrator.HarnessRouter != providers {
		t.Fatal("different topology owner")
	}

	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	if primary.starts != 1 {
		t.Fatalf("primary starts = %d, want 1", primary.starts)
	}
	if developer.starts != 1 {
		t.Fatalf("developer starts = %d, want 1", developer.starts)
	}

	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	if primary.closes != 1 {
		t.Fatalf("primary closes = %d, want 1", primary.closes)
	}
	if developer.closes != 1 {
		t.Fatalf("developer closes = %d, want 1", developer.closes)
	}
}
