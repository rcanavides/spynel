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

	service := New(cfg, primary)

	roleSet := harness.NewRoleSet(
		primary,
		map[harness.Role]harness.Harness{
			harness.RoleDeveloper: developer,
		},
		developer,
	)

	service.SetRoleHarnessRuntime(roleSet)

	if service.Orchestrator.HarnessRouter != roleSet {
		t.Fatal("role router was not attached to orchestrator")
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

	if got := service.Orchestrator.HarnessRouter.HarnessForRole(harness.RoleDeveloper); got != developer {
		t.Fatal("developer role did not resolve to routed harness")
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
