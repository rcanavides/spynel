package config

import (
	"testing"

	providerharness "github.com/agent0ai/spynel/internal/harness"
)

func TestHarnessRoutingFallsBackToDefaultHarness(t *testing.T) {
	cfg := Default()
	cfg.Harness.Name = "claude-code"

	if got := cfg.Harness.NameForRole(providerharness.RoleDeveloper); got != "claude-code" {
		t.Fatalf("developer fallback = %q, want claude-code", got)
	}

	if cfg.Harness.RoleRoutingEnabled() {
		t.Fatal("routing unexpectedly enabled without explicit routes")
	}
}

func TestHarnessRoutingSelectsRolesIndependently(t *testing.T) {
	cfg := Default()
	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Routing = &HarnessRouting{
		Developer:    "codex",
		Reviewer:     "claude-code",
		Notification: "claude-code",
		Heartbeat:    "codex",
	}

	tests := []struct {
		role providerharness.Role
		want string
	}{
		{providerharness.RoleChat, "agent-zero"},
		{providerharness.RoleDeveloper, "codex"},
		{providerharness.RoleReviewer, "claude-code"},
		{providerharness.RoleNotification, "claude-code"},
		{providerharness.RoleHeartbeat, "codex"},
	}

	for _, test := range tests {
		if got := cfg.Harness.NameForRole(test.role); got != test.want {
			t.Fatalf("role %q = %q, want %q", test.role, got, test.want)
		}
	}

	if !cfg.Harness.RoleRoutingEnabled() {
		t.Fatal("routing should be enabled")
	}
}

func TestHarnessRoutingNormalizesNames(t *testing.T) {
	routing := &HarnessRouting{
		Developer: " CODEX ",
		Reviewer:  " Claude-Code ",
		Heartbeat: " Agent-Zero ",
	}

	normalizeHarnessRouting(routing)

	if routing.Developer != "codex" {
		t.Fatalf("developer = %q", routing.Developer)
	}
	if routing.Reviewer != "claude-code" {
		t.Fatalf("reviewer = %q", routing.Reviewer)
	}
	if routing.Heartbeat != "agent-zero" {
		t.Fatalf("heartbeat = %q", routing.Heartbeat)
	}
}

func TestHarnessRoutingRejectsUnknownHarness(t *testing.T) {
	cfg := Default()
	cfg.Harness.Name = "claude-code"
	cfg.Harness.Routing = &HarnessRouting{
		Developer: "not-a-real-harness",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("unknown routed harness was accepted")
	}
}
