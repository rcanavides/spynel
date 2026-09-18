package config

import (
	"strings"
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

func TestProviderForRole(t *testing.T) {
	cfg := Default()
	cfg.Harness.Name = "codex"
	cfg.Harness.Providers = map[string]ProviderProfile{
		"claude-arch": {Harness: "claude-code"},
	}

	// Empty routes resolve the legacy primary harness reference.
	for _, role := range []providerharness.Role{providerharness.RoleChat, providerharness.RoleNotification} {
		ref := cfg.Harness.ProviderForRole(role)
		if ref.ID != "codex" || ref.Kind != "codex" || ref.Profile != nil {
			t.Fatalf("role %q empty route = %+v, want legacy codex kind", role, ref)
		}
	}

	// A legacy catalog kind route resolves an implicit kind reference.
	cfg.Harness.Routing = &HarnessRouting{Reviewer: "agent-zero"}
	if ref := cfg.Harness.ProviderForRole(providerharness.RoleReviewer); ref.ID != "agent-zero" || ref.Kind != "agent-zero" || ref.Profile != nil {
		t.Fatalf("legacy kind route = %+v", ref)
	}

	// A declared profile ID resolves its named instance. The harness is
	// constructed directly because production routing validation still
	// rejects profile IDs until profile composition lands.
	cfg.Harness.Routing = &HarnessRouting{Developer: "claude-arch"}
	ref := cfg.Harness.ProviderForRole(providerharness.RoleDeveloper)
	if ref.ID != "claude-arch" || ref.Kind != "claude-code" || ref.Profile == nil || ref.Profile.Harness != "claude-code" {
		t.Fatalf("profile route = %+v", ref)
	}

	// Production routing validation still accepts only catalog kinds.
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "harness.routing.developer is not a supported coding harness") {
		t.Fatalf("profile routing validation = %v", err)
	}
}
