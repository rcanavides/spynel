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

	// A declared profile ID resolves its named instance.
	cfg.Harness.Routing = &HarnessRouting{Developer: "claude-arch"}
	ref := cfg.Harness.ProviderForRole(providerharness.RoleDeveloper)
	if ref.ID != "claude-arch" || ref.Kind != "claude-code" || ref.Profile == nil || ref.Profile.Harness != "claude-code" {
		t.Fatalf("profile route = %+v", ref)
	}

	// Profile routing is now valid: the profile's harness kind satisfies the
	// same catalog restriction as a legacy kind route.
	if err := cfg.Validate(); err != nil {
		t.Fatalf("profile routing rejected: %v", err)
	}
}

func TestHarnessRoutingAcceptsProviderProfiles(t *testing.T) {
	cfg := Default()
	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Providers = map[string]ProviderProfile{
		"codex-dev":  {Harness: "codex", ReasoningEffort: "high"},
		"acp-runner": {Harness: "acp", ACPCommand: "own-agent", ACPArgs: []string{"--stdio"}},
	}
	cfg.Harness.Routing = &HarnessRouting{
		Developer: "codex-dev",
		Reviewer:  "codex-dev",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("provider profile routing rejected: %v", err)
	}

	// A named ACP profile carries its own command: the legacy global
	// harness.acp_command requirement must not fire for profile routes.
	cfg.Harness.Routing = &HarnessRouting{Notification: "acp-runner"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("named ACP profile routing rejected without global acp_command: %v", err)
	}

	// A profile whose harness kind fails the same catalog restriction keeps
	// a role-attributed error even though profile validation flags it too.
	cfg.Harness.Providers["broken-dev"] = ProviderProfile{Harness: "not-a-harness"}
	cfg.Harness.Routing = &HarnessRouting{Developer: "broken-dev"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "harness.routing.developer selects provider profile broken-dev whose harness is not a supported coding harness") {
		t.Fatalf("invalid profile kind route = %v", err)
	}

	// Values that are neither a declared profile ID nor a catalog kind fail.
	cfg.Harness.Routing = &HarnessRouting{Reviewer: "ghost-dev"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "harness.routing.reviewer is not a supported coding harness") {
		t.Fatalf("unknown route = %v", err)
	}
}
