package harness

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// diagnosticRuntime builds one runtime topology with an explicit stderr
// capture per provider, so tests observe exactly which provider received
// start diagnostics.
func diagnosticRuntime(t *testing.T, providers map[ProviderID]HarnessConfig, roles map[Role]ProviderID, harnesses map[ProviderID]*changeHarness) (*Runtime, map[ProviderID]*bytes.Buffer) {
	t.Helper()
	registry := NewRegistry()
	stderr := make(map[ProviderID]*bytes.Buffer)
	for id, h := range harnesses {
		captured := &bytes.Buffer{}
		cfg := providers[id]
		cfg.Stderr = captured
		providers[id] = cfg
		stderr[id] = captured
		target := h
		registry.Register(string(id), func(HarnessConfig) (Harness, error) { return target, nil })
	}
	r, err := NewRuntimeSpec(registry, RuntimeSpec{Providers: providers, Roles: roles})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, stderr
}

// H7: a non-primary provider Start failure never fails Runtime.Start. The
// healthy primary starts, the failed provider stays explicitly unavailable,
// and exactly one bounded diagnostic naming the provider instance is written
// to that provider's already-configured stderr.
func TestRuntimeStartContinuesAfterNonPrimaryFailureWithOneDiagnostic(t *testing.T) {
	primary := newChangeHarness("agent-zero")
	failed := newChangeHarness("codex")
	failed.startErr = errors.New("codex binary not found")
	r, stderr := diagnosticRuntime(t,
		map[ProviderID]HarnessConfig{
			"agent-zero": {Name: "agent-zero", SessionsFile: "a.json"},
			"codex":      {Name: "codex", SessionsFile: "c.json"},
		},
		map[Role]ProviderID{RoleChat: "agent-zero", RoleDeveloper: "codex"},
		map[ProviderID]*changeHarness{"agent-zero": primary, "codex": failed})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Runtime.Start failed on a non-primary provider failure: %v", err)
	}
	if primary.startCalls.Load() != 1 {
		t.Fatal("healthy primary provider did not start")
	}
	diagnostic := stderr["codex"].String()
	if strings.Count(diagnostic, "\n") != 1 {
		t.Fatalf("non-primary start diagnostics = %d, want exactly one:\n%s", strings.Count(diagnostic, "\n"), diagnostic)
	}
	if !strings.Contains(diagnostic, `"codex"`) {
		t.Fatalf("diagnostic omitted the provider identity: %q", diagnostic)
	}
	if !strings.Contains(diagnostic, "codex binary not found") {
		t.Fatalf("diagnostic omitted the start failure: %q", diagnostic)
	}
	if stderr["agent-zero"].Len() != 0 {
		t.Fatalf("healthy provider received start diagnostics: %q", stderr["agent-zero"].String())
	}
	// The failed provider stays explicitly unavailable while the healthy
	// primary keeps serving.
	if _, _, err := ReserveExecution(r.AcquireRole(RoleDeveloper), "dev"); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("failed non-primary reservation = %v, want ErrProviderUnavailable", err)
	}
	if got := bindingSend(t, r.AcquireRole(RoleChat), "chat", "prompt"); got != "agent-zero-thread" {
		t.Fatal(got)
	}
	primary.finish("chat")
}

// H8: a failing primary/chat provider keeps its original failure semantics.
// Runtime.Start returns the original primary error unchanged and the
// non-primary diagnostic path adds no provider-start diagnostic for the
// primary.
func TestRuntimeStartPrimaryFailureReturnsOriginalErrorWithoutDiagnostic(t *testing.T) {
	primaryFailure := errors.New("chat provider offline")
	primary := newChangeHarness("agent-zero")
	primary.startErr = primaryFailure
	healthy := newChangeHarness("codex")
	r, stderr := diagnosticRuntime(t,
		map[ProviderID]HarnessConfig{
			"agent-zero": {Name: "agent-zero", SessionsFile: "a.json"},
			"codex":      {Name: "codex", SessionsFile: "c.json"},
		},
		map[Role]ProviderID{RoleChat: "agent-zero", RoleDeveloper: "codex"},
		map[ProviderID]*changeHarness{"agent-zero": primary, "codex": healthy})
	err := r.Start(context.Background())
	if err == nil {
		t.Fatal("Runtime.Start hid the primary chat failure")
	}
	if !errors.Is(err, primaryFailure) || err.Error() != primaryFailure.Error() {
		t.Fatalf("primary error changed: %v, want the original %v", err, primaryFailure)
	}
	if healthy.startCalls.Load() != 1 {
		t.Fatal("healthy non-primary provider did not start")
	}
	if stderr["agent-zero"].Len() != 0 {
		t.Fatalf("primary failure received a provider-start diagnostic: %q", stderr["agent-zero"].String())
	}
	if stderr["codex"].Len() != 0 {
		t.Fatalf("healthy non-primary provider received a provider-start diagnostic: %q", stderr["codex"].String())
	}
	if _, _, reserveErr := ReserveExecution(r.AcquireRole(RoleChat), "chat"); !errors.Is(reserveErr, ErrProviderUnavailable) {
		t.Fatalf("failed primary reservation = %v, want ErrProviderUnavailable", reserveErr)
	}
}

// The diagnostic is bounded: an oversized adapter error is truncated to the
// fixed start-diagnostic limit.
func TestProviderStartDiagnosticBoundsErrorText(t *testing.T) {
	stderr := &bytes.Buffer{}
	supervisor := NewSupervisor(NewRegistry(), HarnessConfig{Name: "codex", Stderr: stderr})
	entry := &providerEntry{id: "codex", supervisor: supervisor}
	writeProviderStartDiagnostic(entry, errors.New(strings.Repeat("x", providerStartDiagnosticLimit*4)))
	diagnostic := stderr.String()
	if strings.Count(diagnostic, "\n") != 1 {
		t.Fatalf("bounded diagnostics = %d, want exactly one", strings.Count(diagnostic, "\n"))
	}
	if runes := []rune(diagnostic); len(runes) > providerStartDiagnosticLimit+128 {
		t.Fatalf("bounded diagnostic length = %d runes, want at most the prefix plus the %d-rune bound", len(runes), providerStartDiagnosticLimit)
	}
	if !strings.HasSuffix(strings.TrimSpace(diagnostic), "…") {
		t.Fatalf("truncated diagnostic lost its omission marker: %q", diagnostic)
	}
}
