package harness

import (
	"context"
	"errors"
	"testing"

	"github.com/agent0ai/spynel/internal/core"
)

func ownedRuntimeSpec() RuntimeSpec {
	return RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"codex-dev": {Name: "routed", SessionsFile: "owned-codex.json"},
		"glm-dev":   {Name: "routed", SessionsFile: "owned-glm.json"},
	}, Roles: map[Role]ProviderID{RoleChat: "glm-dev", RoleDeveloper: "glm-dev"}}
}

func ownedRuntimeFixture(t *testing.T) (*instanceFixtures, *changeHarness, *changeHarness) {
	t.Helper()
	f := newInstanceFixtures(t, ownedRuntimeSpec())
	f.start(t)
	codex := instanceFixturesNil(t, f.harness("owned-codex.json", 0), "codex-dev")
	glm := instanceFixturesNil(t, f.harness("owned-glm.json", 0), "glm-dev")
	return f, codex, glm
}

func TestOwnedReservationSelectsExactProviderInstance(t *testing.T) {
	f, codex, glm := ownedRuntimeFixture(t)
	target := f.r.AcquireRole(RoleDeveloper)
	release, err := ReserveOwnedExecution(target, "owned", "codex-dev")
	if err != nil {
		t.Fatal(err)
	}
	if got := ExecutionProvider(target, "owned"); got != "codex-dev" {
		t.Fatalf("ExecutionProvider=%q", got)
	}
	if _, _, err = target.Send(context.Background(), "owned", "exact owner", nil); err != nil {
		t.Fatal(err)
	}
	if got := bindingPrompts(codex, "owned"); len(got) != 1 || got[0] != "exact owner" {
		t.Fatalf("codex prompts=%q", got)
	}
	if got := bindingPrompts(glm, "owned"); len(got) != 0 {
		t.Fatalf("role-mapped provider received owned work: %q", got)
	}
	release()
	codex.finish("owned")
}

func TestOwnedReservationSelectsExactSameKindInstance(t *testing.T) {
	f := newInstanceFixtures(t, instanceSpec())
	f.start(t)
	target := f.r.AcquireRole(RoleReviewer)
	release, err := ReserveOwnedExecution(target, "same-kind", "claude-arch")
	if err != nil {
		t.Fatal(err)
	}
	b := bindingFor(f.r, "same-kind")
	if b == nil || b.id != "claude-arch" || b.provider != f.r.providers["claude-arch"].supervisor {
		t.Fatalf("binding=%+v", b)
	}
	if b.id == ProviderID(b.provider.HarnessConfig().Name) || b.id == "claude-rev" {
		t.Fatalf("owned identity collapsed to %q", b.id)
	}
	release()
}

func TestOwnedReservationOwnerAbsent(t *testing.T) {
	f, codex, glm := ownedRuntimeFixture(t)
	target := f.r.AcquireRole(RoleDeveloper)
	if release, err := ReserveOwnedExecution(target, "absent", "deleted-owner"); release != nil || !errors.Is(err, ErrProviderAbsent) {
		t.Fatalf("reservation release=%v err=%v", release != nil, err)
	} else if errors.Is(err, ErrProviderUnavailable) || errors.Is(err, ErrProviderFenced) {
		t.Fatalf("absence matched another provider error: %v", err)
	}
	if bindingFor(f.r, "absent") != nil {
		t.Fatal("absent owner left a binding")
	}
	if len(bindingPrompts(codex, "absent")) != 0 || len(bindingPrompts(glm, "absent")) != 0 {
		t.Fatal("absent owner fell back to another provider")
	}
}

func TestOwnedReservationOwnerUnavailable(t *testing.T) {
	f := newInstanceFixtures(t, ownedRuntimeSpec())
	f.failNextStart("owned-codex.json")
	f.start(t)
	glm := instanceFixturesNil(t, f.harness("owned-glm.json", 0), "glm-dev")
	target := f.r.AcquireRole(RoleDeveloper)
	if release, err := ReserveOwnedExecution(target, "unavailable", "codex-dev"); release != nil || !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("reservation release=%v err=%v", release != nil, err)
	} else if errors.Is(err, ErrProviderAbsent) || errors.Is(err, ErrProviderFenced) {
		t.Fatalf("unavailable owner matched wrong error: %v", err)
	}
	if bindingFor(f.r, "unavailable") != nil {
		t.Fatal("unavailable owner leaked a binding")
	}
	if len(bindingPrompts(glm, "unavailable")) != 0 {
		t.Fatal("unavailable owner fell back through the role")
	}
}

func TestOwnedReservationOwnerFenced(t *testing.T) {
	f, codex, glm := ownedRuntimeFixture(t)
	entered := make(chan struct{})
	proceed := make(chan struct{})
	candidate := newChangeHarness("routed")
	candidate.onStart = func() {
		close(entered)
		<-proceed
	}
	f.r.registry.Register("routed", func(HarnessConfig) (Harness, error) { return candidate, nil })
	next := ownedRuntimeSpec()
	cfg := next.Providers["codex-dev"]
	cfg.Model = "replacement"
	next.Providers["codex-dev"] = cfg
	done := make(chan error, 1)
	go func() { done <- f.r.Reconcile(context.Background(), next) }()
	<-entered
	release, reserveErr := ReserveOwnedExecution(f.r.AcquireRole(RoleDeveloper), "fenced", "codex-dev")
	binding := bindingFor(f.r, "fenced")
	close(proceed)
	reconcileErr := <-done
	if reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	if release != nil || !errors.Is(reserveErr, ErrProviderFenced) {
		t.Fatalf("reservation release=%v err=%v", release != nil, reserveErr)
	}
	if errors.Is(reserveErr, ErrProviderAbsent) || errors.Is(reserveErr, ErrProviderUnavailable) {
		t.Fatalf("fenced owner matched wrong error: %v", reserveErr)
	}
	if binding != nil {
		t.Fatalf("fenced owner left binding=%+v", binding)
	}
	if len(bindingPrompts(codex, "fenced")) != 0 || len(bindingPrompts(glm, "fenced")) != 0 {
		t.Fatal("fenced owner switched providers")
	}
}

func TestOwnedReservationRuntimeClosed(t *testing.T) {
	f, _, _ := ownedRuntimeFixture(t)
	if err := f.r.Close(); err != nil {
		t.Fatal(err)
	}
	release, err := ReserveOwnedExecution(f.r.AcquireRole(RoleDeveloper), "closed", "codex-dev")
	if release != nil || !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("reservation release=%v err=%v", release != nil, err)
	}
	if errors.Is(err, ErrProviderAbsent) {
		t.Fatalf("closed runtime reported owner absence: %v", err)
	}
}

func TestOwnedReservationReusesSameOwnerBinding(t *testing.T) {
	f, _, _ := ownedRuntimeFixture(t)
	target := f.r.AcquireRole(RoleDeveloper)
	firstRelease, err := ReserveOwnedExecution(target, "shared", "codex-dev")
	if err != nil {
		t.Fatal(err)
	}
	first := bindingFor(f.r, "shared")
	secondRelease, err := ReserveOwnedExecution(target, "shared", "codex-dev")
	if err != nil {
		firstRelease()
		t.Fatal(err)
	}
	if second := bindingFor(f.r, "shared"); second != first || second.admitting != 2 {
		t.Fatalf("binding replaced or miscounted: first=%p second=%p admitting=%d", first, second, second.admitting)
	}
	secondRelease()
	if got := bindingFor(f.r, "shared"); got != first || got.admitting != 1 {
		t.Fatalf("first reservation lost after second release: %+v", got)
	}
	firstRelease()
	if bindingFor(f.r, "shared") != nil {
		t.Fatal("inactive binding remained after all releases")
	}
}

func TestOwnedReservationRejectsDifferentOwnerBinding(t *testing.T) {
	f, codex, glm := ownedRuntimeFixture(t)
	target := f.r.AcquireRole(RoleDeveloper)
	release, err := ReserveOwnedExecution(target, "conflict", "codex-dev")
	if err != nil {
		t.Fatal(err)
	}
	original := bindingFor(f.r, "conflict")
	otherRelease, otherErr := ReserveOwnedExecution(target, "conflict", "glm-dev")
	if otherRelease != nil || !errors.Is(otherErr, ErrProviderFenced) {
		release()
		t.Fatalf("conflicting reservation release=%v err=%v", otherRelease != nil, otherErr)
	}
	if got := bindingFor(f.r, "conflict"); got != original || got.id != "codex-dev" || got.admitting != 1 {
		release()
		t.Fatalf("original binding changed: %+v", got)
	}
	if _, _, err = target.Send(context.Background(), "conflict", "still original", nil); err != nil {
		release()
		t.Fatal(err)
	}
	if got := bindingPrompts(codex, "conflict"); len(got) != 1 || got[0] != "still original" {
		t.Fatalf("original owner prompts=%q", got)
	}
	if got := bindingPrompts(glm, "conflict"); len(got) != 0 {
		t.Fatalf("conflicting owner received work: %q", got)
	}
	release()
	codex.finish("conflict")
	f.r.releaseBinding("conflict")
}

type ownedFallbackTarget struct {
	id           ProviderID
	reserveErr   error
	reservations int
	releases     int
}

func (t *ownedFallbackTarget) ReserveExecution(string) (ProviderID, func(), error) {
	t.reservations++
	if t.reserveErr != nil {
		return "", nil, t.reserveErr
	}
	return t.id, func() { t.releases++ }, nil
}
func (*ownedFallbackTarget) Send(context.Context, string, string, core.Emit) (string, bool, error) {
	return "", false, nil
}
func (*ownedFallbackTarget) Interrupt(context.Context, string) (bool, error) { return false, nil }
func (*ownedFallbackTarget) ResetSession(string) error                       { return nil }
func (*ownedFallbackTarget) ThreadID(string) string                          { return "" }
func (*ownedFallbackTarget) IsActive(string) bool                            { return false }

func TestReserveOwnedExecutionNonRuntimeFallback(t *testing.T) {
	t.Run("mismatch", func(t *testing.T) {
		target := &ownedFallbackTarget{id: "other"}
		release, err := ReserveOwnedExecution(target, "key", "owner")
		if release != nil || !errors.Is(err, ErrProviderAbsent) || target.reservations != 1 || target.releases != 1 {
			t.Fatalf("release=%v err=%v reservations=%d releases=%d", release != nil, err, target.reservations, target.releases)
		}
	})
	t.Run("empty returned identity", func(t *testing.T) {
		target := &ownedFallbackTarget{}
		if release, err := ReserveOwnedExecution(target, "key", "owner"); release != nil || !errors.Is(err, ErrProviderAbsent) || target.releases != 1 {
			t.Fatalf("release=%v err=%v releases=%d", release != nil, err, target.releases)
		}
	})
	t.Run("match", func(t *testing.T) {
		target := &ownedFallbackTarget{id: "owner"}
		release, err := ReserveOwnedExecution(target, "key", "owner")
		if err != nil || release == nil || target.releases != 0 {
			t.Fatalf("release=%v err=%v releases=%d", release != nil, err, target.releases)
		}
		release()
		if target.releases != 1 {
			t.Fatalf("releases=%d", target.releases)
		}
	})
	t.Run("empty owner", func(t *testing.T) {
		target := &ownedFallbackTarget{id: "owner"}
		if release, err := ReserveOwnedExecution(target, "key", ""); release != nil || !errors.Is(err, ErrProviderAbsent) || target.reservations != 0 {
			t.Fatalf("release=%v err=%v reservations=%d", release != nil, err, target.reservations)
		}
	})
	t.Run("reservation error", func(t *testing.T) {
		cause := errors.New("reservation failed")
		target := &ownedFallbackTarget{reserveErr: cause}
		if release, err := ReserveOwnedExecution(target, "key", "owner"); release != nil || !errors.Is(err, cause) || target.releases != 0 {
			t.Fatalf("release=%v err=%v releases=%d", release != nil, err, target.releases)
		}
	})
}

func TestProviderAbsentSentinelSeparation(t *testing.T) {
	for _, other := range []error{ErrProviderUnavailable, ErrProviderFenced} {
		if errors.Is(ErrProviderAbsent, other) || errors.Is(other, ErrProviderAbsent) {
			t.Fatalf("ErrProviderAbsent overlaps %v", other)
		}
	}
}

func TestReserveExecutionRemainsRoleBased(t *testing.T) {
	f, _, _ := ownedRuntimeFixture(t)
	id, release, err := ReserveExecution(f.r.AcquireRole(RoleDeveloper), "new-work")
	if err != nil {
		t.Fatal(err)
	}
	if id != "glm-dev" {
		t.Fatalf("role-based provider=%q", id)
	}
	release()
}
