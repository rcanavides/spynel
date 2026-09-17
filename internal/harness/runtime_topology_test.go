package harness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
)

func topologySpec() RuntimeSpec {
	return RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"agent-zero":  {Name: "agent-zero", SessionsFile: "a.json"},
		"codex":       {Name: "codex", SessionsFile: "c.json"},
		"claude-code": {Name: "claude-code", SessionsFile: "l.json"},
	}, Roles: map[Role]ProviderID{RoleChat: "agent-zero", RoleDeveloper: "codex", RoleReviewer: "claude-code"}}
}
func topologyFixture(t *testing.T, spec RuntimeSpec) (*Runtime, map[ProviderID]*changeHarness) {
	t.Helper()
	registry := NewRegistry()
	providers := make(map[ProviderID]*changeHarness)
	for id := range spec.Providers {
		h := newChangeHarness(string(id))
		providers[id] = h
		registry.Register(string(id), func(HarnessConfig) (Harness, error) { return h, nil })
	}
	r, err := NewRuntimeSpec(registry, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if err = r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r, providers
}
func TestRuntimeTopologyRolesDeduplicationAndClose(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			spec := topologySpec()
			if explicit {
				spec.Roles[RoleReviewer] = " CODEX "
				spec.Roles[RoleNotification] = "codex"
				spec.Roles[RoleHeartbeat] = "codex"
			}
			r, providers := topologyFixture(t, spec)
			for _, role := range []Role{RoleChat, RoleDeveloper, RoleReviewer, RoleNotification, RoleHeartbeat} {
				id := spec.Roles[role]
				if id == "" {
					id = "agent-zero"
				}
				if id == " CODEX " {
					id = "codex"
				}
				if r.AcquireRole(role).(*runtimeTarget).current() != r.providers[id].supervisor {
					t.Fatalf("role %s owner mismatch", role)
				}
			}
			if err := r.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			_ = r.Close()
			_ = r.Close()
			for id, p := range providers {
				if p.startCalls.Load() != 1 || p.closeCalls.Load() != 1 {
					t.Fatalf("%s starts=%d closes=%d", id, p.startCalls.Load(), p.closeCalls.Load())
				}
			}
		})
	}
}
func TestRuntimeSpecRejectsDuplicateOwnership(t *testing.T) {
	cases := []RuntimeSpec{
		{Providers: map[ProviderID]HarnessConfig{"codex": {Name: "codex"}, " CODEX ": {Name: "codex"}}, Roles: map[Role]ProviderID{RoleChat: "codex"}},
		{Providers: map[ProviderID]HarnessConfig{"codex": {Name: "codex", SessionsFile: "same"}, "agent-zero": {Name: "agent-zero", SessionsFile: "./same"}}, Roles: map[Role]ProviderID{RoleChat: "codex"}},
		{Providers: map[ProviderID]HarnessConfig{"codex": {Name: "codex"}}, Roles: map[Role]ProviderID{}},
		{Providers: map[ProviderID]HarnessConfig{"codex": {Name: "codex"}}, Roles: map[Role]ProviderID{RoleChat: "missing"}},
		{Providers: map[ProviderID]HarnessConfig{"codex": {Name: "codex"}, "acp": {Name: "acp"}}, Roles: map[Role]ProviderID{RoleChat: "codex", RoleDeveloper: "acp"}},
	}
	for i, spec := range cases {
		if r, err := NewRuntimeSpec(NewRegistry(), spec); err == nil {
			_ = r.Close()
			t.Fatalf("accepted invalid topology %d", i)
		}
	}
}
func TestRuntimeReconcileRemapRetirementAndStaleBinding(t *testing.T) {
	spec := topologySpec()
	r, p := topologyFixture(t, spec)
	target := r.AcquireRole(RoleDeveloper)
	old := r.providers["codex"].supervisor
	bindingSend(t, target, "K", "old")
	spec.Roles[RoleDeveloper] = "agent-zero"
	if err := r.Reconcile(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	id, release, err := ReserveExecution(target, "K")
	if err != nil || id != "codex" {
		t.Fatalf("bound reservation=%s %v", id, err)
	}
	release()
	if got := bindingSend(t, target, "new", "fresh"); got != "agent-zero-thread" {
		t.Fatal(got)
	}
	if r.providers["codex"].supervisor != old || p["codex"].startCalls.Load() != 1 {
		t.Fatal("unchanged provider replaced")
	}
	delete(spec.Providers, "codex")
	if err := r.Reconcile(context.Background(), spec); !errors.Is(err, ErrProviderFenced) {
		t.Fatalf("active retirement=%v", err)
	}
	if p["codex"].closeCalls.Load() != 0 {
		t.Fatal("active provider closed")
	}
	p["codex"].finish("K")
	// Keep the idle binding in the table deliberately; retirement must probe it.
	if bindingFor(r, "K") == nil {
		t.Fatal("fixture lost stale binding")
	}
	if err := r.Reconcile(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if p["codex"].closeCalls.Load() != 1 {
		t.Fatal("idle provider not closed exactly once")
	}
	// Simulate a delayed stale ownership record after retirement. Admission
	// must re-probe instead of resurrecting the closed supervisor.
	r.mu.Lock()
	r.bindings["K"] = &binding{provider: old}
	r.mu.Unlock()
	if got := bindingSend(t, target, "K", "reused"); got != "agent-zero-thread" {
		t.Fatal("retired owner reused: " + got)
	}
	p["agent-zero"].finish("K")
	p["agent-zero"].finish("new")
}
func TestRuntimeReconcileStructuralFenceAndUnrelatedSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		spec := topologySpec()
		r, p := topologyFixture(t, spec)
		old := r.providers["codex"].supervisor
		entered, release := make(chan struct{}), make(chan struct{})
		candidate := newChangeHarness("codex")
		candidate.onStart = func() { close(entered); <-release }
		r.registry.Register("codex", func(HarnessConfig) (Harness, error) { return candidate, nil })
		cfg := spec.Providers["codex"]
		cfg.Sandbox = "read-only"
		spec.Providers["codex"] = cfg
		_, observer1 := r.Readiness()
		_, observer2 := r.Readiness()
		done := make(chan error, 1)
		go func() { done <- r.Reconcile(context.Background(), spec) }()
		<-entered
		for _, ch := range []<-chan struct{}{observer1, observer2} {
			select {
			case <-ch:
			default:
				t.Fatal("observer missed fence")
			}
		}
		if _, _, err := ReserveExecution(r.AcquireRole(RoleDeveloper), "blocked"); !errors.Is(err, ErrProviderFenced) {
			t.Fatalf("reservation=%v", err)
		}
		if got := bindingSend(t, r.AcquireRole(RoleReviewer), "unrelated", "prompt"); got != "claude-code-thread" {
			t.Fatal(got)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if r.providers["codex"].supervisor != old {
			t.Fatal("supervisor identity changed")
		}
		if p["codex"].closeCalls.Load() != 1 || p["claude-code"].closeCalls.Load() != 0 {
			t.Fatal("incorrect structural retirement")
		}
		p["claude-code"].finish("unrelated")
	})
}
func TestRuntimeChatReservationWaitsThroughStructuralFence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		spec := topologySpec()
		r, _ := topologyFixture(t, spec)
		entered, release := make(chan struct{}), make(chan struct{})
		candidate := newChangeHarness("codex")
		candidate.onStart = func() { close(entered); <-release }
		r.registry.Register("codex", func(HarnessConfig) (Harness, error) { return candidate, nil })
		cfg := spec.Providers["codex"]
		cfg.Sandbox = "read-only"
		spec.Providers["codex"] = cfg
		done := make(chan error, 1)
		go func() { done <- r.Reconcile(context.Background(), spec) }()
		<-entered
		// Orchestrator reservations stay fail-fast through the structural fence.
		if _, _, err := ReserveExecution(r.AcquireRole(RoleDeveloper), "fast"); !errors.Is(err, ErrProviderFenced) {
			t.Fatalf("fail-fast reservation=%v", err)
		}
		type chatReservation struct {
			id      ProviderID
			release func()
			err     error
		}
		waited := make(chan chatReservation, 1)
		go func() {
			id, release, err := ReserveExecutionWait(context.Background(), r.AcquireRole(RoleDeveloper), "chat")
			waited <- chatReservation{id: id, release: release, err: err}
		}()
		synctest.Wait()
		select {
		case <-waited:
			t.Fatal("chat reservation did not wait through the structural fence")
		default:
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancelled := make(chan error, 1)
		go func() {
			_, _, err := ReserveExecutionWait(ctx, r.AcquireRole(RoleDeveloper), "chat-cancel")
			cancelled <- err
		}()
		synctest.Wait()
		select {
		case <-cancelled:
			t.Fatal("cancellation observed before the context was done")
		default:
		}
		cancel()
		if err := awaitChange(t, cancelled); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled wait=%v", err)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		got := awaitChange(t, waited)
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.id != "codex" {
			t.Fatalf("waited provider=%s", got.id)
		}
		got.release()
	})
}
func TestRuntimeStructuralChangeActiveRejectsAllPublication(t *testing.T) {
	spec := topologySpec()
	r, p := topologyFixture(t, spec)
	bindingSend(t, r.AcquireRole(RoleDeveloper), "active", "prompt")
	for id, cfg := range spec.Providers {
		cfg.Sandbox = "read-only"
		spec.Providers[id] = cfg
	}
	spec.Roles[RoleReviewer] = "agent-zero"
	if err := r.Reconcile(context.Background(), spec); err == nil {
		t.Fatal("active change accepted")
	}
	if r.AcquireRole(RoleReviewer).(*runtimeTarget).current() != r.providers["claude-code"].supervisor {
		t.Fatal("partial role publication")
	}
	for id, h := range p {
		if h.closeCalls.Load() != 0 || r.providers[id].supervisor.HarnessConfig().Sandbox != "" {
			t.Fatal("partial provider publication")
		}
	}
	p["codex"].finish("active")
}
func TestRuntimePartialStartupAdditionAndRecovery(t *testing.T) {
	for _, initial := range []bool{false, true} {
		t.Run(fmt.Sprint(initial), func(t *testing.T) {
			spec := topologySpec()
			registry := NewRegistry()
			a, c := newChangeHarness("agent-zero"), newChangeHarness("claude-code")
			registry.Register("agent-zero", func(HarnessConfig) (Harness, error) { return a, nil })
			registry.Register("claude-code", func(HarnessConfig) (Harness, error) { return c, nil })
			failed := newChangeHarness("codex")
			failed.startErr = errors.New("not installed")
			registry.Register("codex", func(HarnessConfig) (Harness, error) { return failed, nil })
			if !initial {
				delete(spec.Providers, "codex")
				delete(spec.Roles, RoleDeveloper)
			}
			r, err := NewRuntimeSpec(registry, spec)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if err = r.Start(context.Background()); err != nil {
				t.Fatal("routed failure killed runtime: ", err)
			}
			spec = topologySpec()
			if !initial {
				if err = r.Reconcile(context.Background(), spec); err != nil {
					t.Fatal(err)
				}
			}
			owner := r.providers["codex"].supervisor
			if _, _, err := ReserveExecution(r.AcquireRole(RoleDeveloper), "unavailable"); !errors.Is(err, ErrProviderUnavailable) {
				t.Fatalf("unavailable role=%v", err)
			}
			bindingSend(t, r.AcquireRole(RoleChat), "chat", "prompt")
			bindingSend(t, r.AcquireRole(RoleReviewer), "review", "prompt")
			recovered := newChangeHarness("codex")
			registry.Register("codex", func(HarnessConfig) (Harness, error) { return recovered, nil })
			_, one := r.Readiness()
			_, two := r.Readiness()
			if err = r.Reconcile(context.Background(), spec); err != nil {
				t.Fatal(err)
			}
			if r.providers["codex"].supervisor != owner {
				t.Fatal("failed supervisor replaced")
			}
			for _, ch := range []<-chan struct{}{one, two} {
				select {
				case <-ch:
				default:
					t.Fatal("observer missed recovery")
				}
			}
			if got := bindingSend(t, r.AcquireRole(RoleDeveloper), "dev", "prompt"); got != "codex-thread" {
				t.Fatal(got)
			}
			if a.startCalls.Load() != 1 || c.startCalls.Load() != 1 || a.closeCalls.Load() != 0 || c.closeCalls.Load() != 0 {
				t.Fatal("healthy provider restarted")
			}
			a.finish("chat")
			c.finish("review")
			recovered.finish("dev")
		})
	}
}
func TestRuntimeConcurrentTopologyLookupAndLifecycle(t *testing.T) {
	spec := topologySpec()
	r, p := topologyFixture(t, spec)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if i == 0 {
					next := topologySpec()
					if j%2 == 0 {
						next.Roles[RoleDeveloper] = "claude-code"
					}
					if err := r.Reconcile(context.Background(), next); err != nil {
						t.Error(err)
					}
				} else {
					id := r.AcquireRole(RoleDeveloper).(*runtimeTarget).HarnessConfig().Name
					if id != "codex" && id != "claude-code" {
						t.Error(id)
					}
				}
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_ = r.Start(context.Background())
			} else {
				_ = r.Close()
			}
		}(i)
	}
	wg.Wait()
	for id, h := range p {
		if h.startCalls.Load() != 1 || h.closeCalls.Load() != 1 {
			t.Fatalf("%s incorrect lifecycle", id)
		}
	}
}

func TestRuntimeAddProviderStartsBeforeRolePublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		spec := RuntimeSpec{Providers: map[ProviderID]HarnessConfig{"agent-zero": {Name: "agent-zero"}}, Roles: map[Role]ProviderID{RoleChat: "agent-zero"}}
		r, p := topologyFixture(t, spec)
		added := newChangeHarness("codex")
		entered, release := make(chan struct{}), make(chan struct{})
		added.onStart = func() { close(entered); <-release }
		r.registry.Register("codex", func(HarnessConfig) (Harness, error) { return added, nil })
		spec.Providers["codex"] = HarnessConfig{Name: "codex"}
		spec.Roles[RoleDeveloper] = "codex"
		done := make(chan error, 1)
		go func() { done <- r.Reconcile(context.Background(), spec) }()
		<-entered
		if got := r.AcquireRole(RoleDeveloper).(*runtimeTarget).HarnessConfig().Name; got != "agent-zero" {
			t.Fatal("published before start: ", got)
		}
		bindingSend(t, r.AcquireRole(RoleChat), "unrelated", "prompt")
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if got := bindingSend(t, r.AcquireRole(RoleDeveloper), "added", "prompt"); got != "codex-thread" {
			t.Fatal(got)
		}
		if p["agent-zero"].startCalls.Load() != 1 || p["agent-zero"].closeCalls.Load() != 0 {
			t.Fatal("primary restarted")
		}
		added.finish("added")
		p["agent-zero"].finish("unrelated")
	})
}
func TestRuntimeReservationAndInferencePreventRetirement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		spec := topologySpec()
		r, p := topologyFixture(t, spec)
		target := r.AcquireRole(RoleDeveloper)
		_, release, err := ReserveExecution(target, "pending")
		if err != nil {
			t.Fatal(err)
		}
		delete(spec.Providers, "codex")
		spec.Roles[RoleDeveloper] = "agent-zero"
		if err = r.Reconcile(context.Background(), spec); !errors.Is(err, ErrProviderFenced) {
			t.Fatalf("pending admission retirement=%v", err)
		}
		release()
		entered, settle := make(chan struct{}), make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- target.(*runtimeTarget).CommitModel("next", func() error { close(entered); <-settle; return nil })
		}()
		<-entered
		if err = r.Reconcile(context.Background(), spec); !errors.Is(err, ErrProviderFenced) {
			t.Fatalf("inference retirement=%v", err)
		}
		if p["codex"].closeCalls.Load() != 0 {
			t.Fatal("closed during persistence")
		}
		close(settle)
		if err = <-done; err != nil {
			t.Fatal(err)
		}
	})
}
