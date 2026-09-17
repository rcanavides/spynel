package harness

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/agent0ai/spynel/internal/core"
)

// Two providers and remapping exist only in these tests, not in production
// construction or an exported topology API. The fixture owns B's lifecycle.
func bindingRuntime(t *testing.T, a Harness) (*Runtime, *Supervisor, *changeHarness) {
	t.Helper()
	registry := NewRegistry()
	registry.Register("a", func(HarnessConfig) (Harness, error) { return a, nil })
	b := newChangeHarness("b")
	registry.Register("b", func(HarnessConfig) (Harness, error) { return b, nil })
	r := NewRuntime(registry, HarnessConfig{Name: "a"})
	bs := NewSupervisor(registry, HarnessConfig{Name: "b"})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := bs.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close(); _ = bs.Close() })
	return r, bs, b
}
func remapBindingRole(r *Runtime, role Role, s supervisorOperations) {
	r.mu.Lock()
	r.roles[role] = s
	r.mu.Unlock()
}
func bindingCount(r *Runtime) int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.bindings) }
func bindingFor(r *Runtime, key string) *binding {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bindings[key]
}
func bindingPrompts(h *changeHarness, key string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.prompts[key]...)
}
func bindingSend(t *testing.T, target ExecutionTarget, key, prompt string) string {
	t.Helper()
	thread, _, err := target.Send(context.Background(), key, prompt, nil)
	if err != nil {
		t.Fatal(err)
	}
	return thread
}

type blockedBindingHarness struct {
	*changeHarness
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (h *blockedBindingHarness) SendWithModel(ctx context.Context, key, prompt, model string, emit core.Emit) (string, bool, error) {
	thread, steered, err := h.supervisorHarness.SendWithModel(ctx, key, prompt, model, emit)
	if h.calls.Add(1) == 1 {
		close(h.entered)
		<-h.release
	}
	return thread, steered, err
}

func TestRuntimeBindingPrecedesSendAndConcurrentRemap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := &blockedBindingHarness{changeHarness: newChangeHarness("a"), entered: make(chan struct{}), release: make(chan struct{})}
		a.interruptErr = errors.New("A interrupt reached")
		r, bs, b := bindingRuntime(t, a)
		target := r.AcquireRole(RoleDeveloper)
		sent := make(chan error, 1)
		go func() { _, _, err := target.Send(context.Background(), "k", "first", nil); sent <- err }()
		awaitChange(t, a.entered)
		owner := bindingFor(r, "k")
		if owner == nil || owner.provider != r.supervisor || owner.admitting != 1 {
			t.Fatal("binding absent before Send return")
		}
		remapBindingRole(r, RoleDeveloper, bs)
		if !target.IsActive("k") {
			t.Fatal("active owner lost during admission")
		}
		// Interrupt queues behind the provider Send; it must still target A.
		interrupted := make(chan error, 1)
		go func() { _, err := target.Interrupt(context.Background(), "k"); interrupted <- err }()
		followed := make(chan error, 1)
		go func() { _, _, err := target.Send(context.Background(), "k", "second", nil); followed <- err }()
		if len(bindingPrompts(b, "k")) != 0 {
			t.Fatal("B admitted bound key")
		}
		close(a.release)
		if err := awaitChange(t, sent); err != nil {
			t.Fatal(err)
		}
		if err := awaitChange(t, interrupted); !errors.Is(err, a.interruptErr) {
			t.Fatalf("interrupt=%v", err)
		}
		if err := awaitChange(t, followed); err != nil {
			t.Fatal(err)
		}
		if !target.IsActive("k") || bindingFor(r, "k").provider != r.supervisor {
			t.Fatal("owner lost after Send returned")
		}
		// Interrupt may cancel the queued follow-up; that does not release A's turn.
		a.finish("k")
	})
}

func TestRuntimeBindingFollowupsControlsAndRelease(t *testing.T) {
	for _, role := range []Role{RoleDeveloper, RoleHeartbeat} {
		t.Run(string(role), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a := newChangeHarness("a")
				r, bs, b := bindingRuntime(t, a)
				target := r.AcquireRole(role).(*runtimeTarget)
				bindingSend(t, target, "k", "first")
				remapBindingRole(r, role, bs)
				if !target.IsActive("k") || target.ThreadID("k") != "a-thread" || target.ConversationAdmission("k") != "queued" {
					t.Fatal("key operations lost owner")
				}
				if target.HarnessConfig().Name != "b" || target.HasActiveTurns() {
					t.Fatal("non-key capability did not use current role")
				}
				bindingSend(t, target, "k", "follow-up")
				if _, _, err := target.SendConversation(context.Background(), "k", "conversation", "message", nil); err != nil {
					t.Fatal(err)
				}
				control := ControlRequest{ID: "c", Prompt: "guidance", ContinuationPrompt: "continue", Validate: func() bool { return true }}
				if result, err := target.SendControl(context.Background(), "k", control); err != nil || !result.Queued {
					t.Fatalf("control=%+v %v", result, err)
				}
				if err := target.ResetSession("k"); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(a.resetKeys, []string{"k"}) || len(b.resetKeys) != 0 {
					t.Fatal("reset did not reach owner")
				}
				// Drain queued turns/continuation deterministically. Every intermediate
				// logical execution must remain on A, even between provider turns.
				for i := 0; i < 8 && r.supervisor.IsActive("k"); i++ {
					if bindingFor(r, "k") == nil || !target.IsActive("k") {
						t.Fatal("queued execution lost binding")
					}
					a.finish("k")
					synctest.Wait()
				}
				if r.supervisor.IsActive("k") {
					t.Fatal("queue failed to settle")
				}
				if got := bindingPrompts(a, "k"); !containsBindingPrompt(got, "guidance") || !containsBindingPrompt(got, "continue") {
					t.Fatalf("control/continuation missing: %q", got)
				}
				if len(bindingPrompts(b, "k")) != 0 {
					t.Fatal("B received A's follow-ups")
				}
				if target.IsActive("k") || bindingFor(r, "k") != nil {
					t.Fatal("inactive binding retained")
				}
				if got := bindingSend(t, target, "k", "next run"); got != "b-thread" {
					t.Fatalf("next=%s", got)
				}
				b.finish("k")
			})
		})
	}
}
func containsBindingPrompt(prompts []string, want string) bool {
	for _, p := range prompts {
		if p == want {
			return true
		}
	}
	return false
}

func TestRuntimeBindingSynchronousCompletionAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			a := &returningSupervisorHarness{changeHarness: newChangeHarness("a"), terminal: make(chan struct{}), release: make(chan struct{})}
			close(a.release)
			if fail {
				a.sendErr = errors.New("send failed")
			}
			r, bs, b := bindingRuntime(t, a)
			target := r.AcquireRole(RoleDeveloper)
			_, _, err := target.Send(context.Background(), "k", "first", nil)
			if (err != nil) != fail {
				t.Fatalf("Send error=%v", err)
			}
			if bindingCount(r) != 0 {
				t.Fatal("settled admission retained binding")
			}
			remapBindingRole(r, RoleDeveloper, bs)
			if got := bindingSend(t, target, "k", "next"); got != "b-thread" {
				t.Fatalf("next=%s", got)
			}
			b.finish("k")
		})
	}
}

func TestRuntimeBindingSurvivesStructuralFenceWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := newChangeHarness("a")
		r, bs, b := bindingRuntime(t, a)
		next := newChangeHarness("a")
		r.supervisor.registry.Register("a", func(HarnessConfig) (Harness, error) { return next, nil })
		change, err := r.supervisor.PrepareChange(context.Background(), HarnessConfig{Name: "a"})
		if err != nil {
			t.Fatal(err)
		}
		defer change.Abort()
		target := r.AcquireRole(RoleDeveloper)
		sent := make(chan string, 1)
		go func() {
			thread, _, err := target.Send(context.Background(), "k", "prompt", nil)
			if err != nil {
				t.Error(err)
			}
			sent <- thread
		}()
		synctest.Wait()
		if bindingFor(r, "k") == nil || len(sent) != 0 {
			t.Fatal("fenced Send did not retain admission binding")
		}
		remapBindingRole(r, RoleDeveloper, bs)
		if target.IsActive("k") {
			t.Fatal("fenced pending admission must not invent activity")
		}
		if bindingFor(r, "k") == nil {
			t.Fatal("inactive probe erased pending admission")
		}
		previous := change.Commit()
		_ = previous.Close()
		if got := awaitChange(t, sent); got != "a-thread" {
			t.Fatalf("fenced send=%s", got)
		}
		if len(bindingPrompts(b, "k")) != 0 {
			t.Fatal("fenced send migrated")
		}
		next.finish("k")
	})
}

// Instrument only the test's operational boundary, delegating the activity
// decision to the real Supervisor before pausing one stale idle probe.
type idleProbeGate struct{ entered, release chan struct{} }
type pausedIdleProbe struct {
	supervisorOperations
	gates chan idleProbeGate
}

func (p *pausedIdleProbe) IsActive(key string) bool {
	active := p.supervisorOperations.IsActive(key)
	select {
	case gate := <-p.gates:
		close(gate.entered)
		<-gate.release
	default:
	}
	return active
}
func TestRuntimeBindingReleaseCannotEraseReadmittedOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := newChangeHarness("a")
		r, bs, b := bindingRuntime(t, a)
		probe := &pausedIdleProbe{supervisorOperations: r.supervisor, gates: make(chan idleProbeGate, 1)}
		remapBindingRole(r, RoleDeveloper, probe)
		target := r.AcquireRole(RoleDeveloper)
		bindingSend(t, target, "k", "first")
		stale := bindingFor(r, "k")
		// Admission's sweep observes active, but has not yet incremented admitting.
		activeGate := idleProbeGate{make(chan struct{}), make(chan struct{})}
		probe.gates <- activeGate
		sent := make(chan error, 1)
		go func() { _, _, err := target.Send(context.Background(), "k", "readmitted", nil); sent <- err }()
		awaitChange(t, activeGate.entered)
		a.finish("k")
		// A concurrent release now observes inactive. Keep that stale result pending
		// until the new admission has both reused the owner and returned active.
		idleGate := idleProbeGate{make(chan struct{}), make(chan struct{})}
		probe.gates <- idleGate
		released := make(chan struct{})
		go func() { r.releaseBinding("k"); close(released) }()
		awaitChange(t, idleGate.entered)
		close(activeGate.release)
		if err := awaitChange(t, sent); err != nil {
			t.Fatal(err)
		}
		replacement := bindingFor(r, "k")
		if replacement == stale || replacement == nil {
			t.Fatal("admission reused stale binding pointer")
		}
		remapBindingRole(r, RoleDeveloper, bs)
		close(idleGate.release)
		awaitChange(t, released)
		if bindingFor(r, "k") != replacement || !target.IsActive("k") {
			t.Fatal("stale idle probe erased new admission")
		}
		if len(bindingPrompts(b, "k")) != 0 {
			t.Fatal("readmission migrated")
		}
		a.finish("k")
	})
}

func TestRuntimeBindingIdleCompatibilitySweepAndClose(t *testing.T) {
	a := newChangeHarness("a")
	r, _, _ := bindingRuntime(t, a)
	target := r.AcquireRole(RoleChat).(*runtimeTarget)
	if stopped, err := target.Interrupt(context.Background(), "idle"); stopped || err != nil {
		t.Fatalf("idle interrupt=%t %v", stopped, err)
	}
	if target.ThreadID("idle") != r.supervisor.ThreadID("idle") {
		t.Fatal("idle thread differs")
	}
	if err := target.ResetSession("idle"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.resetKeys, []string{"idle"}) {
		t.Fatal("reset did not reach current provider")
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprint(i)
		bindingSend(t, target, key, "prompt")
		a.finish(key)
		if bindingCount(r) > 1 {
			t.Fatal("completed unique bindings accumulate")
		}
	}
	bindingSend(t, target, "live", "prompt")
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if bindingCount(r) != 0 {
		t.Fatal("Close retained bindings")
	}
	if _, _, err := target.Send(context.Background(), "live", "late", nil); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("closed send=%v", err)
	}
	if _, _, err := target.SendConversation(context.Background(), "live", "late", "late", nil); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("closed conversation=%v", err)
	}
	if _, err := target.Interrupt(context.Background(), "live"); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("closed interrupt=%v", err)
	}
	if _, err := target.SendControl(context.Background(), "live", ControlRequest{ID: "c", Prompt: "late"}); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("closed control=%v", err)
	}
	if err := target.ResetSession("live"); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("closed reset=%v", err)
	}
	if target.IsActive("live") || target.ThreadID("live") != "" {
		t.Fatal("closed target exposes execution")
	}
}

func TestRuntimeNonKeyCapabilitiesUseCurrentRoleUnlocked(t *testing.T) {
	a := newChangeHarness("a")
	r, bs, b := bindingRuntime(t, a)
	target := r.AcquireRole(RoleReviewer).(*runtimeTarget)
	bindingSend(t, target, "k", "first")
	remapBindingRole(r, RoleReviewer, bs)
	b.onModels = func(context.Context) ([]Model, error) {
		if r.AcquireRole(RoleReviewer) != target {
			t.Error("role target changed")
		}
		return []Model{{ID: "b-model"}}, nil
	}
	models, err := target.Models(context.Background())
	if err != nil || len(models) != 1 || models[0].ID != "b-model" {
		t.Fatalf("models=%v %v", models, err)
	}
	if ready, _ := target.Available(); !ready {
		t.Fatal("current provider unavailable")
	}
	if target.ReadyEvents() != r.ReadyEvents() {
		t.Fatal("wrong readiness provider")
	}
	v, ch := target.Readiness()
	wantV, wantCh := r.Readiness()
	if v != wantV || ch != wantCh {
		t.Fatal("broadcast readiness not forwarded")
	}
	if err := target.CommitModel("b-model", func() error {
		if target.ThreadID("k") != "a-thread" {
			t.Error("callback could not inspect bound owner")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := target.CommitInference(InferenceSelection{Model: "b-next"}, func() error {
		_ = r.AcquireRole(RoleReviewer)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if target.HarnessConfig().Model != "b-next" || r.supervisor.HarnessConfig().Model != "" {
		t.Fatal("inference mutated execution owner")
	}
	a.finish("k")
}
