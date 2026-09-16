package harness

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
)

func TestRuntimeSingleProviderOwnershipAndCapabilities(t *testing.T) {
	registry := NewRegistry()
	provider := newChangeHarness("old")
	created := 0
	registry.Register("old", func(cfg HarnessConfig) (Harness, error) {
		created++
		if cfg.Name != "old" || cfg.SessionsFile != "one-session-file" {
			t.Errorf("config = %+v", cfg)
		}
		return provider, nil
	})
	r := NewRuntime(registry, HarnessConfig{Name: " OLD ", SessionsFile: "one-session-file"})
	target := r.AcquireRole(RoleChat)
	for _, role := range []Role{RoleChat, RoleDeveloper, RoleReviewer, RoleNotification, RoleHeartbeat} {
		if r.AcquireRole(role) != target {
			t.Fatalf("role %s has a different target", role)
		}
	}
	// The wrapper preserves every public operational Supervisor method, while
	// structural and lifecycle methods cannot be recovered by type assertion.
	typ := reflect.TypeOf(target)
	for _, name := range []string{"Start", "Close", "Reconfigure", "PrepareChange", "ConfigureUnavailable"} {
		if _, exists := typ.MethodByName(name); exists {
			t.Errorf("operational target exposes %s", name)
		}
	}
	for _, name := range []string{"Send", "SendConversation", "ConversationAdmission", "Interrupt", "IsActive", "ThreadID", "SendControl", "ResetSession", "Models", "Available", "ReadyEvents", "Readiness", "CommitInference", "CommitModel", "HasActiveTurns", "HarnessConfig"} {
		if _, exists := typ.MethodByName(name); !exists {
			t.Errorf("missing capability %s", name)
		}
	}
	if _, ok := target.(*Supervisor); ok {
		t.Fatal("raw supervisor escaped")
	}
	before, changed := r.Readiness()
	if created != 0 {
		t.Fatal("provider constructed before Start")
	}
	for i := 0; i < 2; i++ {
		if err := r.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if created != 1 || provider.startCalls.Load() != 1 {
		t.Fatalf("created=%d starts=%d", created, provider.startCalls.Load())
	}
	select {
	case <-changed:
	default:
		t.Fatal("runtime did not forward readiness")
	}
	after, _ := target.(interface {
		Readiness() (uint64, <-chan struct{})
	}).Readiness()
	if after <= before {
		t.Fatal("readiness version did not advance")
	}
	if ready, _ := r.Available(); !ready {
		t.Fatal("started provider unavailable")
	}
	for i := 0; i < 2; i++ {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if provider.closeCalls.Load() != 1 {
		t.Fatalf("closed %d times", provider.closeCalls.Load())
	}
}

func TestRuntimeReconfigurePreStartFailureAndRecovery(t *testing.T) {
	registry := NewRegistry()
	old, next := newChangeHarness("old"), newChangeHarness("new")
	old.startErr = errors.New("start failed")
	registry.Register("old", func(HarnessConfig) (Harness, error) { return old, nil })
	registry.Register("new", func(HarnessConfig) (Harness, error) { return next, nil })
	r := NewRuntime(registry, HarnessConfig{Name: "old"})
	defer r.Close()
	if err := r.Reconfigure(HarnessConfig{Name: " OLD ", Model: "pre-start"}); err != nil {
		t.Fatal(err)
	}
	if old.startCalls.Load() != 0 || r.HarnessConfig().Model != "pre-start" {
		t.Fatal("pre-start configuration started a provider or was lost")
	}
	if err := r.Start(context.Background()); err == nil {
		t.Fatal("expected startup failure")
	}
	if ready, _ := r.Available(); ready {
		t.Fatal("failed provider available")
	}
	if err := r.Reconfigure(HarnessConfig{Name: "new"}); err != nil {
		t.Fatal(err)
	}
	if ready, _ := r.Available(); !ready {
		t.Fatal("reconfiguration failed to restore availability")
	}
	if next.startCalls.Load() != 1 {
		t.Fatal("candidate not started exactly once")
	}
	failed := newChangeHarness("failed")
	failed.startErr = errors.New("candidate failed")
	registry.Register("failed", func(HarnessConfig) (Harness, error) { return failed, nil })
	if err := r.Reconfigure(HarnessConfig{Name: "failed"}); err == nil {
		t.Fatal("expected preparation failure")
	}
	if r.HarnessConfig().Name != "new" || next.closeCalls.Load() != 0 || failed.closeCalls.Load() != 1 {
		t.Fatal("failed preparation altered current provider or leaked candidate")
	}
	thread, _, err := r.AcquireRole(RoleChat).Send(context.Background(), "key", "prompt", nil)
	if err != nil || thread != "new-thread" {
		t.Fatalf("send = %q %v", thread, err)
	}
	next.finish("key")
}

func TestRuntimeReconfigurePreparesBeforePublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := NewRegistry()
		old, next := newChangeHarness("old"), newChangeHarness("new")
		entered, release := make(chan struct{}), make(chan struct{})
		next.onStart = func() { close(entered); <-release }
		registry.Register("old", func(HarnessConfig) (Harness, error) { return old, nil })
		registry.Register("new", func(HarnessConfig) (Harness, error) { return next, nil })
		r := NewRuntime(registry, HarnessConfig{Name: "old"})
		defer r.Close()
		if err := r.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		target := r.AcquireRole(RoleChat)
		done := make(chan error, 1)
		go func() { done <- r.Reconfigure(HarnessConfig{Name: "new"}) }()
		awaitChange(t, entered)
		if r.HarnessConfig().Name != "old" || old.closeCalls.Load() != 0 {
			t.Fatal("published before preparation finished")
		}
		sent := make(chan string, 1)
		go func() {
			thread, _, err := target.Send(context.Background(), "key", "prompt", nil)
			if err != nil {
				t.Error(err)
			}
			sent <- thread
		}()
		synctest.Wait()
		if len(sent) != 0 {
			t.Fatal("send escaped preparation fence")
		}
		close(release)
		if err := awaitChange(t, done); err != nil {
			t.Fatal(err)
		}
		if thread := awaitChange(t, sent); thread != "new-thread" {
			t.Fatalf("thread=%s", thread)
		}
		if old.closeCalls.Load() != 1 || next.startCalls.Load() != 1 {
			t.Fatal("incorrect retirement/start count")
		}
		if r.AcquireRole(RoleChat) != target {
			t.Fatal("operational target changed")
		}
		next.finish("key")
	})
}

func TestRuntimeConcurrentLifecycle(t *testing.T) {
	registry := NewRegistry()
	var mu sync.Mutex
	var providers []*changeHarness
	registry.Register("old", func(HarnessConfig) (Harness, error) {
		h := newChangeHarness("old")
		mu.Lock()
		providers = append(providers, h)
		mu.Unlock()
		return h, nil
	})
	r := NewRuntime(registry, HarnessConfig{Name: "old"})
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 3 {
			case 0:
				_ = r.Start(context.Background())
			case 1:
				_ = r.Reconfigure(HarnessConfig{Name: "old"})
			case 2:
				_ = r.Close()
			}
		}(i)
	}
	wg.Wait()
	if ready, _ := r.Available(); ready {
		t.Fatal("closed runtime available")
	}
	for _, h := range providers {
		if h.startCalls.Load() != 1 || h.closeCalls.Load() != 1 {
			t.Fatalf("starts=%d closes=%d", h.startCalls.Load(), h.closeCalls.Load())
		}
	}
}
