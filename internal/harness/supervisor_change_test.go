package harness

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

type changeHarness struct {
	*supervisorHarness
	onStart        func()
	onStartContext func(context.Context) error
	onModels       func(context.Context) ([]Model, error)
	onClose        func()
	onReset        func()
	startCalls     atomic.Int32
	closeCalls     atomic.Int32
	closeErr       error
}

func newChangeHarness(name string) *changeHarness {
	return &changeHarness{supervisorHarness: &supervisorHarness{name: name, followUp: FollowUpQueue, active: map[string]bool{}, emits: map[string]core.Emit{}}}
}
func (h *changeHarness) Start(ctx context.Context) error {
	h.startCalls.Add(1)
	if h.onStart != nil {
		h.onStart()
	}
	if h.onStartContext != nil {
		return h.onStartContext(ctx)
	}
	return h.supervisorHarness.Start(ctx)
}
func (h *changeHarness) Close() error {
	h.closeCalls.Add(1)
	if h.onClose != nil {
		h.onClose()
	}
	_ = h.supervisorHarness.Close()
	return h.closeErr
}
func (h *changeHarness) ResetSession(key string) error {
	if h.onReset != nil {
		h.onReset()
	}
	return h.supervisorHarness.ResetSession(key)
}
func (h *changeHarness) Models(ctx context.Context) ([]Model, error) {
	if h.onModels != nil {
		return h.onModels(ctx)
	}
	return nil, nil
}

func newChangeSupervisor(t *testing.T) (*Supervisor, *changeHarness, *changeHarness) {
	t.Helper()
	old, candidate := newChangeHarness("old"), newChangeHarness("new")
	registry := NewRegistry()
	registry.Register("old", func(HarnessConfig) (Harness, error) { return old, nil })
	registry.Register("new", func(HarnessConfig) (Harness, error) { return candidate, nil })
	s := NewSupervisor(registry, HarnessConfig{Name: "old", Model: "original"})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, old, candidate
}
func awaitChange[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for lifecycle operation")
		var zero T
		return zero
	}
}
func assertFenced(t *testing.T, operation func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- operation() }()
	if err := awaitChange(t, done); !errors.Is(err, ErrProviderFenced) {
		t.Fatalf("operation error = %v, want ErrProviderFenced", err)
	}
}

func TestSupervisorPreparedFenceRejectsExternalOperations(t *testing.T) {
	s, old, candidate := newChangeSupervisor(t)
	entered, release := make(chan struct{}), make(chan struct{})
	candidate.onStart = func() { close(entered); <-release }
	type result struct {
		change *PreparedChange
		err    error
	}
	prepared := make(chan result, 1)
	go func() {
		p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
		prepared <- result{p, err}
	}()
	awaitChange(t, entered)
	// Settle preparation even if an assertion fails, before cleanup calls Close.
	defer func() {
		close(release)
		got := awaitChange(t, prepared)
		if got.change != nil {
			_ = got.change.Abort()
		}
	}()
	ctx := context.Background()
	for name, operation := range map[string]func() error{
		"control": func() error {
			_, err := s.SendControl(ctx, "key", ControlRequest{ID: "control", Prompt: "prompt"})
			return err
		},
		"reset":  func() error { return s.ResetSession("key") },
		"models": func() error { _, err := s.Models(ctx); return err },
		"inference": func() error {
			return s.CommitModel("next", func() error { t.Error("fenced persistence called"); return nil })
		},
		"configure unavailable": func() error { return s.ConfigureUnavailable(HarnessConfig{}, errors.New("missing")) },
		"prepare":               func() error { _, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"}); return err },
	} {
		t.Run(name, func(t *testing.T) { assertFenced(t, operation) })
	}
	if s.HarnessConfig().Name != "old" || s.HasActiveTurns() {
		t.Fatal("prepare published candidate or admitted work")
	}
	old.mu.Lock()
	defer old.mu.Unlock()
	if len(old.resetKeys) != 0 || len(old.prompts) != 0 || old.closed {
		t.Fatal("fenced operations changed the old adapter")
	}
}

func TestSupervisorPrepareRejectsActiveAndPreservesContinuation(t *testing.T) {
	s, old, candidate := newChangeSupervisor(t)
	if _, _, err := s.Send(context.Background(), "key", "first", nil); err != nil {
		t.Fatal(err)
	}
	if _, queued, err := s.Send(context.Background(), "key", "second", nil); err != nil || !queued {
		t.Fatalf("queue = %t %v", queued, err)
	}
	if _, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"}); err == nil {
		t.Fatal("prepared active provider")
	}
	if !s.IsActive("key") || candidate.startCalls.Load() != 0 {
		t.Fatal("rejected prepare changed active work")
	}
	old.finish("key")
	waitSupervisorState(t, func() bool { old.mu.Lock(); defer old.mu.Unlock(); return len(old.prompts["key"]) == 2 })
	old.finish("key")
	waitSupervisorState(t, func() bool { s.mu.RLock(); defer s.mu.RUnlock(); return s.inflight == 0 })
	p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorPrepareFailurePreservesOldState(t *testing.T) {
	for _, factoryFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "start", true: "factory"}[factoryFailure], func(t *testing.T) {
			s, old, candidate := newChangeSupervisor(t)
			cause := errors.New("candidate failed")
			if factoryFailure {
				s.registry.Register("new", func(HarnessConfig) (Harness, error) { return nil, cause })
			} else {
				candidate.startErr = cause
			}
			before := s.HarnessConfig()
			if _, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"}); !errors.Is(err, cause) {
				t.Fatalf("prepare error = %v", err)
			}
			if !reflect.DeepEqual(s.HarnessConfig(), before) || old.closeCalls.Load() != 0 {
				t.Fatal("failure changed old provider/config")
			}
			if !factoryFailure && candidate.closeCalls.Load() != 1 {
				t.Fatal("failed candidate not cleaned up")
			}
			if _, _, err := s.Send(context.Background(), "key", "still works", nil); err != nil {
				t.Fatal(err)
			}
			old.finish("key")
		})
	}
}

func TestSupervisorAbortPreservesOldAndReopensAdmission(t *testing.T) {
	s, old, candidate := newChangeSupervisor(t)
	before := s.HarnessConfig()
	p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
	if err != nil {
		t.Fatal(err)
	}
	// Candidate cleanup can inspect supervisor state without a retained mutex.
	candidate.onClose = func() { _ = s.HarnessConfig() }
	candidate.closeErr = errors.New("cleanup failed")
	if err := p.Abort(); !errors.Is(err, candidate.closeErr) {
		t.Fatalf("abort error = %v", err)
	}
	if !reflect.DeepEqual(s.HarnessConfig(), before) || old.closeCalls.Load() != 0 || candidate.closeCalls.Load() != 1 {
		t.Fatal("abort changed old provider or lost cleanup")
	}
	if _, _, err := s.Send(context.Background(), "key", "after abort", nil); err != nil {
		t.Fatal(err)
	}
	old.finish("key")
}

func TestSupervisorCommitOnlyPublishesAndReturnsPrevious(t *testing.T) {
	s, old, candidate := newChangeSupervisor(t)
	cfg := HarnessConfig{Name: "new", Args: []string{"original"}}
	p, err := s.PrepareChange(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Args[0] = "caller mutation"
	candidate.startErr = errors.New("a second start must not occur")
	if previous := p.Commit(); previous != old {
		t.Fatal("commit did not return old adapter")
	}
	if candidate.startCalls.Load() != 1 || old.closeCalls.Load() != 0 {
		t.Fatal("commit performed lifecycle I/O")
	}
	if got := s.HarnessConfig(); got.Name != "new" || got.Args[0] != "original" {
		t.Fatalf("published config = %#v", got)
	}
	if thread, _, err := s.Send(context.Background(), "key", "new work", nil); err != nil || thread != "new-thread" {
		t.Fatalf("send = %q %v", thread, err)
	}
	candidate.finish("key")
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorCloseOwnsAbandonedPreparation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, old, candidate := newChangeSupervisor(t)
		p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
		if err != nil {
			t.Fatal(err)
		}
		closed := make(chan error, 1)
		go func() { closed <- s.Close() }()
		synctest.Wait()
		if err := awaitChange(t, closed); err != nil {
			t.Fatal(err)
		}
		if old.closeCalls.Load() != 1 || candidate.closeCalls.Load() != 1 {
			t.Fatal("Close did not retire both owned adapters")
		}
		if p.Commit() != nil {
			t.Fatal("orphaned commit published an adapter")
		}
		if err := p.Abort(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if s.HarnessConfig().Name != "old" || old.closeCalls.Load() != 1 || candidate.closeCalls.Load() != 1 {
			t.Fatal("late settlement changed state or repeated cleanup")
		}
		if _, _, err := s.Send(context.Background(), "key", "closed", nil); !errors.Is(err, ErrProviderUnavailable) {
			t.Fatalf("closed send = %v", err)
		}
	})
}

func TestSupervisorCloseWaitsForCandidateStartThenOwnsPreparation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, old, candidate := newChangeSupervisor(t)
		entered, release := make(chan struct{}), make(chan struct{})
		candidate.onStart = func() { close(entered); <-release }
		prepared := make(chan *PreparedChange, 1)
		go func() {
			p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
			if err != nil {
				t.Error(err)
			}
			prepared <- p
		}()
		awaitChange(t, entered)
		closed := make(chan error, 1)
		go func() { closed <- s.Close() }()
		synctest.Wait()
		if len(closed) != 0 || old.closeCalls.Load() != 0 || candidate.closeCalls.Load() != 0 {
			t.Fatal("Close raced candidate startup")
		}
		close(release)
		if err := awaitChange(t, closed); err != nil {
			t.Fatal(err)
		}
		p := awaitChange(t, prepared)
		if p == nil || p.Commit() != nil {
			t.Fatal("closed preparation can publish")
		}
		if err := p.Abort(); err != nil {
			t.Fatal(err)
		}
		if old.closeCalls.Load() != 1 || candidate.closeCalls.Load() != 1 {
			t.Fatal("incomplete/double adapter cleanup")
		}
	})
}

func TestSupervisorCloseObservesPreparationPublishedAfterLifecycleReservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, old, candidate := newChangeSupervisor(t)
		// Exercise the window between reserving lifecycle ownership and publishing
		// a returned change. Close must wake even though no fence exists yet.
		if err := s.beginLifecycle(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		closed := make(chan error, 1)
		go func() { closed <- s.Close() }()
		synctest.Wait()
		if len(closed) != 0 {
			t.Fatal("Close ignored lifecycle ownership")
		}
		s.mu.Lock()
		p := &PreparedChange{supervisor: s, config: HarnessConfig{Name: "new"}, candidate: candidate}
		s.fenced = true
		s.prepared = p
		s.wakeLifecycleLocked()
		s.mu.Unlock()
		if err := awaitChange(t, closed); err != nil {
			t.Fatal(err)
		}
		if old.closeCalls.Load() != 1 || candidate.closeCalls.Load() != 1 {
			t.Fatal("Close missed published preparation")
		}
	})
}

func TestSupervisorReadinessBroadcastAndLegacyEdge(t *testing.T) {
	s := NewSupervisor(NewRegistry(), HarnessConfig{})
	target := newChangeHarness("old")
	s.registry.Register("old", func(HarnessConfig) (Harness, error) { return target, nil })
	// Pre-start reconfiguration retains lazy construction.
	if err := s.Reconfigure(HarnessConfig{Name: "old"}); err != nil {
		t.Fatal(err)
	}
	if target.startCalls.Load() != 0 {
		t.Fatal("pre-start reconfigure started provider")
	}
	version, first := s.Readiness()
	otherVersion, second := s.Readiness()
	if version != otherVersion {
		t.Fatal("inconsistent observer snapshots")
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	awaitChange(t, first)
	awaitChange(t, second)
	awaitChange(t, s.ReadyEvents())
	next, waiting := s.Readiness()
	if next <= version {
		t.Fatal("readiness version did not advance")
	}
	select {
	case <-waiting:
		t.Fatal("fresh snapshot already closed")
	default:
	}
	s.registry.Register("old", func(HarnessConfig) (Harness, error) { return newChangeHarness("old"), nil })
	p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "old"})
	if err != nil {
		t.Fatal(err)
	}
	awaitChange(t, waiting)
	_, abortFirst := s.Readiness()
	_, abortSecond := s.Readiness()
	_ = p.Abort()
	awaitChange(t, abortFirst)
	awaitChange(t, abortSecond)
}

func TestSupervisorAvailabilityErrorsPreserveCause(t *testing.T) {
	cause := errors.New("missing executable")
	registry := NewRegistry()
	registry.Register("bad", func(HarnessConfig) (Harness, error) { return nil, cause })
	s := NewSupervisor(registry, HarnessConfig{Name: "bad"})
	if _, _, err := s.Send(context.Background(), "key", "prompt", nil); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("not-started error = %v", err)
	}
	if err := s.Start(context.Background()); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if _, _, err := s.Send(context.Background(), "key", "prompt", nil); !errors.Is(err, ErrProviderUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("startup error = %v", err)
	}
}

func TestSupervisorPrepareSerializesWithSessionReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, old, candidate := newChangeSupervisor(t)
		entered, release := make(chan struct{}), make(chan struct{})
		old.onReset = func() { close(entered); <-release }
		reset := make(chan error, 1)
		go func() { reset <- s.ResetSession("key") }()
		awaitChange(t, entered)
		prepared := make(chan *PreparedChange, 1)
		go func() {
			p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
			if err != nil {
				t.Error(err)
			}
			prepared <- p
		}()
		synctest.Wait()
		if len(prepared) != 0 || candidate.startCalls.Load() != 0 {
			t.Fatal("prepare raced session mutation")
		}
		close(release)
		if err := awaitChange(t, reset); err != nil {
			t.Fatal(err)
		}
		p := awaitChange(t, prepared)
		if p == nil {
			t.Fatal("prepare failed")
		}
		_ = p.Abort()
	})
}

func TestSupervisorInferencePersistenceRunsUnlockedAndOrdersContinuation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, old, _ := newChangeSupervisor(t)
		if _, _, err := s.Send(context.Background(), "key", "first", nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Send(context.Background(), "key", "second", nil); err != nil {
			t.Fatal(err)
		}
		committed := make(chan error, 1)
		entered, release := make(chan struct{}), make(chan struct{})
		go func() {
			committed <- s.CommitModel("next", func() error {
				_ = s.HarnessConfig()
				_ = s.IsActive("key")
				old.finish("key")
				close(entered)
				<-release
				return nil
			})
		}()
		awaitChange(t, entered)
		synctest.Wait()
		old.mu.Lock()
		count := len(old.prompts["key"])
		old.mu.Unlock()
		if count != 1 {
			t.Fatal("continuation escaped persistence boundary")
		}
		close(release)
		if err := awaitChange(t, committed); err != nil {
			t.Fatal(err)
		}
		waitSupervisorState(t, func() bool { old.mu.Lock(); defer old.mu.Unlock(); return len(old.models["key"]) == 2 })
		old.mu.Lock()
		models := append([]string(nil), old.models["key"]...)
		old.mu.Unlock()
		if !reflect.DeepEqual(models, []string{"original", "next"}) {
			t.Fatalf("models = %v", models)
		}
		old.finish("key")
	})
}

type returningSupervisorHarness struct {
	*changeHarness
	terminal chan struct{}
	release  chan struct{}
	sendErr  error
}

func (h *returningSupervisorHarness) SendWithModel(_ context.Context, _ string, _ string, _ string, emit core.Emit) (string, bool, error) {
	if h.sendErr != nil {
		return "", false, h.sendErr
	}
	emit(core.Event{Kind: core.EventFinal, Done: true})
	close(h.terminal)
	<-h.release
	return "thread", false, nil
}

func TestSupervisorPrepareDrainsSendStillReturningAfterTerminal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := &returningSupervisorHarness{changeHarness: newChangeHarness("old"), terminal: make(chan struct{}), release: make(chan struct{})}
		candidate := newChangeHarness("new")
		registry := NewRegistry()
		registry.Register("old", func(HarnessConfig) (Harness, error) { return old, nil })
		var constructed atomic.Bool
		registry.Register("new", func(HarnessConfig) (Harness, error) { constructed.Store(true); return candidate, nil })
		s := NewSupervisor(registry, HarnessConfig{Name: "old"})
		if err := s.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		sent := make(chan error, 1)
		go func() {
			_, _, err := s.Send(context.Background(), "key", "prompt", func(core.Event) { _ = s.HarnessConfig() })
			sent <- err
		}()
		awaitChange(t, old.terminal)
		prepared := make(chan *PreparedChange, 1)
		go func() {
			p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
			if err != nil {
				t.Error(err)
			}
			prepared <- p
		}()
		synctest.Wait()
		if len(prepared) != 0 || constructed.Load() {
			t.Fatal("candidate created before provider call drained")
		}
		close(old.release)
		if err := awaitChange(t, sent); err != nil {
			t.Fatal(err)
		}
		p := awaitChange(t, prepared)
		if p == nil {
			t.Fatal("prepare failed")
		}
		_ = p.Abort()
	})
}

func TestSupervisorProviderExecutionErrorsAreNotAvailabilityErrors(t *testing.T) {
	cause := errors.New("execution failed")
	target := &returningSupervisorHarness{changeHarness: newChangeHarness("old"), sendErr: cause}
	registry := NewRegistry()
	registry.Register("old", func(HarnessConfig) (Harness, error) { return target, nil })
	s := NewSupervisor(registry, HarnessConfig{Name: "old"})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, _, err := s.Send(context.Background(), "key", "prompt", nil)
	if !errors.Is(err, cause) || errors.Is(err, ErrProviderUnavailable) || errors.Is(err, ErrProviderFenced) {
		t.Fatalf("provider execution error = %v", err)
	}
	if s.HasActiveTurns() {
		t.Fatal("failed admission retained active state")
	}
}

func TestSupervisorFailedInferencePersistenceReopensDispatch(t *testing.T) {
	s, old, _ := newChangeSupervisor(t)
	cause := errors.New("persistence failed")
	if err := s.CommitModel("rejected", func() error {
		_ = s.HarnessConfig()
		return cause
	}); !errors.Is(err, cause) {
		t.Fatalf("commit error = %v", err)
	}
	if _, _, err := s.Send(context.Background(), "key", "prompt", nil); err != nil {
		t.Fatal(err)
	}
	old.mu.Lock()
	model := old.models["key"][0]
	old.mu.Unlock()
	if model != "original" || s.HarnessConfig().Model != "original" {
		t.Fatal("failed persistence changed inference snapshot")
	}
	old.finish("key")
}

func TestSupervisorSendsWaitThroughStructuralPreparation(t *testing.T) {
	for _, conversation := range []bool{false, true} {
		for _, commit := range []bool{false, true} {
			name := map[bool]string{false: "send", true: "conversation"}[conversation] + "/" + map[bool]string{false: "abort", true: "commit"}[commit]
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					s, old, candidate := newChangeSupervisor(t)
					entered, release := make(chan struct{}), make(chan struct{})
					candidate.onStart = func() { close(entered); <-release }
					prepared := make(chan *PreparedChange, 1)
					go func() {
						p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
						if err != nil {
							t.Error(err)
						}
						prepared <- p
					}()
					awaitChange(t, entered)
					type result struct {
						thread string
						err    error
					}
					sent := make(chan result, 1)
					go func() {
						var thread string
						var err error
						if conversation {
							thread, _, err = s.SendConversation(context.Background(), "key", "prompt", "message", nil)
						} else {
							thread, _, err = s.Send(context.Background(), "key", "prompt", nil)
						}
						sent <- result{thread, err}
					}()
					synctest.Wait()
					if len(sent) != 0 || s.HasActiveTurns() {
						t.Fatal("send escaped candidate preparation fence")
					}
					if stopped, err := s.Interrupt(context.Background(), "key"); stopped || err != nil {
						t.Fatalf("idle fenced interrupt = %t %v", stopped, err)
					}
					close(release)
					p := awaitChange(t, prepared)
					if p == nil {
						t.Fatal("prepare failed")
					}
					synctest.Wait()
					if len(sent) != 0 {
						t.Fatal("send escaped unsettled change")
					}
					want := "old-thread"
					if commit {
						want = "new-thread"
						previous := p.Commit()
						if previous != old {
							t.Fatal("wrong previous adapter")
						}
						_ = previous.Close()
					} else {
						if err := p.Abort(); err != nil {
							t.Fatal(err)
						}
					}
					got := awaitChange(t, sent)
					if got.err != nil || got.thread != want {
						t.Fatalf("send = %q %v; want %q", got.thread, got.err, want)
					}
					if commit {
						candidate.finish("key")
					} else {
						old.finish("key")
					}
				})
			})
		}
	}
}

func TestSupervisorSameSessionFenceWaitersCancelIndependently(t *testing.T) {
	for _, conversation := range []bool{false, true} {
		t.Run(map[bool]string{false: "send", true: "conversation"}[conversation], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, old, _ := newChangeSupervisor(t)
				p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
				if err != nil {
					t.Fatal(err)
				}
				defer p.Abort()
				send := func(ctx context.Context) error {
					if conversation {
						_, _, err := s.SendConversation(ctx, "key", "prompt", "message", nil)
						return err
					}
					_, _, err := s.Send(ctx, "key", "prompt", nil)
					return err
				}
				first := make(chan error, 1)
				go func() { first <- send(context.Background()) }()
				synctest.Wait()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				second := make(chan error, 1)
				go func() { second <- send(ctx) }()
				synctest.Wait()
				if len(first) != 0 || len(second) != 0 {
					t.Fatal("fenced send did not wait")
				}
				cancel()
				if err := awaitChange(t, second); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled send = %v", err)
				}
				if err := p.Abort(); err != nil {
					t.Fatal(err)
				}
				if err := awaitChange(t, first); err != nil {
					t.Fatal(err)
				}
				old.mu.Lock()
				calls := len(old.prompts["key"])
				old.mu.Unlock()
				if calls != 1 {
					t.Fatalf("provider received %d prompts; cancelled waiter must not dispatch", calls)
				}
				old.finish("key")
			})
		})
	}
}

func TestSupervisorPrepareFencesThenDrainsModels(t *testing.T) {
	for _, cancelPrepare := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "cancel"}[cancelPrepare], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, old, candidate := newChangeSupervisor(t)
				entered, release := make(chan struct{}), make(chan struct{})
				old.onModels = func(context.Context) ([]Model, error) { close(entered); <-release; return nil, nil }
				models := make(chan error, 1)
				go func() { _, err := s.Models(context.Background()); models <- err }()
				awaitChange(t, entered)
				var constructed atomic.Bool
				s.registry.Register("new", func(HarnessConfig) (Harness, error) { constructed.Store(true); return candidate, nil })
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				type result struct {
					change *PreparedChange
					err    error
				}
				prepared := make(chan result, 1)
				go func() { p, err := s.PrepareChange(ctx, HarnessConfig{Name: "new"}); prepared <- result{p, err} }()
				synctest.Wait()
				if len(prepared) != 0 || constructed.Load() {
					t.Fatal("prepare rejected/constructed before inflight drain")
				}
				s.mu.RLock()
				fenced := s.fenced
				s.mu.RUnlock()
				if !fenced {
					t.Fatal("prepare did not fence before draining")
				}
				if _, err := s.Models(context.Background()); !errors.Is(err, ErrProviderFenced) {
					t.Fatalf("new discovery = %v", err)
				}
				if cancelPrepare {
					cancel()
					got := awaitChange(t, prepared)
					if !errors.Is(got.err, context.Canceled) || got.change != nil || constructed.Load() {
						t.Fatalf("cancelled preparation = %#v", got)
					}
					s.mu.RLock()
					fenced = s.fenced
					s.mu.RUnlock()
					if fenced {
						t.Fatal("cancelled prepare left fence closed")
					}
					close(release)
				} else {
					close(release)
					got := awaitChange(t, prepared)
					if got.err != nil || got.change == nil {
						t.Fatalf("prepare = %#v", got)
					}
					if err := got.change.Abort(); err != nil {
						t.Fatal(err)
					}
				}
				if err := awaitChange(t, models); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestSupervisorStartWaitsForPreStartPreparation(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort", true: "commit"}[commit], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				old, candidate := newChangeHarness("old"), newChangeHarness("new")
				registry := NewRegistry()
				registry.Register("old", func(HarnessConfig) (Harness, error) { return old, nil })
				registry.Register("new", func(HarnessConfig) (Harness, error) { return candidate, nil })
				s := NewSupervisor(registry, HarnessConfig{Name: "old"})
				defer s.Close()
				p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
				if err != nil {
					t.Fatal(err)
				}
				started := make(chan error, 1)
				go func() { started <- s.Start(context.Background()) }()
				synctest.Wait()
				if len(started) != 0 || old.startCalls.Load() != 0 || candidate.startCalls.Load() != 0 {
					t.Fatal("Start raced pre-start configuration")
				}
				if commit {
					if p.Commit() != nil {
						t.Fatal("pre-start commit returned an old adapter")
					}
				} else {
					_ = p.Abort()
				}
				if err := awaitChange(t, started); err != nil {
					t.Fatal(err)
				}
				want := old
				if commit {
					want = candidate
				}
				if old.startCalls.Load()+candidate.startCalls.Load() != 1 || want.startCalls.Load() != 1 {
					t.Fatal("Start used wrong committed configuration")
				}
			})
		})
	}
}

func TestSupervisorInterruptIdleFenceDoesNotCallAdapter(t *testing.T) {
	s, old, _ := newChangeSupervisor(t)
	old.interruptErr = errors.New("adapter must not be called")
	p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Abort()
	if stopped, err := s.Interrupt(context.Background(), "key"); stopped || err != nil {
		t.Fatalf("interrupt = %t %v", stopped, err)
	}
}

func TestSupervisorPreparedSettlementIsIdempotent(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "abort", true: "commit"}[commit], func(t *testing.T) {
			s, old, candidate := newChangeSupervisor(t)
			p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
			if err != nil {
				t.Fatal(err)
			}
			func() {
				defer p.Abort()
				if commit {
					if previous := p.Commit(); previous != old {
						t.Fatal("commit returned wrong old adapter")
					} else {
						_ = previous.Close()
					}
				} else {
					_ = p.Abort()
				}
			}()
			if err := p.Abort(); err != nil {
				t.Fatal(err)
			}
			if p.Commit() != nil {
				t.Fatal("settled change republished")
			}
			want := "old"
			if commit {
				want = "new"
			}
			if s.HarnessConfig().Name != want {
				t.Fatal("deferred cleanup changed publication")
			}
			if commit && candidate.closeCalls.Load() != 0 {
				t.Fatal("deferred abort closed committed adapter")
			}
			if !commit && candidate.closeCalls.Load() != 1 {
				t.Fatal("abort cleanup repeated")
			}
		})
	}
}

func TestSupervisorPrepareContextBoundsCandidateStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, old, candidate := newChangeSupervisor(t)
		entered := make(chan struct{})
		candidate.onStartContext = func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		prepared := make(chan error, 1)
		go func() { _, err := s.PrepareChange(ctx, HarnessConfig{Name: "new"}); prepared <- err }()
		awaitChange(t, entered)
		cancel()
		if err := awaitChange(t, prepared); !errors.Is(err, context.Canceled) {
			t.Fatalf("prepare cancellation = %v", err)
		}
		if candidate.closeCalls.Load() != 1 || old.closeCalls.Load() != 0 || s.HarnessConfig().Name != "old" {
			t.Fatal("cancelled prepare changed old provider")
		}
		if _, _, err := s.Send(context.Background(), "key", "prompt", nil); err != nil {
			t.Fatal(err)
		}
		old.finish("key")
	})
}

func TestSupervisorPreparedAdapterOutlivesPreparationContext(t *testing.T) {
	s, _, candidate := newChangeSupervisor(t)
	var lifetime context.Context
	candidate.onStartContext = func(ctx context.Context) error { lifetime = ctx; return nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := s.PrepareChange(ctx, HarnessConfig{Name: "new"})
	if err != nil {
		t.Fatal(err)
	}
	previous := p.Commit()
	_ = previous.Close()
	cancel()
	if err := lifetime.Err(); err != nil {
		t.Fatalf("transaction cancellation killed committed adapter: %v", err)
	}
}

// SendWithModel follows the ordinary adapter contract: active sends attempt
// native steering and report steered=true even when that attempt fails.
type blockedSteerHarness struct {
	*changeHarness
	entered chan struct{}
	release chan struct{}
}

func (h *blockedSteerHarness) SendWithModel(ctx context.Context, key, prompt, model string, emit core.Emit) (string, bool, error) {
	if h.IsActive(key) {
		thread, err := h.Steer(ctx, key, prompt, emit, nil)
		return thread, true, err
	}
	return h.supervisorHarness.SendWithModel(ctx, key, prompt, model, emit)
}

func (h *blockedSteerHarness) Steer(context.Context, string, string, core.Emit, func() bool) (string, error) {
	close(h.entered)
	<-h.release
	return "old-thread", errors.New("turn finished before steering")
}

func TestSupervisorSteerRetryReleasesInflightBeforeFenceWait(t *testing.T) {
	for _, commit := range []bool{true, false} {
		t.Run(map[bool]string{true: "commit", false: "abort"}[commit], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				old := &blockedSteerHarness{changeHarness: newChangeHarness("old"), entered: make(chan struct{}), release: make(chan struct{})}
				old.followUp = FollowUpSteer
				candidate := newChangeHarness("new")
				registry := NewRegistry()
				registry.Register("old", func(HarnessConfig) (Harness, error) { return old, nil })
				registry.Register("new", func(HarnessConfig) (Harness, error) { return candidate, nil })
				s := NewSupervisor(registry, HarnessConfig{Name: "old"})
				if err := s.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				if _, _, err := s.Send(context.Background(), "key", "original", nil); err != nil {
					t.Fatal(err)
				}
				type result struct {
					thread string
					err    error
				}
				sent := make(chan result, 1)
				go func() {
					thread, _, err := s.Send(context.Background(), "key", "follow-up", nil)
					sent <- result{thread, err}
				}()
				awaitChange(t, old.entered)
				old.finish("key")
				prepared := make(chan *PreparedChange, 1)
				go func() {
					p, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
					if err != nil {
						t.Error(err)
					}
					prepared <- p
				}()
				synctest.Wait()
				s.mu.RLock()
				fenced, inflight, draining := s.fenced, s.inflight, s.inflightDone != nil
				s.mu.RUnlock()
				if !fenced || inflight != 1 || !draining || len(prepared) != 0 {
					t.Fatalf("prepare must drain blocked steer: fenced=%t inflight=%d draining=%t", fenced, inflight, draining)
				}
				close(old.release)
				p := awaitChange(t, prepared)
				if p == nil {
					t.Fatal("prepare failed")
				}
				defer p.Abort()
				synctest.Wait()
				s.mu.RLock()
				inflight = s.inflight
				s.mu.RUnlock()
				if inflight != 0 || len(sent) != 0 {
					t.Fatalf("retry must wait without inflight ownership: inflight=%d completed=%d", inflight, len(sent))
				}
				operation := s.controlOperation("key")
				if !operation.TryLock() {
					t.Fatal("retry retained session lock")
				}
				operation.Unlock()
				want := "old-thread"
				if commit {
					want = "new-thread"
					previous := p.Commit()
					if previous != old {
						t.Fatal("wrong retired adapter")
					}
					if err := previous.Close(); err != nil {
						t.Fatal(err)
					}
				} else if err := p.Abort(); err != nil {
					t.Fatal(err)
				}
				got := awaitChange(t, sent)
				if got.err != nil || got.thread != want {
					t.Fatalf("retry = %q %v; want %q", got.thread, got.err, want)
				}
				if commit {
					candidate.finish("key")
				} else {
					old.finish("key")
				}
			})
		})
	}
}
