package harness

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// These tests pin C8.3's whole-runtime shutdown contract: provider supervisors
// close concurrently with deterministic identity attribution, every concurrent
// or repeated Close caller shares one stored orchestration result, watchers
// are joined before Close returns, admissions fail once the closed state is
// published, and shutdown never waits for active turns to complete naturally.

// closeSpec is a three-provider topology with distinct harness kinds so the
// fixture factory can attribute each provider's close callbacks.
func closeSpec() RuntimeSpec {
	return RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"chat":      {Name: "chat"},
		"developer": {Name: "developer"},
		"reviewer":  {Name: "reviewer"},
	}, Roles: map[Role]ProviderID{RoleChat: "chat", RoleDeveloper: "developer", RoleReviewer: "reviewer"}}
}

// waitRuntimeShutdown blocks until the runtime-owned shutdown lifetime is
// cancelled; its deadline is only a failure guard.
func waitRuntimeShutdown(t *testing.T, r *Runtime) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for r.shutdown.Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("runtime shutdown lifetime was never cancelled")
		}
		time.Sleep(time.Millisecond)
	}
}

// R1: Runtime.Close must enter every independent provider close before any of
// them releases; a sequential close would block on the first gated provider.
func TestRuntimeCloseEntersEveryProviderConcurrently(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	for _, h := range providers {
		h.onClose = func() { entered <- struct{}{}; <-release }
	}
	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	for index := 0; index < 3; index++ {
		awaitChange(t, entered)
	}
	close(release)
	if err := awaitChange(t, done); err != nil {
		t.Fatal(err)
	}
	for id, h := range providers {
		if h.closeCalls.Load() != 1 {
			t.Fatalf("provider %q closed %d times", id, h.closeCalls.Load())
		}
	}
}

// R2: the aggregate Close waits for every gated provider; releasing a subset
// never lets it return while another close is still gated, without depending
// on wall-clock thresholds for ordering.
func TestRuntimeCloseWaitsForEveryProviderClose(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	releases := map[ProviderID]chan struct{}{}
	for id, h := range providers {
		release := make(chan struct{})
		releases[id] = release
		h.onClose = func() { <-release }
	}
	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	// Gate order is deterministic here only through which channel closes;
	// Close may not return while any provider close is still gated.
	close(releases["chat"])
	select {
	case err := <-done:
		t.Fatalf("Close returned with gated providers: %v", err)
	default:
	}
	close(releases["developer"])
	select {
	case err := <-done:
		t.Fatalf("Close returned with a gated provider: %v", err)
	default:
	}
	close(releases["reviewer"])
	if err := awaitChange(t, done); err != nil {
		t.Fatal(err)
	}
}

// R3: one failing provider close surfaces with its ProviderID, while all
// providers still close exactly once.
func TestRuntimeCloseReportsFailingProviderIdentity(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	boom := errors.New("provider exploded")
	providers["developer"].closeErr = boom
	err := r.Close()
	if !errors.Is(err, boom) {
		t.Fatalf("Close = %v, want the provider failure", err)
	}
	if !strings.Contains(err.Error(), `close provider "developer"`) {
		t.Fatalf("Close lost provider identity: %q", err.Error())
	}
	for id, h := range providers {
		if h.closeCalls.Load() != 1 {
			t.Fatalf("provider %q closed %d times", id, h.closeCalls.Load())
		}
	}
}

// R4: multiple failing provider closes all surface, aggregated in ProviderID
// order; healthy providers still close exactly once.
func TestRuntimeCloseAggregatesEveryFailureDeterministically(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	first := errors.New("first failure")
	second := errors.New("second failure")
	providers["reviewer"].closeErr = first
	providers["chat"].closeErr = second
	err := r.Close()
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("Close = %v, want both provider failures", err)
	}
	got := err.Error()
	if strings.Index(got, `close provider "chat"`) > strings.Index(got, `close provider "reviewer"`) {
		t.Fatalf("failures aggregated out of ProviderID order: %q", got)
	}
	if providers["developer"].closeCalls.Load() != 1 {
		t.Fatalf("healthy provider closed %d times", providers["developer"].closeCalls.Load())
	}
}

// R5: many concurrent Close callers observe exactly one shutdown
// orchestration — every supervisor closes once — and the same stored result.
func TestRuntimeConcurrentCloseSharesOneOrchestrationResult(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	boom := errors.New("concurrent close failure")
	providers["reviewer"].closeErr = boom
	const callers = 8
	results := make(chan error, callers)
	for index := 0; index < callers; index++ {
		go func() { results <- r.Close() }()
	}
	var first string
	for index := 0; index < callers; index++ {
		err := awaitChange(t, results)
		if !errors.Is(err, boom) {
			t.Fatalf("concurrent Close = %v, want the shared orchestration failure", err)
		}
		if first == "" {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("callers observed different results: %q vs %q", first, err.Error())
		}
	}
	for id, h := range providers {
		if h.closeCalls.Load() != 1 {
			t.Fatalf("provider %q closed %d times", id, h.closeCalls.Load())
		}
	}
	// A later repeated caller also receives the stored orchestration result.
	if err := r.Close(); !errors.Is(err, boom) || err.Error() != first {
		t.Fatalf("repeated Close = %v, want the stored result %q", err, first)
	}
}

// R12: Close joins every provider watcher before returning, and a watcher can
// no longer notify readiness afterwards.
func TestRuntimeCloseJoinsWatchersBeforeReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, _ := topologyFixture(t, closeSpec())
		r.mu.Lock()
		entries := make([]*providerEntry, 0, len(r.providers))
		for _, p := range r.providers {
			entries = append(entries, p)
		}
		r.mu.Unlock()
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		for _, p := range entries {
			select {
			case <-p.watchDone:
			default:
				t.Fatalf("provider %q watcher survived Close", p.id)
			}
		}
		version, _ := r.Readiness()
		for _, p := range entries {
			p.supervisor.mu.Lock()
			p.supervisor.broadcastReadyLocked()
			p.supervisor.mu.Unlock()
		}
		synctest.Wait()
		if current, _ := r.Readiness(); current != version {
			t.Fatal("readiness changed after Close through a survived watcher")
		}
	})
}

// R12: the watcher join precedes the provider fan-out, which is what makes
// "no watcher survives Close" guaranteed rather than eventual. One watcher is
// parked deterministically inside its forwarding iteration — its supervisor's
// readiness change is armed and the supervisor's write lock is held so the
// watcher's Readiness call cannot return — and a second provider's close entry
// is observable. A correct Close parks at the watcher join and no provider
// close can begin while the parked watcher provably lives; a skipped join
// enters the fan-out and fails the test deterministically.
func TestRuntimeCloseJoinsWatchersBeforeProviderFanOut(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	r.mu.Lock()
	chatSupervisor := r.providers["chat"].supervisor
	chatEntry := r.providers["chat"]
	r.mu.Unlock()
	developerCloseBegan := make(chan struct{})
	providers["developer"].onClose = func() { close(developerCloseBegan) }
	// Quiesce the chat watcher first: a fixture-start readiness broadcast may
	// still be mid-iteration, and that iteration would consume the parking
	// broadcast below without parking the watcher. Waiting for the forwarded
	// version bump proves the watcher finished its iteration and is parked on
	// the supervisor's current readiness channel.
	version, _ := r.Readiness()
	chatSupervisor.mu.Lock()
	chatSupervisor.broadcastReadyLocked()
	chatSupervisor.mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for {
		current, _ := r.Readiness()
		if current > version {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("chat watcher never forwarded the quiescing readiness change")
		}
		time.Sleep(time.Millisecond)
	}
	// Park the chat watcher inside its forwarding iteration: broadcast the
	// readiness change while holding the supervisor's write lock, so the
	// watcher — committed to the changed case by that broadcast — parks in
	// its Readiness call until this test releases the lock. Close can never
	// close a stop channel before that commit, so no other exit exists.
	chatSupervisor.mu.Lock()
	chatSupervisor.broadcastReadyLocked()
	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	select {
	case <-developerCloseBegan:
		chatSupervisor.mu.Unlock()
		t.Fatal("provider fan-out began while a parked watcher was still alive")
	case <-time.After(time.Second):
		// Correct: the fan-out never started because the join still waits
		// on the parked watcher; the deadline is only a guard for a correct
		// run, since no provider close can structurally begin yet.
	}
	chatSupervisor.mu.Unlock()
	// Releasing the supervisor lets the parked watcher observe its closed
	// stop channel, exit, and unblock the join; the fan-out then closes every
	// provider exactly once before Close returns.
	if err := awaitChange(t, done); err != nil {
		t.Fatal(err)
	}
	select {
	case <-chatEntry.watchDone:
	default:
		t.Fatal("chat watcher survived Close")
	}
	for id, h := range providers {
		if got := h.closeCalls.Load(); got != 1 {
			t.Fatalf("provider %q closed %d times, want exactly once", id, got)
		}
	}
}

// R13: once Close publishes the closed state, every admission path fails with
// the existing closed-runtime errors even while the provider fan-out is still
// running.
func TestRuntimeCloseFencesAdmissionsBeforeFanOut(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	release := make(chan struct{})
	openGates := sync.OnceFunc(func() { close(release) })
	// Any failure must still open the gates so cleanup's Close is never
	// wedged behind a fan-out that only this test can release.
	defer openGates()
	providers["chat"].onClose = func() { <-release }
	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	// Synchronize on the closed state itself: the readiness channel also
	// closes for unrelated watcher notifications, which would race ahead of
	// the publication these assertions target. The gated chat close keeps
	// the fan-out provably in flight once the state is observed.
	deadline := time.Now().Add(3 * time.Second)
	for {
		r.mu.Lock()
		closed := r.closed
		r.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close never published the closed state")
		}
		time.Sleep(time.Millisecond)
	}
	target := r.AcquireRole(RoleDeveloper)
	if _, _, err := ReserveExecution(target, "after-close"); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("reservation after close = %v", err)
	}
	if _, _, err := target.Send(context.Background(), "after-close", "prompt", nil); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("send after close = %v", err)
	}
	if _, err := target.(ModelProvider).Models(context.Background()); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("models after close = %v", err)
	}
	if err := r.Reconcile(context.Background(), closeSpec()); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("reconcile after close = %v", err)
	}
	if ready, _ := r.Available(); ready {
		t.Fatal("closed runtime reports available")
	}
	openGates()
	if err := awaitChange(t, done); err != nil {
		t.Fatal(err)
	}
}

// R14: whole-runtime shutdown does not wait for active turns to complete
// naturally; Close returns while the gated turn is still admitted and the
// adapter terminates it.
func TestRuntimeCloseTerminatesActiveTurnsWithoutWaiting(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	target := r.AcquireRole(RoleChat)
	if _, _, err := target.Send(context.Background(), "gated", "work", nil); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	// The turn is never finished naturally; Close must still return.
	if err := awaitChange(t, done); err != nil {
		t.Fatal(err)
	}
	if !providers["chat"].IsActive("gated") {
		t.Fatal("turn completed naturally before shutdown")
	}
	if providers["chat"].closeCalls.Load() != 1 {
		t.Fatal("adapter was not closed by shutdown")
	}
}
