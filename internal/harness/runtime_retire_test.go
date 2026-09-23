package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// These tests pin C8.3's Reconcile-shutdown interactions: closing the runtime
// unblocks blocked startup/reconciliation work without publishing partial
// topologies, a committed reconciliation finishes its retirement exactly once
// before Close proceeds against the published topology, removed and replaced
// resources retire concurrently, a post-publication retirement failure
// surfaces ErrRetirementIncomplete while keeping the new topology active, and
// no removed provider's readiness watcher outlives its removal.

// R6: Runtime.Close cancels a Reconcile blocked in candidate startup before
// its commit point, keeps the old topology published, cleans the blocked
// candidate exactly once, and then shuts the existing topology down.
func TestRuntimeCloseCancelsBlockedReconcile(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	oldDeveloper := r.providers["developer"].supervisor
	entered := make(chan struct{})
	candidate := newChangeHarness("developer")
	candidate.onStartContext = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	r.registry.Register("developer", func(HarnessConfig) (Harness, error) { return candidate, nil })
	spec := closeSpec()
	spec.Providers["developer"] = HarnessConfig{Name: "developer", Sandbox: "read-only"}
	reconciled := make(chan error, 1)
	go func() { reconciled <- r.Reconcile(context.Background(), spec) }()
	awaitChange(t, entered)
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	waitRuntimeShutdown(t, r)
	if err := awaitChange(t, reconciled); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Reconcile = %v, want context.Canceled", err)
	}
	if r.providers["developer"].supervisor != oldDeveloper {
		t.Fatal("cancelled Reconcile published a new topology")
	}
	if got := oldDeveloper.HarnessConfig().Sandbox; got != "" {
		t.Fatalf("cancelled Reconcile altered published config: %q", got)
	}
	if candidate.closeCalls.Load() != 1 {
		t.Fatalf("blocked candidate closed %d times, want exactly once", candidate.closeCalls.Load())
	}
	if candidate.startCalls.Load() != 1 {
		t.Fatalf("blocked candidate started %d times", candidate.startCalls.Load())
	}
	if err := awaitChange(t, closed); err != nil {
		t.Fatal(err)
	}
	for id, h := range providers {
		if h.closeCalls.Load() != 1 {
			t.Fatalf("provider %q closed %d times, want exactly once", id, h.closeCalls.Load())
		}
	}
}

// R6: Runtime.Close cancels a Start whose provider startup is still blocked,
// so shutdown never waits on startup work that the caller never fenced.
func TestRuntimeCloseCancelsBlockedStart(t *testing.T) {
	entered := make(chan struct{})
	chat := newChangeHarness("chat")
	chat.onStartContext = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	registry := NewRegistry()
	registry.Register("chat", func(HarnessConfig) (Harness, error) { return chat, nil })
	r, err := NewRuntimeSpec(registry, RuntimeSpec{Providers: map[ProviderID]HarnessConfig{"chat": {Name: "chat"}}, Roles: map[Role]ProviderID{RoleChat: "chat"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	started := make(chan error, 1)
	go func() { started <- r.Start(context.Background()) }()
	awaitChange(t, entered)
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	waitRuntimeShutdown(t, r)
	if err := awaitChange(t, started); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Start = %v, want context.Canceled", err)
	}
	if err := awaitChange(t, closed); err != nil {
		t.Fatal(err)
	}
	if chat.closeCalls.Load() != 1 {
		t.Fatalf("blocked-start provider closed %d times, want exactly once", chat.closeCalls.Load())
	}
	if ready, _ := r.Available(); ready {
		t.Fatal("closed runtime reports available")
	}
}

// R7: when Reconcile crosses its commit point first, Close waits for the
// committed retirement to finish before shutting down the published topology.
// The removed provider is retired exactly once and never closed a second time
// by the whole-runtime shutdown.
func TestRuntimeCloseWaitsForCommittedReconcileRetirement(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	entered := make(chan struct{})
	release := make(chan struct{})
	providers["reviewer"].onClose = func() { entered <- struct{}{}; <-release }
	spec := closeSpec()
	delete(spec.Providers, "reviewer")
	spec.Roles[RoleReviewer] = "chat"
	reconciled := make(chan error, 1)
	go func() { reconciled <- r.Reconcile(context.Background(), spec) }()
	awaitChange(t, entered)
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	// Retirement holds the lifecycle reservation, so Close can only be
	// waiting; completing while the gate is closed would mean it abandoned
	// or duplicated the in-flight retirement.
	select {
	case err := <-closed:
		t.Fatalf("Close returned while committed retirement was still running: %v", err)
	default:
	}
	close(release)
	if err := awaitChange(t, reconciled); err != nil {
		t.Fatal(err)
	}
	if err := awaitChange(t, closed); err != nil {
		t.Fatal(err)
	}
	if got := providers["reviewer"].closeCalls.Load(); got != 1 {
		t.Fatalf("removed provider closed %d times, want exactly one retirement", got)
	}
	if got := providers["chat"].closeCalls.Load(); got != 1 {
		t.Fatalf("published chat provider closed %d times, want exactly once", got)
	}
	if got := providers["developer"].closeCalls.Load(); got != 1 {
		t.Fatalf("published developer provider closed %d times, want exactly once", got)
	}
}

// R8: removed providers and the previous harness of a replaced provider are
// independent retirement targets, so all of their closes must enter before any
// of them can finish; a serial retirement would deadlock on the first gate.
func TestRuntimeReconcileRetiresRemovedAndReplacedResourcesConcurrently(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	r.mu.Lock()
	removedDeveloper := r.providers["developer"]
	removedReviewer := r.providers["reviewer"]
	keptChat := r.providers["chat"]
	chatSupervisor := keptChat.supervisor
	r.mu.Unlock()
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	gate := func() { entered <- struct{}{}; <-release }
	providers["developer"].onClose = gate
	providers["reviewer"].onClose = gate
	providers["chat"].onClose = gate
	replacement := newChangeHarness("chat")
	r.registry.Register("chat", func(HarnessConfig) (Harness, error) { return replacement, nil })
	spec := closeSpec()
	delete(spec.Providers, "developer")
	spec.Roles[RoleDeveloper] = "chat"
	delete(spec.Providers, "reviewer")
	spec.Roles[RoleReviewer] = "chat"
	spec.Providers["chat"] = HarnessConfig{Name: "chat", Sandbox: "read-only"}
	reconciled := make(chan error, 1)
	go func() { reconciled <- r.Reconcile(context.Background(), spec) }()
	for index := 0; index < 3; index++ {
		awaitChange(t, entered)
	}
	// Publication precedes retirement: while the old resources are still
	// gated inside their closes, the new role table must already resolve
	// every remapped role to the published chat supervisor, the removed
	// providers must already be absent from the published topology, and the
	// replaced chat supervisor must already carry the replacement adapter.
	// Admissions legitimately wait through the structural fence, which spans
	// the whole reconciliation including retirement, so publication is
	// observed without crossing that fence: role resolution and owned
	// admission are the runtime's real publication paths and neither can
	// reach a retiring resource here. Violations are collected and reported
	// after the gates open so a broken ordering fails the test instead of
	// wedging its cleanup behind a retirement that can never finish.
	var violations []string
	for _, role := range []Role{RoleChat, RoleDeveloper, RoleReviewer} {
		if got := r.AcquireRole(role).(*runtimeTarget).current(); got != chatSupervisor {
			violations = append(violations, fmt.Sprintf("role %v was not published before retirement began", role))
		}
	}
	if _, err := ReserveOwnedExecution(r.AcquireRole(RoleReviewer), "gone", "developer"); !errors.Is(err, ErrProviderAbsent) {
		violations = append(violations, fmt.Sprintf("mid-retirement reservation on removed provider = %v, want ErrProviderAbsent", err))
	}
	chatSupervisor.mu.RLock()
	publishedAdapter := chatSupervisor.current
	chatSupervisor.mu.RUnlock()
	if publishedAdapter != replacement {
		violations = append(violations, "replacement adapter was not published before retirement began")
	}
	// Retirement is provably still in flight: its structural fence still
	// blocks chat admissions while the gated closes hold.
	if ready, _ := r.AcquireRole(RoleChat).(Availability).Available(); ready {
		violations = append(violations, "chat admission escaped the structural fence during retirement")
	}
	close(release)
	if len(violations) > 0 {
		t.Fatal(strings.Join(violations, "; "))
	}
	if err := awaitChange(t, reconciled); err != nil {
		t.Fatal(err)
	}
	for id, target := range map[string]*changeHarness{
		"developer": providers["developer"], "reviewer": providers["reviewer"], "chat": providers["chat"],
	} {
		if got := target.closeCalls.Load(); got != 1 {
			t.Fatalf("retirement target %q closed %d times, want exactly once", id, got)
		}
	}
	if got := replacement.closeCalls.Load(); got != 0 {
		t.Fatalf("published replacement closed %d times during retirement", got)
	}
	for _, removed := range []*providerEntry{removedDeveloper, removedReviewer} {
		select {
		case <-removed.watchDone:
		default:
			t.Fatalf("provider %q watcher survived retirement", removed.id)
		}
	}
	select {
	case <-keptChat.watchDone:
		t.Fatal("kept provider watcher was retired by the replacement")
	default:
	}
	if r.AcquireRole(RoleDeveloper).(*runtimeTarget).current() != r.providers["chat"].supervisor {
		t.Fatal("developer role was not republished")
	}
	if got := bindingSend(t, r.AcquireRole(RoleReviewer), "after-retire", "prompt"); got != "chat-thread" {
		t.Fatal(got)
	}
	replacement.finish("after-retire")
}

// R9: a post-publication retirement failure reports ErrRetirementIncomplete
// with deterministic provider attribution, still waits for every cleanup, and
// leaves the published topology active while the removed topology is absent.
func TestRuntimeRetirementFailureKeepsPublishedTopology(t *testing.T) {
	r, providers := topologyFixture(t, closeSpec())
	boom := errors.New("retirement cleanup failed")
	providers["reviewer"].closeErr = boom
	spec := closeSpec()
	delete(spec.Providers, "developer")
	spec.Roles[RoleDeveloper] = "chat"
	delete(spec.Providers, "reviewer")
	spec.Roles[RoleReviewer] = "chat"
	err := r.Reconcile(context.Background(), spec)
	if err == nil || !errors.Is(err, ErrRetirementIncomplete) {
		t.Fatalf("retirement failure = %v, want ErrRetirementIncomplete", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("retirement failure lost its cause: %v", err)
	}
	if !strings.Contains(err.Error(), `retire provider "reviewer"`) {
		t.Fatalf("retirement failure lost provider identity: %q", err.Error())
	}
	// Retirement still waited for the sibling cleanup before returning.
	if got := providers["developer"].closeCalls.Load(); got != 1 {
		t.Fatalf("sibling removed provider closed %d times, want exactly once", got)
	}
	if got := providers["reviewer"].closeCalls.Load(); got != 1 {
		t.Fatalf("failing removed provider closed %d times, want exactly once", got)
	}
	// The requested topology stays published and active.
	if _, ok := r.providers["reviewer"]; ok {
		t.Fatal("removed provider remained in the published topology")
	}
	if _, ok := r.providers["developer"]; ok {
		t.Fatal("removed provider remained in the published topology")
	}
	if got := r.AcquireRole(RoleReviewer).(*runtimeTarget).current(); got != r.providers["chat"].supervisor {
		t.Fatal("reviewer role was not republished to the chat provider")
	}
	if got := bindingSend(t, r.AcquireRole(RoleDeveloper), "after-failure", "prompt"); got != "chat-thread" {
		t.Fatal(got)
	}
	providers["chat"].finish("after-failure")
	// The removed topology is gone: owned work pinned to it fails closed.
	if _, err := ReserveOwnedExecution(r.AcquireRole(RoleReviewer), "gone", "reviewer"); !errors.Is(err, ErrProviderAbsent) {
		t.Fatalf("owned reservation on removed provider = %v, want ErrProviderAbsent", err)
	}
}

// R12: removing a provider joins its readiness watcher before Reconcile
// returns, and the retired watcher can no longer forward readiness changes.
func TestRuntimeReconcileJoinsRemovedProviderWatcher(t *testing.T) {
	r, _ := topologyFixture(t, closeSpec())
	r.mu.Lock()
	removed := r.providers["reviewer"]
	r.mu.Unlock()
	spec := closeSpec()
	delete(spec.Providers, "reviewer")
	spec.Roles[RoleReviewer] = "chat"
	if err := r.Reconcile(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	select {
	case <-removed.watchDone:
	default:
		t.Fatal("removed provider watcher survived Reconcile")
	}
	version, _ := r.Readiness()
	removed.supervisor.mu.Lock()
	removed.supervisor.broadcastReadyLocked()
	removed.supervisor.mu.Unlock()
	if current, _ := r.Readiness(); current != version {
		t.Fatal("retired watcher mutated runtime readiness")
	}
}
