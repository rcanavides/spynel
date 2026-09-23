package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/workspace"
)

type fakeHarness struct {
	mu         sync.Mutex
	calls      int
	keys       []string
	prompts    []string
	threads    map[string]string
	active     map[string]bool
	beforeEmit func()
	events     []core.Event
}

func newFakeRecipient() *fakeHarness {
	return &fakeHarness{threads: map[string]string{}, active: map[string]bool{}}
}

func (f *fakeHarness) Start(context.Context) error { return nil }
func (f *fakeHarness) Close() error                { return nil }
func (f *fakeHarness) ResetSession(key string) error {
	delete(f.threads, key)
	return nil
}
func (f *fakeHarness) ThreadID(key string) string { return f.threads[key] }
func (f *fakeHarness) IsActive(key string) bool   { return f.active[key] }
func (f *fakeHarness) Interrupt(_ context.Context, key string) (bool, error) {
	if !f.active[key] {
		return false, nil
	}
	f.active[key] = false
	return true, nil
}
func (f *fakeHarness) Send(_ context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	f.mu.Lock()
	f.calls++
	f.keys = append(f.keys, key)
	f.prompts = append(f.prompts, prompt)
	thread := f.threads[key]
	if thread == "" {
		thread = "thread-" + key
		f.threads[key] = thread
	}
	f.mu.Unlock()
	if f.beforeEmit != nil && !strings.HasPrefix(key, "orchestrator:notification:") {
		f.beforeEmit()
	}
	if f.events != nil {
		for _, event := range f.events {
			event.ThreadID = thread
			emit(event)
		}
	} else {
		emit(core.Event{Kind: core.EventFinal, Text: "done", ThreadID: thread, Done: true})
	}
	return thread, false, nil
}

func TestLiveParallelLimitRaisesPromptlyAndLowersWithoutCancelling(t *testing.T) {
	cfg := config.Default()
	cfg.Orchestrator.MaxParallel = 1
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !manager.acquireCapacity(ctx) {
		t.Fatal("initial capacity was unavailable")
	}
	acquired := make(chan struct{})
	go func() {
		if manager.acquireCapacity(ctx) {
			close(acquired)
		}
	}()
	select {
	case <-acquired:
		t.Fatal("limit one admitted overlapping work")
	case <-time.After(25 * time.Millisecond):
	}
	cfg.Orchestrator.MaxParallel = 2
	manager.ApplyRuntimeConfig(cfg)
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("raised limit did not admit waiting work promptly")
	}
	cfg.Orchestrator.MaxParallel = 1
	manager.ApplyRuntimeConfig(cfg)
	third := make(chan struct{})
	go func() {
		if manager.acquireCapacity(ctx) {
			close(third)
		}
	}()
	manager.releaseCapacity()
	select {
	case <-third:
		t.Fatal("lowered limit admitted work while active count remained at the bound")
	case <-time.After(25 * time.Millisecond):
	}
	manager.releaseCapacity()
	select {
	case <-third:
	case <-time.After(time.Second):
		t.Fatal("waiting work was not admitted after active work drained below the new bound")
	}
	manager.releaseCapacity()
}

// H1: the temporary single-writer gate serializes route-scanned workflow
// dispatches. While one workflow turn holds the writer slot inside its
// provider turn, a second claimed workflow cannot start its own provider
// turn; both complete once the first releases.
func TestWorkflowWriterGateSerializesConcurrentRouteDispatches(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "first writer task", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "second writer task", ""); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	unblock := make(chan struct{})
	fake.beforeEmit = func() { <-unblock }
	manager := New(cfg, fake, extensions.Runner{})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	fake.mu.Lock()
	calls := fake.calls
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("in-flight provider turns = %d, want exactly one while it holds the writer gate", calls)
	}
	close(unblock)
	manager.Wait()
	if fake.calls != 2 {
		t.Fatalf("dispatch calls = %d, want 2 after both serialized turns completed", fake.calls)
	}
}

// H2: a workflow parked on the writer gate must not consume a max_parallel
// capacity slot. With the writer slot held and max_parallel reduced to one,
// the parked dispatch leaves the single capacity slot free for other work.
func TestWorkflowWriterWaitHoldsNoCapacitySlot(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Orchestrator.MaxParallel = 1
	if _, err := Create(cfg, "tasks", "writer capacity task", ""); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	if !manager.acquireWorkflowWriter(context.Background()) {
		t.Fatal("writer ownership was unavailable")
	}
	manager.dispatch(context.Background(), workflowRoutes()[0], leases[0], true)
	time.Sleep(50 * time.Millisecond)
	capacityCtx, cancelCapacity := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCapacity()
	if !manager.acquireCapacity(capacityCtx) {
		t.Fatal("workflow parked on the writer gate consumed a capacity slot")
	}
	fake.mu.Lock()
	calls := fake.calls
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("dispatch calls = %d, want 1 while the gated turn waits for writer ownership", calls)
	}
	manager.releaseCapacity()
	manager.releaseWorkflowWriter()
	manager.Wait()
	if fake.calls != 2 {
		t.Fatalf("dispatch calls = %d, want 2 after the gated turn completed", fake.calls)
	}
}

// H3: ownership pairing. A waiter cancelled while parked on the writer gate
// returns false without acquiring ownership, and every released ownership
// admits exactly one parked waiter.
func TestWorkflowWriterCancelWhileWaitingKeepsOwnershipPairing(t *testing.T) {
	cfg := config.Default()
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !manager.acquireWorkflowWriter(ctx) {
		t.Fatal("initial writer ownership was unavailable")
	}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	waiterDone := make(chan bool, 1)
	go func() { waiterDone <- manager.acquireWorkflowWriter(waitCtx) }()
	select {
	case acquired := <-waiterDone:
		if acquired {
			t.Fatal("waiter admitted while the single writer slot was held")
		}
		t.Fatal("waiter returned before cancellation")
	case <-time.After(25 * time.Millisecond):
	}
	cancelWait()
	select {
	case acquired := <-waiterDone:
		if acquired {
			t.Fatal("cancelled waiter reported acquiring writer ownership")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter never returned")
	}
	manager.releaseWorkflowWriter()
	if !manager.acquireWorkflowWriter(ctx) {
		t.Fatal("release did not return writer ownership")
	}
	parked := make(chan struct{})
	go func() {
		if manager.acquireWorkflowWriter(ctx) {
			close(parked)
		}
	}()
	select {
	case <-parked:
		t.Fatal("cancelled waiter leaked an ownership slot")
	case <-time.After(25 * time.Millisecond):
	}
	manager.releaseWorkflowWriter()
	select {
	case <-parked:
	case <-time.After(time.Second):
		t.Fatal("release did not admit the parked waiter")
	}
	manager.releaseWorkflowWriter()
}

// ordinaryTurnGateHarness records one ordinary agent turn with explicit
// admission and release channels so a test observes the exact in-flight
// window without timing assumptions.
type ordinaryTurnGateHarness struct {
	mu       sync.Mutex
	prompts  []string
	admitted chan struct{}
	release  chan struct{}
}

func (h *ordinaryTurnGateHarness) Start(context.Context) error { return nil }
func (h *ordinaryTurnGateHarness) Close() error                { return nil }
func (h *ordinaryTurnGateHarness) ResetSession(string) error   { return nil }
func (h *ordinaryTurnGateHarness) ThreadID(string) string      { return "thread-ordinary" }
func (h *ordinaryTurnGateHarness) IsActive(string) bool        { return false }
func (h *ordinaryTurnGateHarness) Interrupt(context.Context, string) (bool, error) {
	return false, nil
}

func (h *ordinaryTurnGateHarness) Send(_ context.Context, _ string, prompt string, emit core.Emit) (string, bool, error) {
	h.mu.Lock()
	h.prompts = append(h.prompts, prompt)
	h.mu.Unlock()
	close(h.admitted)
	<-h.release
	emit(core.Event{Kind: core.EventFinal, Text: "ordinary turn complete", ThreadID: "thread-ordinary", Done: true})
	return "thread-ordinary", false, nil
}

// H4: ordinary notification and heartbeat turns never pass through the
// workflow writer gate and never join workflow inflight accounting. While
// the single writer slot is held by a route-scanned workflow writer, both
// ordinary turns still admit and run to completion.
func TestOrdinaryTurnsProceedWhileWorkflowWriterHeld(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.StatePath("tasks", "done"), "task.md")
	document := Document{FrontMatter: map[string]any{
		"id": "task-h4", "title": "Publish report", "status": "done", "attempt": 1,
		"notify": map[string]any{"enabled": true, "origin": "tui/local", "on": []any{"done"}},
	}, Body: "# Publish report\n\n## Progress\n\n- 2026-08-09T11:00:00Z — Report published.\n"}
	if err := WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
	notificationTarget := &ordinaryTurnGateHarness{admitted: make(chan struct{}), release: make(chan struct{})}
	heartbeatTarget := &ordinaryTurnGateHarness{admitted: make(chan struct{}), release: make(chan struct{})}
	fallback := newFakeRecipient()
	manager := New(cfg, fallback, extensions.Runner{})
	manager.HarnessRouter = harness.NewStaticRoleRouter(fallback, map[harness.Role]harness.Harness{
		harness.RoleNotification: notificationTarget,
		harness.RoleHeartbeat:    heartbeatTarget,
	})
	manager.SetPrimaryOwned(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !manager.acquireWorkflowWriter(ctx) {
		t.Fatal("writer ownership was unavailable")
	}
	defer manager.releaseWorkflowWriter()

	manager.startTaskNotificationAgent(ctx, Lease{ID: "lease-h4-notify", Route: "tasks", Phase: phaseTaskImplementation, ClaimAttempt: 1}, "done", path)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		manager.runSemanticHeartbeatOnce(ctx)
	}()

	select {
	case <-notificationTarget.admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("notification ordinary turn did not proceed while the workflow writer gate was held")
	}
	select {
	case <-heartbeatTarget.admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat ordinary turn did not proceed while the workflow writer gate was held")
	}

	// Neither ordinary turn consumed the writer slot: the test still owns
	// the only admission.
	manager.writerMu.Lock()
	writerActive := manager.writerActive
	manager.writerMu.Unlock()
	if writerActive != 1 {
		t.Fatalf("ordinary turns changed workflow writer ownership: active = %d, want 1 held by this test", writerActive)
	}
	// Neither ordinary turn participates in workflow inflight accounting.
	manager.mu.Lock()
	inflight := len(manager.inflight)
	manager.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("ordinary turns entered workflow inflight accounting: %d entries", inflight)
	}
	_, active, err := manager.Status()
	if err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("workflow inflight status = %d while only ordinary turns run", active)
	}

	close(notificationTarget.release)
	close(heartbeatTarget.release)
	manager.Wait()
	select {
	case <-heartbeatDone:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat ordinary turn did not finish after its provider release")
	}
	notificationTarget.mu.Lock()
	notificationCalls := len(notificationTarget.prompts)
	notificationTarget.mu.Unlock()
	if notificationCalls != 1 {
		t.Fatalf("notification provider turns = %d, want exactly 1", notificationCalls)
	}
	heartbeatTarget.mu.Lock()
	heartbeatCalls := len(heartbeatTarget.prompts)
	heartbeatTarget.mu.Unlock()
	if heartbeatCalls != 1 {
		t.Fatalf("heartbeat provider turns = %d, want exactly 1", heartbeatCalls)
	}
	// The writer slot is still fully held by this test alone: ordinary turns
	// never released ownership they did not acquire.
	probeCtx, cancelProbe := context.WithCancel(context.Background())
	probeDone := make(chan bool, 1)
	go func() { probeDone <- manager.acquireWorkflowWriter(probeCtx) }()
	select {
	case acquired := <-probeDone:
		if acquired {
			t.Fatal("ordinary turns released the workflow writer slot")
		}
		t.Fatal("writer probe returned before cancellation")
	case <-time.After(50 * time.Millisecond):
	}
	cancelProbe()
	select {
	case acquired := <-probeDone:
		if acquired {
			t.Fatal("cancelled writer probe acquired ownership")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled writer probe never returned")
	}
}

// entryTrackerHarness records every workflow provider-turn entry and parks
// each entered turn on one shared release channel.
type entryTrackerHarness struct {
	mu      sync.Mutex
	entered []string
	entries chan string
	unblock chan struct{}
}

func (h *entryTrackerHarness) Start(context.Context) error { return nil }
func (h *entryTrackerHarness) Close() error                { return nil }
func (h *entryTrackerHarness) ResetSession(string) error   { return nil }
func (h *entryTrackerHarness) ThreadID(key string) string  { return "thread-" + key }
func (h *entryTrackerHarness) IsActive(string) bool        { return false }
func (h *entryTrackerHarness) Interrupt(context.Context, string) (bool, error) {
	return false, nil
}

func (h *entryTrackerHarness) Send(_ context.Context, key string, _ string, emit core.Emit) (string, bool, error) {
	h.mu.Lock()
	h.entered = append(h.entered, key)
	h.mu.Unlock()
	h.entries <- key
	<-h.unblock
	emit(core.Event{Kind: core.EventFinal, Text: "done", ThreadID: key, Done: true})
	return key, false, nil
}

// H6: the temporary writer gate covers the durable recovery path as well as
// fresh route-scanned claims. One resumed interrupted claim and one fresh
// scan claim both reach the shared writer serialization path; exactly one
// reaches provider execution while the first holds the slot, and the other
// proceeds after release.
func TestWorkflowWriterGateSerializesRecoveryWithFreshClaim(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	target := &entryTrackerHarness{entries: make(chan string, 2), unblock: make(chan struct{})}
	manager := New(cfg, target, extensions.Runner{})

	// Recovery workflow: an interrupted claim persisted as a claiming lease
	// beside its already-claimed working document, exactly as a crashed
	// process would leave it for resumeInterruptedClaims.
	recoveryID := "tasks-h6-recovery"
	recoveryPath := filepath.Join(cfg.StatePath("tasks", "working"), "recovery-task.md")
	recoveryDocument := Document{FrontMatter: map[string]any{
		"id": recoveryID, "title": "Writer recovery task", "status": "working", "attempt": 2,
	}, Body: "# Writer recovery task\n\n## Progress\n\n- 2026-08-09T11:00:00Z — Claimed before the interruption.\n"}
	if err := WriteDocument(recoveryPath, recoveryDocument); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	recoveryKey := leaseID("tasks:"+phaseTaskImplementation, recoveryID)
	recoverySession := phaseSessionKey("tasks", recoveryID, phaseTaskImplementation, 2)
	if err := manager.saveLease(Lease{
		ID: recoveryKey, ClaimID: recoveryKey, DocumentType: "task", Route: "tasks",
		File: recoveryPath, SessionKey: recoverySession, State: "claiming",
		Phase: phaseTaskImplementation, ClaimAttempt: 2, StartedAt: now, HeartbeatAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	// Fresh workflow: an ordinary unclaimed todo document.
	freshPath, err := Create(cfg, "tasks", "writer fresh task", "")
	if err != nil {
		t.Fatal(err)
	}
	freshDocument, err := ReadDocument(freshPath)
	if err != nil {
		t.Fatal(err)
	}
	freshID := documentID(freshDocument)
	freshSession := phaseSessionKey("tasks", freshID, phaseTaskImplementation, 1)

	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := awaitEntry(t, target.entries, "no workflow execution entered its provider turn")
	manager.writerMu.Lock()
	writerActive := manager.writerActive
	manager.writerMu.Unlock()
	if writerActive != 1 {
		t.Fatalf("first gated execution did not hold the single writer slot: active = %d", writerActive)
	}
	// Exactly one reaches execution while the first is gated.
	select {
	case second := <-target.entries:
		t.Fatalf("workflow execution %q reached the provider while the first execution held the writer gate", second)
	case <-time.After(100 * time.Millisecond):
	}
	target.mu.Lock()
	entered := len(target.entered)
	target.mu.Unlock()
	if entered != 1 {
		t.Fatalf("provider turns entered = %d, want exactly one while the writer gate is held", entered)
	}
	if first != recoverySession && first != freshSession {
		t.Fatalf("unexpected first execution session %q", first)
	}
	// After release the other proceeds.
	close(target.unblock)
	second := awaitEntry(t, target.entries, "the waiting workflow execution did not proceed after the writer slot was released")
	if second == first {
		t.Fatalf("the same execution %q re-entered the provider instead of the waiting workflow proceeding", second)
	}
	manager.Wait()
	target.mu.Lock()
	keys := append([]string(nil), target.entered...)
	target.mu.Unlock()
	if len(keys) != 2 {
		t.Fatalf("provider turns entered = %d, want the recovery and fresh executions", len(keys))
	}
	got := map[string]bool{keys[0]: true, keys[1]: true}
	if !got[recoverySession] || !got[freshSession] {
		t.Fatalf("writer path sessions = %v, want recovery %q and fresh %q", keys, recoverySession, freshSession)
	}
	recoveryLease, err := manager.loadLease(recoveryKey)
	if err != nil {
		t.Fatal(err)
	}
	if recoveryLease.RecoveryCount < 1 {
		t.Fatalf("recovery execution did not run the durable recovery path: lease = %#v", recoveryLease)
	}
}

func awaitEntry(t *testing.T, entries <-chan string, message string) string {
	t.Helper()
	select {
	case key := <-entries:
		return key
	case <-time.After(2 * time.Second):
		t.Fatal(message)
		return ""
	}
}

func TestLiveScanIntervalResetsRunningSchedulerFromAcceptedChange(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Orchestrator.IntervalSec = 3600
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- manager.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-runDone
	})
	deadline := time.Now().Add(time.Second)
	for {
		manager.scanTimerMu.Lock()
		running := manager.scanTimer != nil
		manager.scanTimerMu.Unlock()
		if running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("route scheduler did not start")
		}
		time.Sleep(time.Millisecond)
	}
	cfg.Orchestrator.IntervalSec = 2
	before := time.Now()
	manager.ApplyRuntimeConfig(cfg)
	after := time.Now()
	manager.scanTimerMu.Lock()
	next := manager.scanNext
	manager.scanTimerMu.Unlock()
	if next.Before(before.Add(2*time.Second)) || next.After(after.Add(2*time.Second)) {
		t.Fatalf("next scan deadline %s was not measured from acceptance window [%s, %s]", next, before.Add(2*time.Second), after.Add(2*time.Second))
	}
	if got := manager.scanInterval(); got != 2*time.Second {
		t.Fatalf("live scan interval = %s", got)
	}
}

func TestImplementationDoneMoveIsReconciledIntoIndependentReview(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	task, err := Create(cfg, "tasks", "move and release", "")
	if err != nil {
		t.Fatal(err)
	}
	working := filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), filepath.Base(task))
	done := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "done", filepath.Base(task))
	fake := newFakeRecipient()
	var manager *Manager
	var moved sync.Once
	fake.beforeEmit = func() {
		moved.Do(func() {
			if err := os.Rename(working, done); err != nil {
				t.Errorf("move completed task: %v", err)
			}
			if err := manager.recoverStale(context.Background()); err != nil {
				t.Errorf("recover after completed move: %v", err)
			}
		})
	}
	manager = New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	started := 0
	finished := 0
	var attempts []int
	manager.JobStarted = func(lease Lease, description string, _ time.Time, _ int, implementationAttempts int) (int, error) {
		if lease.SessionKey == "" || description == "" {
			t.Errorf("empty orchestrator job metadata: %q %q", lease.SessionKey, description)
		}
		attempts = append(attempts, implementationAttempts)
		started++
		return 7, nil
	}
	manager.JobFinished = func(id int) {
		if id != 7 {
			t.Errorf("finished job ID = %d, want 7", id)
		}
		finished++
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 || leases[0].Phase != phaseTaskReview {
		t.Fatalf("task did not enter review phase: %#v, %v", leases, err)
	}
	reviewing := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "reviewing", filepath.Base(task))
	if _, err := os.Stat(reviewing); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(done); !os.IsNotExist(err) {
		t.Fatalf("unreviewed task remained done: %v", err)
	}
	if started != 2 || finished != 1 {
		t.Fatalf("job lifecycle callbacks = %d started, %d finished", started, finished)
	}
	if !reflect.DeepEqual(attempts, []int{1, 1}) {
		t.Fatalf("implementation-attempt snapshots = %v, want [1 1]", attempts)
	}
	claimed, err := ReadDocument(reviewing)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.FrontMatter["attempt"] != 1 || claimed.FrontMatter["review_attempt"] != 1 {
		t.Fatalf("review claim counters = %#v, want one implementation and one review", claimed.FrontMatter)
	}
}

func TestProviderCompletionRemainsAwaitingTransitionUntilDurableMove(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	task, err := Create(cfg, "tasks", "await durable move", "")
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	fake.events = []core.Event{
		{Kind: core.EventStatus, Execution: &core.ExecutionStatus{State: "reconnecting", ReconnectAttempt: 1}},
		{Kind: core.EventStatus, Execution: &core.ExecutionStatus{State: "running"}},
		{Kind: core.EventFinal, Text: "done", Done: true, Execution: &core.ExecutionStatus{State: "finishing"}},
	}
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	started, finished := 0, 0
	var states []string
	var providerStates []string
	manager.JobStarted = func(lease Lease, _ string, _ time.Time, _, _ int) (int, error) {
		if lease.DocumentType == "notification" {
			return 12, nil
		}
		started++
		return 11, nil
	}
	manager.JobUpdated = func(_ int, lease Lease) { states = append(states, lease.State) }
	manager.JobExecutionUpdated = func(_ int, status core.ExecutionStatus) { providerStates = append(providerStates, status.State) }
	manager.JobFinished = func(id int) {
		if id == 12 {
			return
		}
		if id != 11 {
			t.Errorf("finished ID=%d", id)
		}
		finished++
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	working := filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), filepath.Base(task))
	if started != 1 || finished != 0 || len(states) == 0 || states[len(states)-1] != "awaiting_transition" || !reflect.DeepEqual(providerStates, []string{"reconnecting", "running"}) {
		t.Fatalf("before transition: started=%d finished=%d lease=%v provider=%v", started, finished, states, providerStates)
	}
	waiting := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "waiting", filepath.Base(task))
	if err := moveDocument(working, waiting, "waiting", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if finished != 1 {
		t.Fatalf("durable reconciliation finished jobs %d times", finished)
	}
}

func TestClaimLeasePreventsDuplicatesAndStaleLeaseRecovers(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	task, err := Create(cfg, "tasks", "verify lease behavior", "")
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	working := filepath.Join(cfg.Resolve(workflowRoutes()[0].Working), filepath.Base(task))
	if _, err := os.Stat(working); err != nil {
		t.Fatalf("task was not claimed into working: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("dispatch calls = %d, want 1", fake.calls)
	}
	document, err := ReadDocument(working)
	if err != nil {
		t.Fatal(err)
	}
	firstAssigned, iterations, ok := DurableTiming(document)
	if !ok || firstAssigned.IsZero() || iterations != 1 {
		t.Fatalf("initial durable timing = %s %d %t", firstAssigned, iterations, ok)
	}
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 1 {
		t.Fatalf("leased task was duplicated: %d calls", fake.calls)
	}
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	lease := leases[0]
	lease.HeartbeatAt = time.Now().Add(-time.Hour)
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	fake.active[lease.SessionKey] = true
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 1 {
		t.Fatalf("active harness session received duplicate stale recovery: %d calls", fake.calls)
	}
	fake.active[lease.SessionKey] = false
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 2 {
		t.Fatalf("stale task was not recovered: %d calls", fake.calls)
	}
	document, err = ReadDocument(working)
	if err != nil {
		t.Fatal(err)
	}
	recoveredFirst, iterations, ok := DurableTiming(document)
	if !ok || !recoveredFirst.Equal(firstAssigned) || iterations != 2 {
		t.Fatalf("recovered durable timing = %s %d %t", recoveredFirst, iterations, ok)
	}
	recovered, err := manager.loadLease(lease.ID)
	if err != nil || recovered.RecoveryCount != 1 || recovered.State != "awaiting_transition" {
		t.Fatalf("recovered lease = %#v, %v", recovered, err)
	}
}

func TestRecoveryDispatchStateTracksProviderLifecycle(t *testing.T) {
	for _, test := range []struct {
		name, priorState, wantState, wantError string
		events                                 []core.Event
	}{
		{name: "admission", priorState: "error", wantState: "processing", events: []core.Event{}},
		{name: "running", priorState: "awaiting_transition", wantState: "processing", events: []core.Event{
			{Kind: core.EventStatus, Execution: &core.ExecutionStatus{State: "running"}},
			{Kind: core.EventDelta, Text: "resumed work"},
		}},
		{name: "readmitted", priorState: "recovering", wantState: "processing", events: []core.Event{}},
		{name: "completed", priorState: "processing", wantState: "awaiting_transition", events: []core.Event{
			{Kind: core.EventFinal, Done: true},
		}},
		{name: "failed", priorState: "error", wantState: "awaiting_transition", wantError: "new failure", events: []core.Event{
			{Kind: core.EventError, Text: "new failure", Done: true},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := workspace.Init(root, false); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(config.PathForRoot(root))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Create(cfg, "tasks", "recovery state", ""); err != nil {
				t.Fatal(err)
			}
			fake := newFakeRecipient()
			manager := New(cfg, fake, extensions.Runner{})
			if err := manager.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager.Wait()
			leases, err := manager.loadLeases()
			if err != nil || len(leases) != 1 {
				t.Fatalf("leases = %#v, %v", leases, err)
			}
			lease := leases[0]
			lease.State, lease.LastError = test.priorState, "previous failure"
			if err := manager.saveLease(lease); err != nil {
				t.Fatal(err)
			}
			fake.events = test.events
			manager.JobStarted = func(current Lease, _ string, _ time.Time, _, _ int) (int, error) {
				if current.State != "recovering" || current.LastError != "" {
					t.Errorf("recovery admission retained previous state: %#v", current)
				}
				return 1, nil
			}
			var updates []Lease
			manager.JobUpdated = func(_ int, current Lease) { updates = append(updates, current) }
			manager.dispatch(context.Background(), workflowRoutes()[0], lease, true)
			manager.Wait()
			current, err := manager.loadLease(lease.ID)
			if err != nil || current.State != test.wantState || current.LastError != test.wantError || current.RecoveryCount != 1 {
				t.Fatalf("recovered lease = %#v, %v", current, err)
			}
			if len(updates) == 0 || !reflect.DeepEqual(updates[len(updates)-1], current) {
				t.Fatalf("job updates did not publish the persisted lease: %#v", updates)
			}
			if test.name == "running" {
				for _, update := range updates {
					if update.State != "processing" {
						t.Fatalf("provider activity retained recovery state: %#v", update)
					}
				}
			}
		})
	}
}

func TestFutureDispatchTimeDefersGoalPlanning(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	goal, err := Create(cfg, "goals", "review this later", "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(goal)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["next_dispatch_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if err := WriteDocument(goal, document); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRecipient()
	manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.calls != 0 {
		t.Fatalf("future goal dispatched %d times", fake.calls)
	}
	if _, err := os.Stat(goal); err != nil {
		t.Fatalf("deferred goal moved unexpectedly: %v", err)
	}
}

func TestFutureCheckpointDoesNotDeferImplementationOrPlanning(t *testing.T) {
	for _, route := range []string{"tasks", "goals"} {
		t.Run(route, func(t *testing.T) {
			root := t.TempDir()
			if err := workspace.Init(root, false); err != nil {
				t.Fatal(err)
			}
			cfg, _ := config.Load(config.PathForRoot(root))
			path, err := Create(cfg, route, "checkpoint belongs to active goals", "")
			if err != nil {
				t.Fatal(err)
			}
			document, err := ReadDocument(path)
			if err != nil {
				t.Fatal(err)
			}
			document.FrontMatter["next_review_at"] = time.Now().Add(7 * 24 * time.Hour).UTC().Format(time.RFC3339)
			if err := WriteDocument(path, document); err != nil {
				t.Fatal(err)
			}
			fake := newFakeRecipient()
			manager := New(cfg, fake, extensions.Runner{Directory: filepath.Join(root, "missing")})
			if err := manager.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager.Wait()
			if fake.calls != 1 {
				t.Fatalf("%s dispatches = %d, want 1", route, fake.calls)
			}
		})
	}
}

func TestReviewTransitionAcceptRejectAndSelfReviewGuard(t *testing.T) {
	for _, test := range []struct {
		name, target, want string
		sameThread         bool
	}{{"accept", "done", "done", false}, {"reject", "todo", "todo", false}, {"self-review", "done", "todo", true}} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := workspace.Init(root, false); err != nil {
				t.Fatal(err)
			}
			cfg, _ := config.Load(config.PathForRoot(root))
			route := workflowRoutes()[0]
			reviewDir := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "review")
			path := filepath.Join(filepath.Dir(reviewDir), "reviewing", "review.md")
			doc := Document{FrontMatter: map[string]any{"id": "review-id", "title": "review", "status": "reviewing", "created_at": time.Now().UTC().Format(time.RFC3339), "updated_at": time.Now().UTC().Format(time.RFC3339), "review_attempt": 1, "notify": map[string]any{"enabled": false}}, Body: "# review\n"}
			if err := WriteDocument(path, doc); err != nil {
				t.Fatal(err)
			}
			thread := "review-thread"
			implementer := "implementation-thread"
			if test.sameThread {
				implementer = thread
			}
			manager := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
			lease := Lease{ID: "lease", Route: "tasks", File: path, SessionKey: "review-session", Phase: phaseTaskReview, ClaimAttempt: 1, ThreadID: thread, ImplementerThread: implementer, StartedAt: time.Now(), HeartbeatAt: time.Now()}
			if err := manager.saveLease(lease); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), test.target, filepath.Base(path))
			if err := os.Rename(path, target); err != nil {
				t.Fatal(err)
			}
			if err := manager.reconcileTransitions(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), test.want, filepath.Base(path))
			if _, err := os.Stat(want); err != nil {
				t.Fatalf("wanted %s: %v", want, err)
			}
			if manager.leaseExists(lease.ID) {
				t.Fatal("terminal review retained lease")
			}
		})
	}
}

func TestReviewDispatchUsesFreshAttemptSessionAcrossRestart(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	route := workflowRoutes()[0]
	path := filepath.Join(filepath.Dir(cfg.Resolve(route.Source)), "review", "retry.md")
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "retry", "title": "retry", "status": "review", "created_at": time.Now().UTC().Format(time.RFC3339), "updated_at": time.Now().UTC().Format(time.RFC3339)}, Body: "# retry\n"}); err != nil {
		t.Fatal(err)
	}
	first := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	if err := first.scanPhaseQueue(context.Background(), route, filepath.Dir(path), filepath.Join(filepath.Dir(filepath.Dir(path)), "reviewing"), phaseTaskReview); err != nil {
		t.Fatal(err)
	}
	first.Wait()
	leases, _ := first.loadLeases()
	if len(leases) != 1 || !strings.HasSuffix(leases[0].SessionKey, ":1") {
		t.Fatalf("first review lease = %#v", leases)
	}
	restarted := New(cfg, newFakeRecipient(), extensions.Runner{Directory: filepath.Join(root, "missing")})
	if err := restarted.scanPhaseQueue(context.Background(), route, filepath.Dir(path), filepath.Join(filepath.Dir(filepath.Dir(path)), "reviewing"), phaseTaskReview); err != nil {
		t.Fatal(err)
	}
	restarted.Wait()
	leases, _ = restarted.loadLeases()
	if len(leases) != 1 {
		t.Fatalf("restart duplicated review: %#v", leases)
	}
}

func TestControlReservationsPersistAndRefreshRegisteredDurableJob(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".spynel", "tasks", "working", "control.md")
	assigned := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{
		"id": "control", "status": "working", "first_assigned_at": assigned.Format(time.RFC3339), "provider_iterations": 1,
	}, Body: "# Control\n"}); err != nil {
		t.Fatal(err)
	}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	lease := Lease{ID: "control-lease", OwnerID: manager.ownerID, Route: "tasks", File: path, SessionKey: "orchestrator:control", State: "processing", Phase: phaseTaskImplementation, StartedAt: assigned, HeartbeatAt: assigned}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	manager.setRuntimeJob(lease.ID, 7)
	var observed []int
	manager.JobTimingUpdated = func(id int, first time.Time, iterations int) {
		if id != 7 || !first.Equal(assigned) {
			t.Errorf("timing update = id %d first %s", id, first)
		}
		observed = append(observed, iterations)
	}
	for range 2 {
		if !manager.ReserveControlProviderTurn(lease, "control") {
			t.Fatal("control provider-turn reservation failed")
		}
	}
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	first, iterations, ok := DurableTiming(document)
	if !ok || !first.Equal(assigned) || iterations != 3 || !reflect.DeepEqual(observed, []int{2, 3}) {
		t.Fatalf("control timing = %s %d %t, updates %#v", first, iterations, ok, observed)
	}
}

func TestPrepareControlContinuationRevalidatesLeaseOwnerAndDurableState(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	target := newFakeRecipient()
	manager := New(cfg, target, extensions.Runner{})
	path := filepath.Join(cfg.StatePath("tasks", "working"), "control.md")
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "control", "status": "working"}, Body: "# Control\n"}); err != nil {
		t.Fatal(err)
	}
	lease := Lease{ID: "control-lease", OwnerID: manager.ownerID, SessionKey: "orchestrator:control", File: path, Route: "tasks", Phase: phaseTaskImplementation, State: "awaiting_transition", StartedAt: time.Now().UTC()}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	manager.setRuntimeJob(lease.ID, 1)
	target.active[lease.SessionKey] = true
	if !manager.PrepareControlContinuation(lease, "control") {
		t.Fatal("valid continuation was refused")
	}
	current, err := manager.loadLease(lease.ID)
	if err != nil || current.State != "processing" {
		t.Fatalf("continued lease = %#v, %v", current, err)
	}
	firstReservation := manager.MarkControlCancellation(lease.SessionKey)
	secondReservation := manager.MarkControlCancellation(lease.SessionKey)
	if manager.PrepareControlContinuation(lease, "control") {
		t.Fatal("cancelled control was allowed to continue")
	}
	manager.RestoreControlCancellation(firstReservation)
	if manager.PrepareControlContinuation(lease, "control") {
		t.Fatal("one rollback cleared another cancellation reservation")
	}
	manager.RestoreControlCancellation(secondReservation)
	if !manager.ControlStillValid(lease, "control") {
		t.Fatal("all rejected cancellation reservations did not restore control")
	}
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "replacement", "status": "working"}, Body: "# Replacement\n"}); err != nil {
		t.Fatal(err)
	}
	if manager.ControlStillValid(lease, "control") {
		t.Fatal("replacement document identity was accepted")
	}
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "control", "status": "reviewing"}, Body: "# Wrong phase\n"}); err != nil {
		t.Fatal(err)
	}
	if manager.ControlStillValid(lease, "control") {
		t.Fatal("phase/status mismatch was accepted")
	}
	if err := WriteDocument(path, Document{FrontMatter: map[string]any{"id": "control", "status": "working"}, Body: "# Control\n"}); err != nil {
		t.Fatal(err)
	}
	changedOwner := lease
	changedOwner.OwnerID = "stale-owner"
	if manager.PrepareControlContinuation(changedOwner, "control") {
		t.Fatal("changed owner was allowed to continue")
	}
	if err := os.Rename(path, filepath.Join(cfg.StatePath("tasks", "waiting"), filepath.Base(path))); err != nil {
		t.Fatal(err)
	}
	if manager.PrepareControlContinuation(lease, "control") {
		t.Fatal("moved durable task was allowed to continue")
	}
}

func TestTransportOnlyDoneStatusDoesNotEndOrchestratorProviderTurn(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	if _, err := Create(cfg, "tasks", "retain native control emitter", ""); err != nil {
		t.Fatal(err)
	}
	target := newFakeRecipient()
	target.events = []core.Event{{Kind: core.EventStatus, Done: true, Text: "transport emitter retained"}}
	manager := New(cfg, target, extensions.Runner{})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	if leases[0].State != "processing" {
		t.Fatalf("transport-only done changed lease state to %q", leases[0].State)
	}
}

func TestAutomaticCleanupRunsOnlyForPrimaryAndUsesLiveRetention(t *testing.T) {
	cfg := config.Default()
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	ticks := make(chan time.Time, 3)
	manager.cleanupTicks = ticks
	called := make(chan int, 1)
	manager.Cleanup = func(_ context.Context, days int) (string, error) {
		called <- days
		return "complete", nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { manager.runAutomaticCleanup(ctx); close(done) }()
	ticks <- time.Now()
	select {
	case days := <-called:
		t.Fatalf("secondary ran automatic cleanup with %d days", days)
	case <-time.After(20 * time.Millisecond):
	}
	manager.SetPrimaryOwned(true)
	next := cfg
	next.Workspace.CleanupRetentionDays = 45
	manager.ApplyRuntimeConfig(next)
	ticks <- time.Now()
	select {
	case days := <-called:
		if days != 45 {
			t.Fatalf("automatic cleanup retention = %d, want 45", days)
		}
	case <-time.After(time.Second):
		t.Fatal("primary did not run automatic cleanup")
	}
	cancel()
	<-done
}

func TestTaskNotificationDecisionCoversTerminalAndActionableWaitingTransitions(t *testing.T) {
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		status string
		wakeAt string
		want   bool
	}{
		{name: "done", status: "done", want: true},
		{name: "failed", status: "failed", want: true},
		{name: "cancelled", status: "cancelled", want: true},
		{name: "unscheduled waiting", status: "waiting", want: true},
		{name: "malformed waiting schedule", status: "waiting", wakeAt: "not-a-time", want: true},
		{name: "due waiting schedule", status: "waiting", wakeAt: now.Format(time.RFC3339), want: true},
		{name: "future waiting schedule", status: "waiting", wakeAt: now.Add(time.Minute).Format(time.RFC3339), want: false},
		{name: "working", status: "working", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := Document{FrontMatter: map[string]any{}}
			if test.wakeAt != "" {
				document.FrontMatter["wake_at"] = test.wakeAt
			}
			if got := requiresTaskNotificationDecision(document, test.status, now); got != test.want {
				t.Fatalf("requiresTaskNotificationDecision(%q, wake_at=%q) = %v, want %v", test.status, test.wakeAt, got, test.want)
			}
		})
	}
}
