package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/workspace"
)

func scriptedManager(t *testing.T) (config.Config, *scriptedHarness, *Manager) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	target := newScriptedHarness()
	manager := New(cfg, target, extensions.Runner{Directory: filepath.Join(root, "missing")})
	return cfg, target, manager
}

// turnAction scripts one provider turn. The emitter goroutine models the real
// supervisor ordering: the durable document move happens, the active entry is
// removed, and only afterwards does the terminal emit (and its durable lease
// write) execute. Every field is optional channel-gated instrumentation.
type turnAction struct {
	entered   chan struct{}
	inactive  chan struct{}
	release   chan struct{}
	completed chan struct{}
	move      func()
}

type scriptedHarness struct {
	mu                  sync.Mutex
	calls               int
	notificationCalls   int
	threads             map[string]string
	active              map[string]bool
	actions             map[string]turnAction
	notificationStarted chan struct{}
	notificationRelease chan struct{}
	notificationHold    chan struct{}
	notificationOnce    sync.Once
}

func newScriptedHarness() *scriptedHarness {
	return &scriptedHarness{
		threads: map[string]string{},
		active:  map[string]bool{},
		actions: map[string]turnAction{},
	}
}

func (h *scriptedHarness) plan(key string, action turnAction) {
	h.mu.Lock()
	h.actions[key] = action
	h.mu.Unlock()
}

func (h *scriptedHarness) Start(context.Context) error { return nil }
func (h *scriptedHarness) Close() error                { return nil }
func (h *scriptedHarness) ResetSession(key string) error {
	h.mu.Lock()
	delete(h.threads, key)
	h.mu.Unlock()
	return nil
}

func (h *scriptedHarness) ThreadID(key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.threads[key]
}

func (h *scriptedHarness) IsActive(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active[key]
}

func (h *scriptedHarness) Interrupt(_ context.Context, key string) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.active[key] {
		return false, nil
	}
	h.active[key] = false
	return true, nil
}

func (h *scriptedHarness) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func (h *scriptedHarness) notificationCallCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.notificationCalls
}

func (h *scriptedHarness) Send(_ context.Context, key, _ string, emit core.Emit) (string, bool, error) {
	h.mu.Lock()
	h.calls++
	thread := h.threads[key]
	if thread == "" {
		thread = "thread-" + key
		h.threads[key] = thread
	}
	h.active[key] = true
	action := h.actions[key]
	h.mu.Unlock()
	if action.entered != nil {
		close(action.entered)
	}
	if strings.HasPrefix(key, "orchestrator:notification:") {
		h.mu.Lock()
		h.notificationCalls++
		h.mu.Unlock()
		h.notificationOnce.Do(func() {
			if h.notificationStarted != nil {
				close(h.notificationStarted)
			}
		})
		if h.notificationHold != nil {
			<-h.notificationHold
		}
		if h.notificationRelease != nil {
			<-h.notificationRelease
		}
		emit(core.Event{Kind: core.EventFinal, Text: "notified", ThreadID: thread, Done: true})
		h.mu.Lock()
		h.active[key] = false
		h.mu.Unlock()
		if action.completed != nil {
			close(action.completed)
		}
		return thread, false, nil
	}
	if action.release != nil {
		// Model the real supervisor ordering deterministically: the active
		// entry is already removed when Send returns, while the terminal emit
		// (and its durable lease write) is still pending.
		h.mu.Lock()
		h.active[key] = false
		h.mu.Unlock()
	}
	go func() {
		if action.move != nil {
			action.move()
		}
		if action.release == nil {
			h.mu.Lock()
			h.active[key] = false
			h.mu.Unlock()
		}
		if action.inactive != nil {
			close(action.inactive)
		}
		if action.release != nil {
			<-action.release
		}
		emit(core.Event{Kind: core.EventFinal, Text: "done", ThreadID: thread, Done: true})
		if action.completed != nil {
			close(action.completed)
		}
	}()
	return thread, false, nil
}

func taskDonePath(task string) string {
	base := filepath.Dir(filepath.Dir(task))
	return filepath.Join(base, "done", filepath.Base(task))
}

func taskWorkingPath(task string) string {
	base := filepath.Dir(filepath.Dir(task))
	return filepath.Join(base, "working", filepath.Base(task))
}

func taskDocumentID(t *testing.T, task string) string {
	t.Helper()
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	return documentID(document)
}

func assertTaskSettledDone(t *testing.T, task string, manager *Manager) {
	t.Helper()
	done := taskDonePath(task)
	settled, err := ReadDocument(done)
	if err != nil || stringField(settled, "status") != "done" {
		t.Fatalf("settled task = %v at %s, err %v", settled.FrontMatter["status"], done, err)
	}
	id := taskDocumentID(t, done)
	if _, err := manager.loadLease(leaseID("tasks:"+phaseTaskImplementation, id)); !os.IsNotExist(err) {
		t.Fatalf("implementation lease remained: %v", err)
	}
}

// J9: RunOnce waits for a dispatched implementation turn and settles its
// working -> done transition durably. The regression half proves the old
// ScanOnce + WaitForIdle behavior leaves the terminal state unreconciled.
func TestRunOnceSettlesWorkingToDone(t *testing.T) {
	cfg, regressionFake, regressionManager := workflowTestManager(t)
	regressionTask, err := CreateWithOptions(cfg, "tasks", "regression settle", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	regressionWorking := taskWorkingPath(regressionTask)
	regressionDone := taskDonePath(regressionTask)
	regressionFake.beforeEmit = func() {
		recordDirectCompletion(t, regressionWorking, regressionDone)
	}
	if err := regressionManager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	regressionManager.Wait()
	if err := regressionManager.WaitForIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	regressionID := taskDocumentID(t, regressionDone)
	staleLease, err := regressionManager.loadLease(leaseID("tasks:"+phaseTaskImplementation, regressionID))
	if err != nil || staleLease.State != "awaiting_transition" {
		t.Fatalf("pre-settlement regression lease = %#v, err %v; want awaiting_transition", staleLease, err)
	}

	settleCfg, fake, manager := scriptedManager(t)
	var jobsMu sync.Mutex
	finished := []int{}
	manager.JobStarted = func(Lease, string, time.Time, int, int) (int, error) { return 7, nil }
	manager.JobFinished = func(id int) {
		jobsMu.Lock()
		finished = append(finished, id)
		jobsMu.Unlock()
	}
	task, err := CreateWithOptions(settleCfg, "tasks", "settle directly", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	id := taskDocumentID(t, task)
	session := phaseSessionKey("tasks", id, phaseTaskImplementation, 1)
	working := taskWorkingPath(task)
	done := taskDonePath(task)
	entered := make(chan struct{})
	fake.plan(session, turnAction{entered: entered, move: func() {
		recordDirectCompletion(t, working, done)
	}})
	err = manager.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce = %v", err)
	}
	_ = entered
	assertTaskSettledDone(t, task, manager)
	if got := fake.callCount(); got != 1 {
		t.Fatalf("provider dispatches = %d, want 1", got)
	}
	jobsMu.Lock()
	defer jobsMu.Unlock()
	if len(finished) != 1 || finished[0] != 7 {
		t.Fatalf("JobFinished = %v, want [7]", finished)
	}
}

// J10: JobFinished fires exactly once for the dispatched job even though
// settlement keeps running additional reconcile rounds for later transitions.
func TestRunOnceFinishesJobExactlyOnceAcrossSettleRounds(t *testing.T) {
	cfg, fake, manager := scriptedManager(t)
	var jobsMu sync.Mutex
	finished := []int{}
	manager.JobStarted = func(Lease, string, time.Time, int, int) (int, error) { return 7, nil }
	manager.JobFinished = func(id int) {
		jobsMu.Lock()
		finished = append(finished, id)
		jobsMu.Unlock()
		if id == 7 {
			// A second durable transition keeps settlement busy for another
			// round so exactly-once accounting is observed across rounds.
			transitionLease(t, cfg, manager, "later round transition")
		}
	}
	task, err := CreateWithOptions(cfg, "tasks", "finish exactly once", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	id := taskDocumentID(t, task)
	session := phaseSessionKey("tasks", id, phaseTaskImplementation, 1)
	fake.plan(session, turnAction{move: func() {
		recordDirectCompletion(t, taskWorkingPath(task), taskDonePath(task))
	}})
	if err = manager.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce = %v", err)
	}
	assertTaskSettledDone(t, task, manager)
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 0 {
		t.Fatalf("leases after settle rounds = %#v, err %v", leases, err)
	}
	jobsMu.Lock()
	defer jobsMu.Unlock()
	if len(finished) != 1 || finished[0] != 7 {
		t.Fatalf("JobFinished across settle rounds = %v, want exactly [7]", finished)
	}
}

// J11: settlement reconciles working -> review durably without claiming the
// review queue, so no review dispatch happens during one-shot settlement.
func TestRunOnceDoesNotDispatchReviewDuringSettlement(t *testing.T) {
	cfg, fake, manager := scriptedManager(t)
	task, err := Create(cfg, "tasks", "hand off to review", "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["review_required"] = true
	if err = WriteDocument(task, document); err != nil {
		t.Fatal(err)
	}
	id := taskDocumentID(t, task)
	session := phaseSessionKey("tasks", id, phaseTaskImplementation, 1)
	reviewPath := filepath.Join(filepath.Dir(filepath.Dir(task)), "review", filepath.Base(task))
	fake.plan(session, turnAction{move: func() {
		if err := moveDocument(taskWorkingPath(task), reviewPath, "review", time.Now().UTC()); err != nil {
			t.Errorf("move task to review: %v", err)
		}
	}})
	if err = manager.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce = %v", err)
	}
	if _, err := manager.loadLease(leaseID("tasks:"+phaseTaskImplementation, id)); !os.IsNotExist(err) {
		t.Fatalf("implementation lease remained: %v", err)
	}
	reviewed, err := ReadDocument(reviewPath)
	if err != nil || stringField(reviewed, "status") != "review" {
		t.Fatalf("task not in review: status=%v err=%v", reviewed.FrontMatter["status"], err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(reviewPath), "reviewing", filepath.Base(task))); !os.IsNotExist(err) {
		t.Fatal("settlement claimed the review queue")
	}
	leases, err := manager.loadLeases()
	if err != nil {
		t.Fatal(err)
	}
	for _, lease := range leases {
		if lease.Phase == phaseTaskReview {
			t.Fatalf("settlement created a review lease: %#v", lease)
		}
	}
	if got := fake.callCount(); got != 1 {
		t.Fatalf("provider dispatches = %d, want 1 (implementation only)", got)
	}
}

// J12: the terminal notification agent starts exactly once, keeps RunOnce
// waiting, and is never restarted by later settle rounds.
func TestRunOnceWaitsForTerminalNotificationExactlyOnce(t *testing.T) {
	cfg, fake, manager := scriptedManager(t)
	cfg.Orchestrator.TaskNotifications = config.TaskNotificationsAlways
	manager.ApplyRuntimeConfig(cfg)
	task, err := CreateWithOptions(cfg, "tasks", "notify once", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["notify"] = map[string]any{"enabled": true, "origin": "tui/local", "on": []any{"done"}}
	if err = WriteDocument(task, document); err != nil {
		t.Fatal(err)
	}
	id := taskDocumentID(t, task)
	session := phaseSessionKey("tasks", id, phaseTaskImplementation, 1)
	fake.notificationStarted = make(chan struct{})
	fake.notificationRelease = make(chan struct{})
	fake.plan(session, turnAction{move: func() {
		recordDirectCompletion(t, taskWorkingPath(task), taskDonePath(task))
	}})
	result := make(chan error, 1)
	go func() { result <- manager.RunOnce(context.Background()) }()
	select {
	case <-fake.notificationStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal notification never started")
	}
	select {
	case err := <-result:
		t.Fatalf("RunOnce returned while the notification agent was active: %v", err)
	case <-time.After(time.Second):
	}
	close(fake.notificationRelease)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("RunOnce = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunOnce did not finish after notification completion")
	}
	if got := fake.notificationCallCount(); got != 1 {
		t.Fatalf("notification dispatches = %d, want exactly 1", got)
	}
	if got := fake.callCount(); got != 2 {
		t.Fatalf("provider dispatches = %d, want implementation plus one notification", got)
	}
	assertTaskSettledDone(t, task, manager)
}

// Waiting for manager jobs must honour cancellation: while a manager-tracked
// agent goroutine is live and no lease looks busy, a cancelled RunOnce
// returns context.Canceled instead of blocking on the job wait.
func TestRunOnceWaitForIdleIsCancellableWhileJobsActive(t *testing.T) {
	cfg, fake, manager := scriptedManager(t)
	cfg.Orchestrator.TaskNotifications = config.TaskNotificationsAlways
	manager.ApplyRuntimeConfig(cfg)
	task, err := CreateWithOptions(cfg, "tasks", "cancel while notifying", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["notify"] = map[string]any{"enabled": true, "origin": "tui/local", "on": []any{"done"}}
	if err = WriteDocument(task, document); err != nil {
		t.Fatal(err)
	}
	id := taskDocumentID(t, task)
	fake.notificationStarted = make(chan struct{})
	fake.notificationHold = make(chan struct{})
	fake.notificationRelease = make(chan struct{})
	fake.plan(phaseSessionKey("tasks", id, phaseTaskImplementation, 1), turnAction{move: func() {
		recordDirectCompletion(t, taskWorkingPath(task), taskDonePath(task))
	}})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- manager.RunOnce(ctx) }()
	select {
	case <-fake.notificationStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal notification never started")
	}
	// The reconciled task leaves no busy lease while the blocked notification
	// goroutine keeps the manager job wait occupied; cancellation must
	// release RunOnce promptly through the context-aware wait.
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled RunOnce = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunOnce ignored cancellation while manager jobs were active")
	}
	close(fake.notificationHold)
	close(fake.notificationRelease)
	manager.Wait()
	assertTaskSettledDone(t, task, manager)
}

// J13: the terminal gate tracks the terminal emitter's durable write, not the
// turn's IsActive state. While the emitter is blocked before its durable
// write, reconciliation must not run even though the harness already reports
// the turn inactive.
func TestRunOnceWaitsForTerminalEmitDurableWrite(t *testing.T) {
	cfg, fake, manager := scriptedManager(t)
	var jobsMu sync.Mutex
	finished := []int{}
	var updatedStates []string
	var finalEvents int
	manager.JobStarted = func(Lease, string, time.Time, int, int) (int, error) { return 7, nil }
	manager.JobFinished = func(id int) {
		jobsMu.Lock()
		finished = append(finished, id)
		jobsMu.Unlock()
	}
	manager.JobUpdated = func(_ int, lease Lease) {
		jobsMu.Lock()
		updatedStates = append(updatedStates, lease.State)
		jobsMu.Unlock()
	}
	manager.JobEvent = func(_ int, event core.Event) {
		if event.Done && event.Kind == core.EventFinal {
			jobsMu.Lock()
			finalEvents++
			jobsMu.Unlock()
		}
	}
	task, err := CreateWithOptions(cfg, "tasks", "wait for durable terminal", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	id := taskDocumentID(t, task)
	session := phaseSessionKey("tasks", id, phaseTaskImplementation, 1)
	entered := make(chan struct{})
	inactive := make(chan struct{})
	release := make(chan struct{})
	fake.plan(session, turnAction{
		entered:  entered,
		inactive: inactive,
		release:  release,
		move: func() {
			recordDirectCompletion(t, taskWorkingPath(task), taskDonePath(task))
		},
	})
	result := make(chan error, 1)
	go func() { result <- manager.RunOnce(context.Background()) }()
	<-entered
	<-inactive
	// The harness already reports the turn inactive while the terminal emit
	// is still blocked before its durable write.
	select {
	case err := <-result:
		t.Fatalf("RunOnce settled without the terminal durable write: %v", err)
	case <-time.After(time.Second):
	}
	held, err := manager.loadLease(leaseID("tasks:"+phaseTaskImplementation, id))
	if err != nil || held.State != "processing" {
		t.Fatalf("blocked terminal lease = %#v, err %v; want still processing", held, err)
	}
	if _, err := ReadDocument(taskDonePath(task)); err != nil {
		t.Fatalf("durable document move missing: %v", err)
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("RunOnce = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunOnce did not finish after the terminal emit")
	}
	jobsMu.Lock()
	defer jobsMu.Unlock()
	if finalEvents != 1 {
		t.Fatalf("terminal events = %d, want 1", finalEvents)
	}
	foundAwaiting := false
	for _, state := range updatedStates {
		if state == "awaiting_transition" {
			foundAwaiting = true
		}
	}
	if !foundAwaiting {
		t.Fatalf("terminal durable state never landed: %v", updatedStates)
	}
	if len(finished) != 1 || finished[0] != 7 {
		t.Fatalf("JobFinished = %v, want [7]", finished)
	}
	assertTaskSettledDone(t, task, manager)
}

// P1 regression: a RunOnce-owned turn whose terminal emit is still pending
// can have its runtime job retired by another live-primary scan that already
// reconciled the transition. The terminal emit then reaches the retired-job
// guard, which performs no durable mutation; a non-continuing terminal event
// must still release the settlement gate there, or RunOnce blocks until
// cancellation.
func TestRunOnceTerminalEmitAfterRuntimeJobRetirementReleasesGate(t *testing.T) {
	cfg, fake, manager := scriptedManager(t)
	var jobsMu sync.Mutex
	finished := []int{}
	manager.JobStarted = func(Lease, string, time.Time, int, int) (int, error) { return 7, nil }
	manager.JobFinished = func(id int) {
		jobsMu.Lock()
		finished = append(finished, id)
		jobsMu.Unlock()
	}
	task, err := CreateWithOptions(cfg, "tasks", "terminal after job retirement", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	id := taskDocumentID(t, task)
	session := phaseSessionKey("tasks", id, phaseTaskImplementation, 1)
	entered := make(chan struct{})
	inactive := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan struct{})
	fake.plan(session, turnAction{
		entered:   entered,
		inactive:  inactive,
		release:   release,
		completed: completed,
		move: func() {
			recordDirectCompletion(t, taskWorkingPath(task), taskDonePath(task))
		},
	})
	result := make(chan error, 1)
	go func() { result <- manager.RunOnce(context.Background()) }()
	<-entered
	<-inactive
	// RunOnce is now parked on the settlement gate while the terminal emit
	// is blocked. A gate-free live-primary scan reconciles the already-moved
	// transition through the real scan path, which retires the runtime job.
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatalf("live-primary scan failed: %v", err)
	}
	close(release)
	<-completed
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("RunOnce = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunOnce ignored the terminal emit of a retired runtime job: settlement gate leaked")
	}
	jobsMu.Lock()
	defer jobsMu.Unlock()
	if len(finished) != 1 || finished[0] != 7 {
		t.Fatalf("JobFinished = %v, want exactly [7]", finished)
	}
	assertTaskSettledDone(t, task, manager)
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 0 {
		t.Fatalf("leases after settled run once = %#v, err %v", leases, err)
	}
}

// stagedTransition is a reconcilable transition document held outside the
// workflow folders until a settle round asks for it.
type stagedTransition struct {
	staging string
	done    string
	lease   Lease
}

func stageTransition(t *testing.T, cfg config.Config, title string) stagedTransition {
	t.Helper()
	todo, err := Create(cfg, "tasks", title, "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(todo)
	if err != nil {
		t.Fatal(err)
	}
	id := documentID(document)
	staging := filepath.Join(cfg.Root, "staging", fmt.Sprintf("%s.md", id))
	if err := os.MkdirAll(filepath.Dir(staging), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(todo, staging); err != nil {
		t.Fatal(err)
	}
	base := filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source))
	name := filepath.Base(todo)
	now := time.Now().UTC()
	lease := Lease{
		ID: leaseID("tasks:"+phaseTaskReview, id), ClaimID: leaseID("tasks:"+phaseTaskReview, id),
		DocumentType: "task", Route: "tasks", File: filepath.Join(base, "reviewing", name),
		SessionKey: phaseSessionKey("tasks", id, phaseTaskReview, 1), State: "awaiting_transition",
		Phase: phaseTaskReview, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now,
	}
	return stagedTransition{staging: staging, done: filepath.Join(base, "done", name), lease: lease}
}

func (item stagedTransition) publish(t *testing.T, manager *Manager, jobID int) {
	if err := moveDocument(item.staging, item.done, "done", time.Now().UTC()); err != nil {
		t.Errorf("stage transition: %v", err)
		return
	}
	if err := manager.saveLease(item.lease); err != nil {
		t.Errorf("stage transition lease: %v", err)
		return
	}
	if jobID > 0 {
		manager.setRuntimeJob(item.lease.ID, jobID)
	}
}

// J14: settlement is bounded. When reconciliation keeps finding fresh durable
// transitions every round, RunOnce performs exactly the bounded rounds,
// reports that it did not settle, and leaves durable state valid.
func TestRunOnceBoundsSettlementRounds(t *testing.T) {
	cfg, _, manager := scriptedManager(t)
	// The staged feed pins the C7.6b bound independently of the production
	// constant so a removed or changed bound fails fast on assertions.
	const rounds = 8
	const stagedCount = rounds + 1
	staged := make([]stagedTransition, stagedCount)
	for i := range staged {
		staged[i] = stageTransition(t, cfg, fmt.Sprintf("bounded settle round %d", i))
	}
	var jobsMu sync.Mutex
	finished := []int{}
	manager.JobFinished = func(id int) {
		jobsMu.Lock()
		finished = append(finished, id)
		jobsMu.Unlock()
		next := id - 100 + 1
		if next < stagedCount {
			staged[next].publish(t, manager, id+1)
		}
	}
	staged[0].publish(t, manager, 100)
	err := manager.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("did not settle within %d rounds", rounds)) {
		t.Fatalf("RunOnce error = %v, want did-not-settle bound", err)
	}
	jobsMu.Lock()
	defer jobsMu.Unlock()
	if len(finished) != rounds+1 {
		t.Fatalf("reconciled transitions = %d, want %d (initial scan plus %d rounds)", len(finished), rounds+1, rounds)
	}
	leases, leaseErr := manager.loadLeases()
	if leaseErr != nil || len(leases) != 0 {
		t.Fatalf("durable state after bounded settle = %#v, err %v", leases, leaseErr)
	}
	for _, item := range staged {
		settled, readErr := ReadDocument(item.done)
		if readErr != nil || stringField(settled, "status") != "done" {
			t.Fatalf("staged transition %s invalid: %v, %v", item.done, settled.FrontMatter["status"], readErr)
		}
	}
}

// J15: reconciliation of an owned goal review -> planning transition may
// dispatch the planning continuation, and settlement must wait for that
// continuation and reconcile its result without admitting unrelated work.
func TestRunOnceContinuesGoalReviewIntoPlanning(t *testing.T) {
	cfg, fake, manager := scriptedManager(t)
	goal := writeActiveGoalRound(t, cfg, "all_round_tasks_settled", time.Now().Add(-time.Minute), "done")
	goalBase := filepath.Dir(cfg.Resolve(workflowRoutes()[1].Source))
	reviewPath := filepath.Join(goalBase, "review", filepath.Base(goal))
	if err := moveDocument(goal, reviewPath, "review", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// The continuation ends in active/, so no round checkpoint may remain:
	// a leftover next_review_at without checkpoint_reason would fail goal
	// activation validation.
	goalDocument, err := ReadDocument(reviewPath)
	if err != nil {
		t.Fatal(err)
	}
	delete(goalDocument.FrontMatter, "next_review_at")
	if err = WriteDocument(reviewPath, goalDocument); err != nil {
		t.Fatal(err)
	}
	goalID := taskDocumentID(t, reviewPath)
	name := filepath.Base(goal)
	reviewing := filepath.Join(goalBase, "reviewing", name)
	planning := filepath.Join(goalBase, "planning", name)
	active := filepath.Join(goalBase, "active", name)
	// Unrelated queued work must not be admitted during settlement. The task
	// is created by the goal review turn's emitter, which runs after RunOnce's
	// one ordinary scan has already read the implementation queue, so it is
	// genuinely eligible for every settle round that follows.
	var unrelatedMu sync.Mutex
	unrelated := ""
	fake.plan(phaseSessionKey("goals", goalID, phaseGoalReview, 1), turnAction{move: func() {
		if err := moveDocument(reviewing, planning, "planning", time.Now().UTC()); err != nil {
			t.Errorf("move goal to planning: %v", err)
			return
		}
		created, createErr := Create(cfg, "tasks", "unrelated eligible task", "")
		if createErr != nil {
			t.Errorf("create unrelated eligible task: %v", createErr)
			return
		}
		unrelatedMu.Lock()
		unrelated = created
		unrelatedMu.Unlock()
	}})
	fake.plan(phaseSessionKey("goals", goalID, phaseGoalPlanning, 1), turnAction{move: func() {
		if err := moveDocument(planning, active, "active", time.Now().UTC()); err != nil {
			t.Errorf("move goal to active: %v", err)
		}
	}})
	if err = manager.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce = %v", err)
	}
	activated, err := ReadDocument(active)
	if err != nil || stringField(activated, "status") != "active" {
		t.Fatalf("goal not activated: status=%v err=%v", activated.FrontMatter["status"], err)
	}
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 0 {
		t.Fatalf("leases after goal continuation = %#v, err %v", leases, err)
	}
	if got := fake.callCount(); got != 2 {
		t.Fatalf("provider dispatches = %d, want review plus planning continuation only", got)
	}
	unrelatedMu.Lock()
	unrelatedPath := unrelated
	unrelatedMu.Unlock()
	if unrelatedPath == "" {
		t.Fatal("unrelated eligible task was never created")
	}
	if _, err := os.Stat(unrelatedPath); err != nil {
		t.Fatalf("unrelated eligible task was claimed during settlement: %v", err)
	}
	if _, err := os.Stat(taskWorkingPath(unrelatedPath)); !os.IsNotExist(err) {
		t.Fatal("unrelated eligible task left the queue during settlement")
	}
	unrelatedID := taskDocumentID(t, unrelatedPath)
	if _, err := manager.loadLease(leaseID("tasks:"+phaseTaskImplementation, unrelatedID)); !os.IsNotExist(err) {
		t.Fatalf("unrelated eligible task received a lease during settlement: %v", err)
	}
}

// J17: concurrent serve-style scans with gate-free contexts cannot crash
// manager synchronization, contaminate the RunOnce settlement gate, or
// violate exactly-once job completion. The last serve turn stays blocked at
// its terminal emit while RunOnce settles its own task and returns.
func TestRunOnceIsolatesSettlementGateFromConcurrentServeWork(t *testing.T) {
	cfg, fake, manager := scriptedManager(t)
	var jobsMu sync.Mutex
	started := 0
	finished := []int{}
	manager.JobStarted = func(Lease, string, time.Time, int, int) (int, error) {
		jobsMu.Lock()
		defer jobsMu.Unlock()
		started++
		return started, nil
	}
	manager.JobFinished = func(id int) {
		jobsMu.Lock()
		finished = append(finished, id)
		jobsMu.Unlock()
	}
	onceTask, err := CreateWithOptions(cfg, "tasks", "run once own task", "", CreateOptions{NoReview: true})
	if err != nil {
		t.Fatal(err)
	}
	onceID := taskDocumentID(t, onceTask)
	onceEntered := make(chan struct{})
	fake.plan(phaseSessionKey("tasks", onceID, phaseTaskImplementation, 1), turnAction{
		entered: onceEntered,
		move:    func() { recordDirectCompletion(t, taskWorkingPath(onceTask), taskDonePath(onceTask)) },
	})

	const serveIterations = 3
	var serveEntered [serveIterations]chan struct{}
	var serveCompleted [serveIterations]chan struct{}
	for i := range serveEntered {
		serveEntered[i] = make(chan struct{})
		serveCompleted[i] = make(chan struct{})
	}
	serveRelease := make(chan struct{})
	serveReady := make(chan struct{})
	serveStart := make(chan struct{})
	serveDone := make(chan struct{})
	serveCtx := context.Background()
	if settlementFrom(serveCtx) != nil {
		t.Fatal("serve context must not carry a settlement gate")
	}
	go func() {
		close(serveReady)
		<-serveStart
		for i := 0; i < serveIterations; i++ {
			task, createErr := Create(cfg, "tasks", fmt.Sprintf("serve work %d", i), "")
			if createErr != nil {
				t.Errorf("serve task %d: %v", i, createErr)
				return
			}
			key := phaseSessionKey("tasks", taskDocumentID(t, task), phaseTaskImplementation, 1)
			action := turnAction{entered: serveEntered[i], completed: serveCompleted[i]}
			if i == serveIterations-1 {
				// The last serve turn blocks at its terminal emit so the test
				// proves it can never be inside the RunOnce gate.
				action.release = serveRelease
			}
			fake.plan(key, action)
			if scanErr := manager.ScanOnce(serveCtx); scanErr != nil {
				t.Errorf("serve scan %d: %v", i, scanErr)
				return
			}
			<-serveEntered[i]
			if i < serveIterations-1 {
				<-serveCompleted[i]
			}
		}
		close(serveDone)
	}()
	<-serveReady
	result := make(chan error, 1)
	go func() { result <- manager.RunOnce(context.Background()) }()
	<-onceEntered
	close(serveStart)
	<-serveDone

	var runErr error
	select {
	case runErr = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("RunOnce did not return")
	}
	if runErr != nil {
		t.Fatalf("RunOnce = %v", runErr)
	}
	select {
	case <-serveCompleted[serveIterations-1]:
		t.Fatal("blocked serve turn completed before its release")
	default:
	}
	assertTaskSettledDone(t, onceTask, manager)
	close(serveRelease)
	<-serveCompleted[serveIterations-1]
	manager.Wait()
	jobsMu.Lock()
	defer jobsMu.Unlock()
	seen := map[int]int{}
	for _, id := range finished {
		seen[id]++
		if seen[id] > 1 {
			t.Fatalf("JobFinished fired %d times for job %d", seen[id], id)
		}
	}
	if len(finished) != 1 || finished[0] != 1 {
		t.Fatalf("JobFinished = %v, want exactly [1] for the run-once task", finished)
	}
	if got := fake.callCount(); got != serveIterations+1 {
		t.Fatalf("provider dispatches = %d, want %d", got, serveIterations+1)
	}
}
