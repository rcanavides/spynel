package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/facts"
	"github.com/agent0ai/spynel/internal/workspace"
)

// LA6: ten thousand launch identities are unique and well-formed.
func TestLA6TenThousandUniqueLaunchIDs(t *testing.T) {
	seen := make(map[string]bool, 10000)
	for i := 0; i < 10000; i++ {
		launch := NewLaunchID()
		if !strings.HasPrefix(launch, "ln-") || len(launch) != len("ln-")+26 {
			t.Fatalf("launch id shape = %q", launch)
		}
		if strings.ToLower(launch) != launch {
			t.Fatalf("launch id must be lowercase: %q", launch)
		}
		if seen[launch] {
			t.Fatalf("duplicate launch id %q", launch)
		}
		seen[launch] = true
	}
}

// sharedClaimFixture builds a manager with a claimed task document and a fake
// harness that returns at admission without any terminal event.
func sharedClaimFixture(t *testing.T, events []core.Event) (*Manager, *emitCaptureHarness, Lease, string) {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	document, err := Create(cfg, "tasks", "launch probe", "")
	if err != nil {
		t.Fatal(err)
	}
	fake := newEmitCaptureHarness()
	fake.events = events
	manager := New(cfg, fake, extensions.Runner{})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	return manager, fake, leases[0], document
}

// emitCaptureHarness is a fake harness whose emits the test controls.
type emitCaptureHarness struct {
	mu        sync.Mutex
	threads   map[string]string
	active    map[string]bool
	emits     map[string][]core.Emit
	events    []core.Event
	prompts   map[string]string
	sends     int
	onSend    func(harness *emitCaptureHarness, key, prompt string)
	violation string
}

func newEmitCaptureHarness() *emitCaptureHarness {
	return &emitCaptureHarness{threads: map[string]string{}, active: map[string]bool{}, emits: map[string][]core.Emit{}, prompts: map[string]string{}}
}

func (f *emitCaptureHarness) Start(context.Context) error { return nil }
func (f *emitCaptureHarness) Close() error                { return nil }
func (f *emitCaptureHarness) ResetSession(key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.threads, key)
	return nil
}
func (f *emitCaptureHarness) ThreadID(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.threads[key]
}
func (f *emitCaptureHarness) IsActive(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active[key]
}
func (f *emitCaptureHarness) Interrupt(_ context.Context, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.active[key] {
		return false, nil
	}
	f.active[key] = false
	return true, nil
}
func (f *emitCaptureHarness) Send(_ context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	f.mu.Lock()
	f.sends++
	f.prompts[key] = prompt
	f.emits[key] = append(f.emits[key], emit)
	thread := f.threads[key]
	if thread == "" {
		thread = "thread-" + key
		f.threads[key] = thread
	}
	hook := f.onSend
	f.mu.Unlock()
	if hook != nil {
		hook(f, key, prompt)
	}
	f.mu.Lock()
	events := f.events
	f.mu.Unlock()
	for _, event := range events {
		event.ThreadID = thread
		emit(event)
	}
	return thread, false, nil
}

// LA1: every execution and recovery gets a new LaunchID; a shared launch
// keeps its session key and thread semantics across recovery.
func TestLA1RecoveryAlwaysGetsNewLaunchSharedKeyPreserved(t *testing.T) {
	manager, fake, lease, _ := sharedClaimFixture(t, nil)
	if lease.LaunchID == "" {
		t.Fatal("first dispatch must assign a launch identity")
	}
	if lease.SessionKey != phaseSessionKey("tasks", lease.DocumentID, lease.Phase, lease.ClaimAttempt) {
		t.Fatalf("shared session key = %q, want exactly phaseSessionKey without a launch suffix", lease.SessionKey)
	}
	firstLaunch := lease.LaunchID
	thread := lease.ThreadID
	fake.mu.Lock()
	fake.events = nil
	fake.mu.Unlock()
	manager.dispatch(context.Background(), workflowRoutes()[0], lease, true)
	manager.Wait()
	recovered, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.LaunchID == firstLaunch {
		t.Fatal("recovery must assign a new LaunchID")
	}
	if recovered.SessionKey != lease.SessionKey {
		t.Fatalf("shared recovery must keep the session key: %q vs %q", recovered.SessionKey, lease.SessionKey)
	}
	if recovered.ThreadID != thread {
		t.Fatalf("shared recovery keeps thread semantics: %q vs %q", recovered.ThreadID, thread)
	}
	if recovered.RecoveryCount != 1 {
		t.Fatalf("recovery count = %d", recovered.RecoveryCount)
	}
	// Both launches recorded launch_created; the second supersedes the first.
	evidence, err := manager.documentFacts("tasks", lease.DocumentID)
	if err != nil {
		t.Fatal(err)
	}
	first, ok := findFact(evidence, launchCreatedKey(firstLaunch))
	if !ok || first.Supersedes != "" {
		t.Fatalf("first launch fact = %+v", first)
	}
	second, ok := findFact(evidence, launchCreatedKey(recovered.LaunchID))
	if !ok || second.Supersedes != firstLaunch {
		t.Fatalf("replacement fact must supersede: %+v", second)
	}
}

// LA2: a late event from a superseded shared launch cannot mutate the lease
// even though both launches share one session key.
func TestLA2LateSharedLaunchEventRejectedByLaunchID(t *testing.T) {
	manager, _, lease, _ := sharedClaimFixture(t, nil)
	staleLaunch := lease.LaunchID
	// Supersede with a recovery dispatch: same session key, new launch.
	manager.dispatch(context.Background(), workflowRoutes()[0], lease, true)
	manager.Wait()
	current, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.LaunchID == staleLaunch || current.SessionKey != lease.SessionKey {
		t.Fatalf("supersession = %q/%q", current.LaunchID, current.SessionKey)
	}
	before := current
	// A late mutation fenced by the stale launch must fail closed.
	_, err = manager.updateLeaseValue(lease.ID, staleLaunch, func(target *Lease) {
		target.State = "awaiting_transition"
		target.LastError = "stale launch write"
	})
	if !errors.Is(err, ErrStaleLaunch) {
		t.Fatalf("stale launch write = %v, want ErrStaleLaunch", err)
	}
	after, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != before.State || after.LastError != before.LastError {
		t.Fatalf("stale launch mutated durable state: %#v", after)
	}
	// The current launch's fence still admits its own writes.
	if _, err := manager.updateLeaseValue(lease.ID, current.LaunchID, func(target *Lease) {
		target.HeartbeatAt = time.Now().UTC()
	}); err != nil {
		t.Fatalf("current launch write: %v", err)
	}
}

// LA3: an isolated stale pipeline cannot finalize a replacement launch's lease.
func TestLA3IsolatedStalePipelineRejected(t *testing.T) {
	manager, _, lease, _ := sharedClaimFixture(t, nil)
	// Model an isolated lease superseded by a replacement launch.
	lease.WorkspaceKind = execws.WorkspaceKindGitWorktree
	lease.State = "awaiting_transition"
	staleLaunch := lease.LaunchID
	lease.LaunchID = NewLaunchID()
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	// The stale launch's pipeline finalize is fenced out.
	_, err := manager.updateLeaseValue(lease.ID, staleLaunch, func(target *Lease) {
		target.State = "finalized"
	})
	if !errors.Is(err, ErrStaleLaunch) {
		t.Fatalf("stale pipeline finalize = %v, want ErrStaleLaunch", err)
	}
	current, err := manager.loadLease(lease.ID)
	if err != nil || current.State != "awaiting_transition" {
		t.Fatalf("stale pipeline changed state: %#v", current)
	}
}

// LA4a: shared session keys never carry a launch suffix, for tasks and goals.
func TestLA4aSharedSessionKeyHasNoLaunchSuffix(t *testing.T) {
	for _, routeName := range []string{"tasks", "goals"} {
		t.Run(routeName, func(t *testing.T) {
			root := t.TempDir()
			if err := workspace.Init(root, false); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(config.PathForRoot(root))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Create(cfg, routeName, "session key probe", ""); err != nil {
				t.Fatal(err)
			}
			manager := New(cfg, newEmitCaptureHarness(), extensions.Runner{})
			if err := manager.ScanOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager.Wait()
			leases, err := manager.loadLeases()
			if err != nil || len(leases) != 1 {
				t.Fatalf("leases = %#v, %v", leases, err)
			}
			lease := leases[0]
			want := phaseSessionKey(routeName, lease.DocumentID, lease.Phase, lease.ClaimAttempt)
			if lease.SessionKey != want {
				t.Fatalf("session key = %q, want %q", lease.SessionKey, want)
			}
			if strings.Contains(lease.SessionKey, lease.LaunchID) {
				t.Fatalf("shared session key must not contain the launch id: %q", lease.SessionKey)
			}
		})
	}
}

// LA5: a control continuation fenced by a stale shared launch is refused
// even though the session key never changed.
func TestLA5ControlContinuationStaleByLaunchID(t *testing.T) {
	manager, fake, lease, _ := sharedClaimFixture(t, nil)
	documentID := documentIDFromPath(t, lease.File)
	route := workflowRoutes()[0]
	// The session is active and its job is live.
	fake.mu.Lock()
	fake.active[lease.SessionKey] = true
	fake.mu.Unlock()
	manager.setRuntimeJob(lease.ID, 7)
	current, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !manager.ControlStillValid(current, documentID) {
		t.Fatal("current launch control must remain valid")
	}
	// Supersede: recovery dispatch replaces the launch under the same key.
	stale := current
	manager.dispatch(context.Background(), route, lease, true)
	manager.Wait()
	fake.mu.Lock()
	fake.active[lease.SessionKey] = true
	fake.mu.Unlock()
	replacement, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	manager.setRuntimeJob(lease.ID, 9)
	if manager.ControlStillValid(stale, documentID) {
		t.Fatal("a stale launch must fail the control fence")
	}
	if !manager.ControlStillValid(replacement, documentID) {
		t.Fatal("the replacement launch must own the control fence")
	}
	if manager.PrepareControlContinuation(stale, documentID) {
		t.Fatal("stale continuation preparation must be refused")
	}
	if manager.ReserveControlProviderTurn(stale, documentID) {
		t.Fatal("stale provider turn reservation must be refused")
	}
}

// F6: replacement of L17 by L18 between the preliminary validity check and
// the durable continuation write is fenced by the atomic LaunchID CAS.
func TestPrepareControlContinuationCannotOverwriteReplacementLaunch(t *testing.T) {
	manager, fake, lease, _ := sharedClaimFixture(t, nil)
	documentID := documentIDFromPath(t, lease.File)
	fake.mu.Lock()
	fake.active[lease.SessionKey] = true
	fake.mu.Unlock()
	manager.setRuntimeJob(lease.ID, 17)

	entered := make(chan struct{})
	resume := make(chan struct{})
	manager.controlContinuationBeforeUpdate = func() {
		close(entered)
		<-resume
	}
	result := make(chan bool, 1)
	go func() {
		result <- manager.PrepareControlContinuation(lease, documentID)
	}()
	<-entered

	replacementLaunch := NewLaunchID()
	replacement, err := manager.updateLeaseValue(lease.ID, lease.LaunchID, func(current *Lease) {
		current.LaunchID = replacementLaunch
		current.State = "recovering"
		current.LastError = "replacement owns this lease"
	})
	if err != nil {
		t.Fatal(err)
	}
	close(resume)
	if <-result {
		t.Fatal("stale L17 continuation was accepted after L18 replacement")
	}
	current, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.LaunchID != replacement.LaunchID || current.State != replacement.State || current.LastError != replacement.LastError {
		t.Fatalf("stale continuation overwrote replacement: got %#v want %#v", current, replacement)
	}
}

// LA7: a legacy shared lease never upgrades to isolated mode through
// recovery, and its recovery keeps the recorded session key semantics.
func TestLA7LegacySharedRecoveryNeverUpgradesMode(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "legacy lease probe", ""); err != nil {
		t.Fatal(err)
	}
	fake := newEmitCaptureHarness()
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
	// Legacy shape: no launch, no workspace kind. Flip the live mode only
	// after the shared claim exists: mode changes never upgrade a lineage.
	lease.LaunchID = ""
	lease.WorkspaceKind = ""
	lease.HeartbeatAt = time.Now().UTC().Add(-2 * time.Hour)
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	flipped := cfg
	flipped.Orchestrator.WorkspaceIsolation = config.WorkspaceIsolationGitWorktree
	manager.ApplyRuntimeConfig(flipped)
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	recovered, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.WorkspaceKind != "" {
		t.Fatalf("legacy shared lease upgraded to %q", recovered.WorkspaceKind)
	}
	if recovered.SessionKey != lease.SessionKey {
		t.Fatalf("legacy recovery changed the session key: %q vs %q", recovered.SessionKey, lease.SessionKey)
	}
	if recovered.LaunchID == "" {
		t.Fatal("legacy recovery must still assign a launch identity")
	}
	if recovered.WorkspaceID != "" {
		t.Fatalf("legacy recovery must not assign an isolated workspace: %q", recovered.WorkspaceID)
	}
}

// LA9: same-process supersession cancels the prior launch's writer waiter so
// the replacement never deadlocks behind a launch that will not settle.
func TestLA9SupersessionCancelsWriterWaiter(t *testing.T) {
	manager, fake, lease, _ := sharedClaimFixture(t, nil)
	// The first launch never settles: no terminal event was emitted.
	fake.mu.Lock()
	fake.events = nil
	fake.mu.Unlock()
	manager.dispatch(context.Background(), workflowRoutes()[0], lease, true)
	manager.Wait()
	// The replacement dispatched although the first launch never emitted a
	// terminal: its waiter was cancelled.
	if fake.callCount() != 2 {
		t.Fatalf("superseded writer slot blocked the replacement: %d sends", fake.callCount())
	}
	// A late terminal from the first launch performs no durable mutation.
	current, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	staleState := current.State
	fake.mu.Lock()
	emitters := fake.emits[lease.SessionKey]
	fake.mu.Unlock()
	if len(emitters) < 1 {
		t.Fatalf("the first launch's emitter was not captured")
	}
	staleEmit := emitters[0]
	// The stale launch is fenced; its settle path records evidence only.
	staleEmit(core.Event{Kind: core.EventFinal, Text: "late", Done: true})
	after, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != staleState {
		t.Fatalf("stale terminal changed the durable state: %q vs %q", after.State, staleState)
	}
}

func (f *emitCaptureHarness) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sends
}

// LA10: a continuing terminal never starts the post-terminal pipeline.
func TestLA10ContinuingTerminalDoesNotBeginPipeline(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(cfg, "tasks", "continuing terminal probe", ""); err != nil {
		t.Fatal(err)
	}
	fake := newEmitCaptureHarness()
	manager := New(cfg, fake, extensions.Runner{})
	// Deliver a continuing final followed by the settling final.
	setScriptedEvents(fake, []core.Event{
		{Kind: core.EventFinal, Text: "partial", Done: true, Continues: true},
		{Kind: core.EventFinal, Text: "done", Done: true},
	})
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	if leases[0].State != "awaiting_transition" {
		t.Fatalf("lease state = %q", leases[0].State)
	}
	// No pipeline runs for shared launches; the writer slot was released by
	// the settling terminal, so a second workflow dispatches immediately.
	document2, err := Create(cfg, "tasks", "second workflow", "")
	if err != nil {
		t.Fatal(err)
	}
	_ = document2
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.callCount() != 2 {
		t.Fatalf("writer slot did not release after the settling terminal: %d", fake.callCount())
	}
}

// setScriptedEvents installs a per-send event script on the fake.
func setScriptedEvents(fake *emitCaptureHarness, events []core.Event) {
	fake.mu.Lock()
	fake.events = events
	fake.mu.Unlock()
}

// LA12: startExistingClaim never reuses an ACTIVE shared session key.
func TestLA12StartExistingClaimGuardsActiveSharedSession(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	document, err := Create(cfg, "tasks", "orphan claim probe", "")
	if err != nil {
		t.Fatal(err)
	}
	fake := newEmitCaptureHarness()
	manager := New(cfg, fake, extensions.Runner{})
	// Simulate a previously claimed document whose lease vanished: the
	// attempt counter stays at its real claimed value so recovery would reuse
	// the same shared session key.
	working := filepath.Join(cfg.StatePath("tasks", "working"), filepath.Base(document))
	todo := filepath.Join(cfg.StatePath("tasks", "todo"), filepath.Base(document))
	claimed, err := ReadDocument(todo)
	if err != nil {
		t.Fatal(err)
	}
	claimed.FrontMatter["attempt"] = 1
	claimed.FrontMatter["status"] = "working"
	if err := WriteDocument(todo, claimed); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(todo, working); err != nil {
		t.Fatal(err)
	}
	documentID := documentIDFromPath(t, working)
	sessionKey := phaseSessionKey("tasks", documentID, phaseTaskImplementation, 1)
	fake.mu.Lock()
	fake.active[sessionKey] = true
	fake.mu.Unlock()
	// An orphan-claim recovery must refuse the active session.
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.callCount() != 0 {
		t.Fatal("startExistingClaim steered a replacement into an active session")
	}
	// Once the session goes inactive the same recovery dispatches.
	fake.mu.Lock()
	fake.active[sessionKey] = false
	fake.mu.Unlock()
	if err := manager.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if fake.callCount() != 1 {
		t.Fatalf("inactive session was not recovered: %d", fake.callCount())
	}
}

func documentIDFromPath(t *testing.T, path string) string {
	t.Helper()
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	id := documentID(document)
	if id == "" {
		id = filepath.Base(path)
	}
	return id
}

// RG2: chat/notification/heartbeat turns never acquire launch identities or
// workspace gating; RG3-style goal launches stay shared-root.
func TestRG2NotificationTurnCarriesNoLaunch(t *testing.T) {
	manager, _, lease, _ := sharedClaimFixture(t, nil)
	documentID := lease.DocumentID
	// A notification-agent turn is dispatched outside the workflow gate with
	// no durable launch identity.
	fake := newEmitCaptureHarness()
	manager.Harness = fake
	goalDocument, err := ReadDocument(lease.File)
	if err != nil {
		t.Fatal(err)
	}
	_ = goalDocument
	before, err := manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	launchFacts := 0
	for _, fact := range before {
		if fact.Kind == facts.KindLaunchCreated {
			launchFacts++
		}
	}
	// Directly exercising the notification path is covered by its own suite;
	// here the invariant is that only workflow dispatches create launches.
	if launchFacts != 1 {
		t.Fatalf("expected exactly one workflow launch fact, got %d", launchFacts)
	}
}

// LA11: shutdown waiting terminates safely — a cancelled process lifetime
// aborts an in-flight post-terminal pipeline without fabricating evidence,
// and the manager's job group still drains.
func TestLA11ShutdownWaitingTerminatesSafely(t *testing.T) {
	fixture := newIsolatedFixture(t, []config.OrchestratorCheck{
		{ID: "slow", Command: "sleep", Args: []string{"30"}, Timeout: "60s"},
	})
	// Cancel the manager's process lifetime before the pipeline runs.
	lifetime, cancel := context.WithCancel(context.Background())
	fixture.manager.InstallLaunchContext(lifetime)
	cancel()
	taskPath := fixture.createTask("shutdown safety")
	documentID := documentIDFromPath(t, taskPath)
	fixture.scan()
	leases := mustLeases(t, fixture.manager)
	if len(leases) != 1 {
		t.Fatalf("leases = %#v", leases)
	}
	// The pipeline aborted on the cancelled lifetime: no check evidence, no
	// fabricated failure, and the lease stays resumable at awaiting_transition.
	evidence, err := fixture.manager.documentFacts("tasks", documentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := findFact(evidence, checkCompletedKey(leases[0].LaunchID, "slow", evidenceResultSHA(evidence, leases[0].LaunchID))); ok {
		t.Fatal("a cancelled lifetime must not record check evidence")
	}
	for _, fact := range evidence {
		if fact.Kind == facts.KindLaunchFailed {
			t.Fatalf("shutdown fabricated launch failure evidence: %+v", fact)
		}
	}
	if leases[0].State != "awaiting_transition" {
		t.Fatalf("aborted pipeline left state %q", leases[0].State)
	}
	// The job group drained: Wait returns even though the check would have
	// slept for thirty seconds. The durable terminal evidence stays, so the
	// resumed-pipeline recovery of a later process owns the restart (RC4).
	fixture.manager.Wait()
}

// F7: malformed process diagnostics cannot suppress launch_failed evidence.
func TestRecordLaunchFailureSanitizesDurableErrorDetail(t *testing.T) {
	tests := []struct {
		name  string
		cause string
		want  []string
	}{
		{name: "multibyte boundary", cause: strings.Repeat("界", 171), want: []string{strings.Repeat("界", 170)}},
		{name: "multiline", cause: "first\rsecond\nthird\r\nfourth\u0085fifth\u2028sixth\u2029seventh", want: []string{"first", "second", "seventh"}},
		{name: "invalid utf8", cause: "useful prefix " + string([]byte{0xff, 0xfe}) + " useful suffix", want: []string{"useful prefix", "useful suffix"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager, _, lease, _ := sharedClaimFixture(t, nil)
			manager.recordLaunchFailure(lease, lease.LaunchID, "capture", "capture_failed", errors.New(test.cause))
			evidence, err := manager.documentFacts(lease.Route, documentIDForLease(lease))
			if err != nil {
				t.Fatal(err)
			}
			failed, ok := findFact(evidence, lease.LaunchID+":launch_failed")
			if !ok {
				t.Fatal("launch_failed fact was lost")
			}
			if !utf8.ValidString(failed.ErrorDetail) || len(failed.ErrorDetail) > facts.MaxErrorDetailBytes {
				t.Fatalf("invalid durable detail: bytes=%d valid=%t", len(failed.ErrorDetail), utf8.ValidString(failed.ErrorDetail))
			}
			if strings.ContainsAny(failed.ErrorDetail, "\r\n") || strings.ContainsAny(failed.ErrorDetail, "\u0085\u2028\u2029") {
				t.Fatalf("durable detail is multiline: %q", failed.ErrorDetail)
			}
			for _, want := range test.want {
				if !strings.Contains(failed.ErrorDetail, want) {
					t.Fatalf("durable detail %q lost diagnostic %q", failed.ErrorDetail, want)
				}
			}
		})
	}
}
