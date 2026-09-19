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

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/harness"
)

const malformedFragment = "this is not a Markdown document\n"

func transitionLease(t *testing.T, cfg config.Config, manager *Manager, title string) (Lease, string, string) {
	t.Helper()
	todo, err := Create(cfg, "tasks", title, "")
	if err != nil {
		t.Fatal(err)
	}
	document, err := ReadDocument(todo)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Dir(filepath.Dir(todo))
	name := filepath.Base(todo)
	reviewing := filepath.Join(base, "reviewing", name)
	done := filepath.Join(base, "done", name)
	if err := moveDocument(todo, done, "done", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	id := leaseID("tasks:"+phaseTaskReview, documentID(document))
	lease := Lease{ID: id, ClaimID: id, DocumentType: "task", Route: "tasks", File: reviewing,
		SessionKey: phaseSessionKey("tasks", documentID(document), phaseTaskReview, 1), State: "awaiting_transition",
		Phase: phaseTaskReview, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	return lease, done, filepath.Join(base, "review", name)
}

func writeMalformedFragment(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(malformedFragment), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertMalformedFragment(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || string(data) != malformedFragment {
		t.Fatalf("malformed fragment changed at %s: %q, %v", path, data, err)
	}
}

func TestReconcileIgnoresUnreadableFragmentAtEarlierCandidatePath(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	lease, done, fragment := transitionLease(t, cfg, manager, "later valid transition")
	writeMalformedFragment(t, fragment)
	var logs []string
	manager.Log = func(message string) { logs = append(logs, message) }
	var finished []int
	manager.JobFinished = func(id int) { finished = append(finished, id) }
	manager.setRuntimeJob(lease.ID, 42)

	err := manager.reconcileTransitions(context.Background())
	manager.Wait()
	if err == nil || !strings.Contains(err.Error(), lease.ID) || !strings.Contains(err.Error(), fragment) || !strings.Contains(err.Error(), "markdown document must begin with YAML front matter") {
		t.Fatalf("reconcile error lacks lease/path: %v", err)
	}
	if _, err := manager.loadLease(lease.ID); !os.IsNotExist(err) {
		t.Fatalf("successful transition retained lease: %v", err)
	}
	if _, err := ReadDocument(done); err != nil {
		t.Fatalf("later valid candidate changed: %v", err)
	}
	if len(finished) != 1 || finished[0] != 42 {
		t.Fatalf("finished jobs = %v", finished)
	}
	assertMalformedFragment(t, fragment)
	if !strings.Contains(strings.Join(logs, "\n"), "skipped") || !strings.Contains(strings.Join(logs, "\n"), fragment) {
		t.Fatalf("unreadable candidate was not logged: %v", logs)
	}
}

func TestScanIsolationRetainsBadLeaseAndReconcilesAnother(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	bad, badDone, badFragment := transitionLease(t, cfg, manager, "bad transition")
	good, _, _ := transitionLease(t, cfg, manager, "good transition")
	repairBytes, err := os.ReadFile(badDone)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(badDone); err != nil {
		t.Fatal(err)
	}
	writeMalformedFragment(t, badFragment)
	before, err := os.ReadFile(manager.leasePath(bad.ID))
	if err != nil {
		t.Fatal(err)
	}
	var finished []int
	manager.JobFinished = func(id int) { finished = append(finished, id) }
	manager.setRuntimeJob(bad.ID, 1)
	manager.setRuntimeJob(good.ID, 2)

	err = manager.reconcileTransitions(context.Background())
	manager.Wait()
	if err == nil || !strings.Contains(err.Error(), bad.ID) || !strings.Contains(err.Error(), "no readable transition candidate") {
		t.Fatalf("bad lease error = %v", err)
	}
	after, err := os.ReadFile(manager.leasePath(bad.ID))
	if err != nil || string(after) != string(before) || manager.runtimeJob(bad.ID) != 1 {
		t.Fatalf("bad lease/job changed: %v, job=%d", err, manager.runtimeJob(bad.ID))
	}
	if _, err := manager.loadLease(good.ID); !os.IsNotExist(err) {
		t.Fatalf("good lease not reconciled: %v", err)
	}
	if len(finished) != 1 || finished[0] != 2 {
		t.Fatalf("finished jobs = %v", finished)
	}
	assertMalformedFragment(t, badFragment)
	if err := manager.reconcileTransitions(context.Background()); err == nil {
		t.Fatal("second scan hid malformed candidate")
	}
	manager.Wait()
	assertMalformedFragment(t, badFragment)
	stillUnchanged, err := os.ReadFile(manager.leasePath(bad.ID))
	if err != nil || string(stillUnchanged) != string(before) {
		t.Fatalf("second scan changed bad lease: %v", err)
	}
	if err := os.Remove(badFragment); err != nil {
		t.Fatal(err)
	}
	// A repaired candidate is processed on the next scan, without recreating
	// the lease or finishing the live job early.
	if err := os.WriteFile(badDone, repairBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := manager.reconcileTransitions(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	if _, err := manager.loadLease(bad.ID); !os.IsNotExist(err) || manager.runtimeJob(bad.ID) != 0 {
		t.Fatalf("repaired lease/job not finished: %v, job=%d", err, manager.runtimeJob(bad.ID))
	}
	if len(finished) != 2 || finished[1] != 1 {
		t.Fatalf("repair finished jobs = %v", finished)
	}
}

func TestScanIsolationContinuesAfterSelectedCandidateReconcileFailure(t *testing.T) {
	cfg, _, manager := workflowTestManager(t)
	bad, badDone, badReview := transitionLease(t, cfg, manager, "selected candidate cannot redirect")
	if err := moveDocument(badDone, badReview, "review", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// The review candidate parses successfully. Reconciliation then tries to
	// redirect it to todo, but cannot open its stable provider-turn lock.
	lockPath := providerTurnLockPath(badReview)
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lockPath, 0o700); err != nil {
		t.Fatal(err)
	}
	good, goodDone, _ := transitionLease(t, cfg, manager, "independent selected candidate")
	for _, item := range []struct {
		lease *Lease
		id    string
	}{{&bad, "a-selected-candidate-fails"}, {&good, "z-independent-candidate"}} {
		if err := os.Remove(manager.leasePath(item.lease.ID)); err != nil {
			t.Fatal(err)
		}
		item.lease.ID, item.lease.ClaimID = item.id, item.id
		if err := manager.saveLease(*item.lease); err != nil {
			t.Fatal(err)
		}
	}
	badLeaseBefore, err := os.ReadFile(manager.leasePath(bad.ID))
	if err != nil {
		t.Fatal(err)
	}
	badDocumentBefore, err := os.ReadFile(badReview)
	if err != nil {
		t.Fatal(err)
	}
	var finished []int
	manager.JobFinished = func(id int) { finished = append(finished, id) }
	manager.setRuntimeJob(bad.ID, 71)
	manager.setRuntimeJob(good.ID, 72)

	err = manager.ScanOnce(context.Background())
	manager.Wait()
	if err == nil || !strings.Contains(err.Error(), bad.ID) || !strings.Contains(err.Error(), badReview) || !strings.Contains(err.Error(), "open provider-turn lock") {
		t.Fatalf("selected candidate failure was not returned: %v", err)
	}
	badLeaseAfter, err := os.ReadFile(manager.leasePath(bad.ID))
	if err != nil || string(badLeaseAfter) != string(badLeaseBefore) || manager.runtimeJob(bad.ID) != 71 {
		t.Fatalf("failed lease/job changed: %v, job=%d", err, manager.runtimeJob(bad.ID))
	}
	badDocumentAfter, err := os.ReadFile(badReview)
	if err != nil || string(badDocumentAfter) != string(badDocumentBefore) {
		t.Fatalf("failed candidate document changed: %v", err)
	}
	if _, err := os.Stat(badDone); !os.IsNotExist(err) {
		t.Fatalf("failed candidate moved: %v", err)
	}
	if _, err := manager.loadLease(good.ID); !os.IsNotExist(err) || manager.runtimeJob(good.ID) != 0 {
		t.Fatalf("independent lease/job did not reconcile: %v, job=%d", err, manager.runtimeJob(good.ID))
	}
	goodDocument, err := ReadDocument(goodDone)
	if err != nil || stringField(goodDocument, "status") != "done" {
		t.Fatalf("independent document did not stay done: %#v, %v", goodDocument.FrontMatter, err)
	}
	if len(finished) != 1 || finished[0] != 72 {
		t.Fatalf("finished jobs = %v", finished)
	}
}

func TestScanIsolationContinuesToQueuedWorkAndJoinsCancellation(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	bad, badDone, fragment := transitionLease(t, cfg, manager, "bad scan transition")
	if err := os.Remove(badDone); err != nil {
		t.Fatal(err)
	}
	writeMalformedFragment(t, fragment)
	if _, err := Create(cfg, "tasks", "independent queued task", ""); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	fake.beforeEmit = func() { once.Do(func() { close(entered) }); <-release }
	t.Cleanup(func() { close(release); manager.Wait() })
	if err := manager.ScanOnce(context.Background()); err == nil || !strings.Contains(err.Error(), bad.ID) {
		t.Fatalf("scan did not report bad lease: %v", err)
	}
	<-entered
	fake.mu.Lock()
	calls := fake.calls
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("queued work dispatches = %d", calls)
	}
	assertMalformedFragment(t, fragment)

	// The first scan has already claimed its queued task. This second one must
	// remain untouched when reconciliation cancels the next scan before queues.
	queued, err := Create(cfg, "tasks", "must stay queued after cancellation", "")
	if err != nil {
		t.Fatal(err)
	}
	queuedBefore, err := os.ReadFile(queued)
	if err != nil {
		t.Fatal(err)
	}
	queuedDocument, err := ReadDocument(queued)
	if err != nil {
		t.Fatal(err)
	}
	queuedLeaseID := leaseID("tasks:"+phaseTaskImplementation, documentID(queuedDocument))
	queuedWorking := filepath.Join(filepath.Dir(filepath.Dir(queued)), "working", filepath.Base(queued))
	ctx, cancel := context.WithCancel(context.Background())
	manager.Log = func(message string) {
		if strings.Contains(message, "unreadable transition candidate") {
			cancel()
		}
	}
	err = manager.ScanOnce(ctx)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), bad.ID) {
		t.Fatalf("cancelled scan did not join prior lease error: %v", err)
	}
	fake.mu.Lock()
	calls = fake.calls
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("cancelled scan started more work: %d dispatches", calls)
	}
	queuedAfter, err := os.ReadFile(queued)
	if err != nil || string(queuedAfter) != string(queuedBefore) {
		t.Fatalf("cancelled scan changed queued document: %v", err)
	}
	if _, err := os.Stat(queuedWorking); !os.IsNotExist(err) {
		t.Fatalf("cancelled scan claimed queued document: %v", err)
	}
	if _, err := manager.loadLease(queuedLeaseID); !os.IsNotExist(err) || manager.runtimeJob(queuedLeaseID) != 0 {
		t.Fatalf("cancelled scan created queued lease/job: %v, job=%d", err, manager.runtimeJob(queuedLeaseID))
	}
}

func TestInterruptedClaimsIsolationKeepsMalformedLeaseRetryable(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	first, err := Create(cfg, "tasks", "unreadable interrupted claim", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := Create(cfg, "tasks", "readable interrupted claim", "")
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Dir(filepath.Dir(first))
	firstWorking := filepath.Join(base, "working", filepath.Base(first))
	secondWorking := filepath.Join(base, "working", filepath.Base(second))
	if err := os.Rename(first, firstWorking); err != nil {
		t.Fatal(err)
	}
	writeMalformedFragment(t, firstWorking)
	if _, err := ClaimDocument(second, secondWorking, "working", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	bad := Lease{ID: "claim-bad", Route: "tasks", File: firstWorking, SessionKey: "claim-bad", State: "claiming", Phase: phaseTaskImplementation, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now}
	good := Lease{ID: "claim-good", Route: "tasks", File: secondWorking, SessionKey: "claim-good", State: "claiming", Phase: phaseTaskImplementation, ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now}
	for _, lease := range []Lease{bad, good} {
		if err := manager.saveLease(lease); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadFile(manager.leasePath(bad.ID))
	if err != nil {
		t.Fatal(err)
	}
	err = manager.resumeInterruptedClaims(context.Background())
	if err == nil || !strings.Contains(err.Error(), bad.ID) {
		t.Fatalf("resume error = %v", err)
	}
	manager.Wait()
	after, err := os.ReadFile(manager.leasePath(bad.ID))
	if err != nil || string(after) != string(before) {
		t.Fatalf("bad lease changed: %v", err)
	}
	resumed, err := manager.loadLease(good.ID)
	if err != nil || resumed.State == "claiming" {
		t.Fatalf("good claim not resumed: %#v, %v", resumed, err)
	}
	fake.mu.Lock()
	calls := fake.calls
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("resumed dispatches = %d", calls)
	}
	assertMalformedFragment(t, firstWorking)
}

func TestOrphanClaimsIsolationRecoversReadablePeer(t *testing.T) {
	cfg, fake, manager := workflowTestManager(t)
	good, err := Create(cfg, "tasks", "readable orphan claim", "")
	if err != nil {
		t.Fatal(err)
	}
	working := filepath.Join(filepath.Dir(filepath.Dir(good)), "working")
	fragment := filepath.Join(working, "malformed.md")
	writeMalformedFragment(t, fragment)
	valid := filepath.Join(working, filepath.Base(good))
	if _, err := ClaimDocument(good, valid, "working", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	err = manager.recoverOrphanClaims(context.Background())
	if err == nil || !strings.Contains(err.Error(), fragment) {
		t.Fatalf("orphan error = %v", err)
	}
	manager.Wait()
	assertMalformedFragment(t, fragment)
	fake.mu.Lock()
	calls := fake.calls
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("valid orphan dispatches = %d", calls)
	}
	if !manager.hasLeaseForFile(valid) {
		t.Fatal("valid orphan did not receive lease")
	}
}

// Keep a reservation fake here so resume's skipped-claim path also exercises
// the existing release discipline without a provider process.
type countingReservation struct {
	*fakeHarness
	mu       sync.Mutex
	reserved int
	released int
}

func (c *countingReservation) ReserveExecution(string) (harness.ProviderID, func(), error) {
	c.mu.Lock()
	c.reserved++
	c.mu.Unlock()
	return "replacement", func() { c.mu.Lock(); c.released++; c.mu.Unlock() }, nil
}

func (c *countingReservation) ReserveOwnedExecution(string, harness.ProviderID) (func(), error) {
	return nil, harness.ErrProviderAbsent
}

func TestInterruptedClaimsIsolationDoesNotPersistSkippedOwnerMove(t *testing.T) {
	cfg, _, _ := workflowTestManager(t)
	fake := &countingReservation{fakeHarness: newFakeRecipient()}
	manager := New(cfg, fake, extensions.Runner{})
	path := filepath.Join(filepath.Dir(cfg.Resolve(workflowRoutes()[0].Source)), "working", "unreadable.md")
	writeMalformedFragment(t, path)
	now := time.Now().UTC()
	lease := Lease{ID: "owner-move-skipped", Route: "tasks", File: path, SessionKey: "owner-move-skipped", State: "claiming", Phase: phaseTaskImplementation, Provider: "old-owner", ThreadID: "old-thread", ClaimAttempt: 1, StartedAt: now, HeartbeatAt: now}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(manager.leasePath(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.resumeInterruptedClaims(context.Background()); err == nil {
		t.Fatal("expected malformed claim error")
	}
	after, err := os.ReadFile(manager.leasePath(lease.ID))
	if err != nil || string(after) != string(before) {
		t.Fatalf("skipped move persisted: %v", err)
	}
	fake.mu.Lock()
	reserved, released := fake.reserved, fake.released
	fake.mu.Unlock()
	if reserved != 1 || released != 1 {
		t.Fatalf("reservations = %d, releases = %d", reserved, released)
	}
}
