package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/workspace"
)

type blockedLeaseFixture struct {
	cfg       config.Config
	manager   *Manager
	runtime   *harness.Runtime
	spec      harness.RuntimeSpec
	provider  *availabilityHarness
	available atomic.Bool
	lease     Lease
	working   string
}

func newBlockedLeaseFixture(t *testing.T, blocked bool, heartbeat time.Time) *blockedLeaseFixture {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	task, err := Create(cfg, "tasks", "blocked lease fixture", "")
	if err != nil {
		t.Fatal(err)
	}
	working := cfg.StatePath("tasks", "working", filepath.Base(task))
	document, err := ReadDocument(task)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["status"] = "working"
	if err = WriteDocument(task, document); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(task, working); err != nil {
		t.Fatal(err)
	}

	fixture := &blockedLeaseFixture{cfg: cfg, working: working}
	registry := harness.NewRegistry()
	chat := newFakeRecipient()
	fixture.provider = &availabilityHarness{fakeHarness: newFakeRecipient()}
	fixture.provider.start = func() error {
		if !fixture.available.Load() {
			return errors.New("provider unavailable")
		}
		return nil
	}
	registry.Register("chat", func(harness.HarnessConfig) (harness.Harness, error) { return chat, nil })
	registry.Register("routed", func(harness.HarnessConfig) (harness.Harness, error) { return fixture.provider, nil })
	fixture.spec = harness.RuntimeSpec{
		Providers: map[harness.ProviderID]harness.HarnessConfig{"chat": {Name: "chat"}, "routed": {Name: "routed"}},
		Roles:     map[harness.Role]harness.ProviderID{harness.RoleChat: "chat", harness.RoleDeveloper: "routed"},
	}
	fixture.runtime, err = harness.NewRuntimeSpec(registry, fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.runtime.Close() })
	if err = fixture.runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.manager = New(cfg, fixture.runtime.AcquireRole(harness.RoleChat), extensions.Runner{})
	fixture.manager.HarnessRouter = fixture.runtime
	blockedAt := time.Date(2026, time.September, 18, 12, 0, 0, 0, time.UTC)
	fixture.manager.heartbeatNow = func() time.Time { return blockedAt }
	fixture.lease = Lease{
		ID: "blocked-existing", Route: "tasks", OwnerID: fixture.manager.ownerID,
		SessionKey: "tasks-blocked-existing", Phase: phaseTaskImplementation,
		RecoveryCount: 2, State: "processing", File: working,
		StartedAt: heartbeat.Add(-time.Minute), HeartbeatAt: heartbeat, LastError: "preserved diagnostic",
	}
	if blocked {
		fixture.lease.Blocked = &LeaseBlock{Reason: LeaseBlockedProviderUnavailable, Since: blockedAt}
	}
	if err = fixture.manager.saveLease(fixture.lease); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (f *blockedLeaseFixture) makeAvailable(t *testing.T) {
	t.Helper()
	f.available.Store(true)
	if err := f.runtime.Reconcile(context.Background(), f.spec); err != nil {
		t.Fatal(err)
	}
}

func fakeHarnessCalls(h *fakeHarness) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func TestBlockedLeaseClearsOnSuccessfulReservation(t *testing.T) {
	fixture := newBlockedLeaseFixture(t, true, time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC))
	fixture.makeAvailable(t)
	if err := fixture.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.manager.Wait()
	if calls := fakeHarnessCalls(fixture.provider.fakeHarness); calls != 1 {
		t.Fatalf("provider dispatches = %d, want 1", calls)
	}
	after, err := fixture.manager.loadLease(fixture.lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Blocked != nil {
		t.Fatalf("successful reservation retained block: %+v", after.Blocked)
	}
}

func TestBlockedLeaseRetriedBeforeStaleAfter(t *testing.T) {
	fresh := time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC)
	fixture := newBlockedLeaseFixture(t, true, fresh)
	fixture.makeAvailable(t)
	if err := fixture.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.manager.Wait()
	if calls := fakeHarnessCalls(fixture.provider.fakeHarness); calls != 1 {
		t.Fatalf("fresh blocked lease dispatches = %d, want 1", calls)
	}
	after, err := fixture.manager.loadLease(fixture.lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Blocked != nil {
		t.Fatalf("fresh blocked lease retained block: %+v", after.Blocked)
	}
}

func TestBlockedSinceIsStableAcrossScans(t *testing.T) {
	fixture := newBlockedLeaseFixture(t, false, time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC))
	firstBlockedTime := time.Date(2026, time.September, 18, 12, 0, 0, 0, time.UTC)
	if err := fixture.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := fixture.manager.loadLease(fixture.lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Blocked == nil || first.Blocked.Reason != LeaseBlockedProviderUnavailable || !first.Blocked.Since.Equal(firstBlockedTime) {
		t.Fatalf("first unavailable scan did not block at the injected time: %+v", first.Blocked)
	}
	bytesAfterFirstScan, err := os.ReadFile(fixture.manager.leasePath(fixture.lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	documentAfterFirstScan, err := os.ReadFile(fixture.working)
	if err != nil {
		t.Fatal(err)
	}
	laterBlockedTime := firstBlockedTime.Add(49 * time.Minute)
	fixture.manager.heartbeatNow = func() time.Time { return laterBlockedTime }
	if err = fixture.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := fixture.manager.loadLease(fixture.lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	bytesAfterSecondScan, err := os.ReadFile(fixture.manager.leasePath(fixture.lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	documentAfterSecondScan, err := os.ReadFile(fixture.working)
	if err != nil {
		t.Fatal(err)
	}
	if second.Blocked == nil || second.Blocked.Reason != first.Blocked.Reason || !second.Blocked.Since.Equal(firstBlockedTime) {
		t.Fatalf("repeated unavailable scan rewrote block: first=%+v second=%+v", first.Blocked, second.Blocked)
	}
	if second.Blocked.Since.Equal(laterBlockedTime) {
		t.Fatalf("blocked.since followed the advanced clock: got %s, want original %s", second.Blocked.Since, firstBlockedTime)
	}
	if !bytes.Equal(bytesAfterFirstScan, bytesAfterSecondScan) {
		t.Fatal("repeated unavailable scan rewrote lease bytes")
	}
	if !bytes.Equal(documentAfterFirstScan, documentAfterSecondScan) {
		t.Fatal("unavailable retry rewrote workflow document")
	}
	if calls := fakeHarnessCalls(fixture.provider.fakeHarness); calls != 0 {
		t.Fatalf("unavailable provider calls = %d, want 0", calls)
	}
}

func TestBlockedLeaseSurvivesRestart(t *testing.T) {
	fixture := newBlockedLeaseFixture(t, false, time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err := fixture.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	blocked, err := fixture.manager.loadLease(fixture.lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Blocked == nil {
		t.Fatal("unavailable lease was not blocked before restart")
	}
	beforeUnavailable, err := os.ReadFile(fixture.manager.leasePath(fixture.lease.ID))
	if err != nil {
		t.Fatal(err)
	}

	restarted := New(fixture.cfg, fixture.runtime.AcquireRole(harness.RoleChat), extensions.Runner{})
	restarted.HarnessRouter = fixture.runtime
	loaded, err := restarted.loadLease(fixture.lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Blocked == nil || loaded.Blocked.Reason != blocked.Blocked.Reason || !loaded.Blocked.Since.Equal(blocked.Blocked.Since) {
		t.Fatalf("restart lost block: before=%+v after=%+v", blocked.Blocked, loaded.Blocked)
	}
	if err = restarted.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	afterUnavailable, err := os.ReadFile(restarted.leasePath(fixture.lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeUnavailable, afterUnavailable) {
		t.Fatal("restart rewrote unchanged unavailable block")
	}

	fixture.makeAvailable(t)
	if err = restarted.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted.Wait()
	after, err := restarted.loadLease(fixture.lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Blocked != nil {
		t.Fatalf("successful post-restart reservation retained block: %+v", after.Blocked)
	}
}

func TestBlockedIgnoresDocumentFrontMatter(t *testing.T) {
	t.Run("cannot author", func(t *testing.T) {
		root := t.TempDir()
		if err := workspace.Init(root, false); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(config.PathForRoot(root))
		if err != nil {
			t.Fatal(err)
		}
		task, err := Create(cfg, "tasks", "spoof blocked state", "")
		if err != nil {
			t.Fatal(err)
		}
		document, err := ReadDocument(task)
		if err != nil {
			t.Fatal(err)
		}
		document.FrontMatter["blocked"] = LeaseBlockedProviderUnavailable
		document.FrontMatter["blocked_reason"] = "spoofed"
		if err = WriteDocument(task, document); err != nil {
			t.Fatal(err)
		}
		provider := newFakeRecipient()
		manager := New(cfg, provider, extensions.Runner{})
		if err = manager.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		manager.Wait()
		leases, err := manager.loadLeases()
		if err != nil || len(leases) != 1 {
			t.Fatalf("leases = %+v, %v", leases, err)
		}
		if leases[0].Blocked != nil {
			t.Fatalf("document authored durable block: %+v", leases[0].Blocked)
		}
	})

	t.Run("cannot clear", func(t *testing.T) {
		fixture := newBlockedLeaseFixture(t, true, time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC))
		document, err := ReadDocument(fixture.working)
		if err != nil {
			t.Fatal(err)
		}
		document.FrontMatter["blocked"] = ""
		document.FrontMatter["blocked_reason"] = ""
		if err = WriteDocument(fixture.working, document); err != nil {
			t.Fatal(err)
		}
		if err = fixture.manager.ScanOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		after, err := fixture.manager.loadLease(fixture.lease.ID)
		if err != nil {
			t.Fatal(err)
		}
		if after.Blocked == nil || after.Blocked.Reason != fixture.lease.Blocked.Reason || !after.Blocked.Since.Equal(fixture.lease.Blocked.Since) {
			t.Fatalf("document cleared durable block: before=%+v after=%+v", fixture.lease.Blocked, after.Blocked)
		}
	})
}

func TestLegacyLeaseJSONUnchanged(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	lease := Lease{ID: "unblocked", Route: "tasks", File: "task.md", SessionKey: "task-session", State: "processing"}
	if err = manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(manager.leasePath(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(`"blocked"`)) {
		t.Fatalf("nil block changed lease JSON: %s", data)
	}

	legacy := []byte("{\n  \"id\": \"legacy\",\n  \"route\": \"tasks\",\n  \"file\": \"task.md\",\n  \"session_key\": \"legacy-session\",\n  \"state\": \"processing\",\n  \"recovery_count\": 0\n}\n")
	if err = os.WriteFile(manager.leasePath("legacy"), legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := manager.loadLease("legacy")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Blocked != nil {
		t.Fatalf("legacy lease loaded a block: %+v", loaded.Blocked)
	}
}

func TestShutdownDoesNotMarkBlocked(t *testing.T) {
	fixture := newBlockedLeaseFixture(t, false, time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC))
	before, err := os.ReadFile(fixture.manager.leasePath(fixture.lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fixture.manager.dispatch(ctx, workflowRoutes()[0], fixture.lease, true)
	if err = fixture.manager.recoverStale(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := fixture.manager.loadLease(fixture.lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Blocked != nil {
		t.Fatalf("cancelled context blocked lease: %+v", after.Blocked)
	}
	afterBytes, err := os.ReadFile(fixture.manager.leasePath(fixture.lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, afterBytes) {
		t.Fatal("cancelled unavailable reservation rewrote lease")
	}
}
