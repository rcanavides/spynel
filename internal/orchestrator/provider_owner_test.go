package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/workspace"
)

func providerOwnerConfig(t *testing.T) config.Config {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func providerOwnerRuntime(t *testing.T, spec harness.RuntimeSpec) *harness.Runtime {
	t.Helper()
	registry := harness.NewRegistry()
	registry.Register("chat", func(harness.HarnessConfig) (harness.Harness, error) { return newFakeRecipient(), nil })
	registry.Register("routed", func(harness.HarnessConfig) (harness.Harness, error) { return newFakeRecipient(), nil })
	runtime, err := harness.NewRuntimeSpec(registry, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if err = runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func providerOwnerManager(cfg config.Config, runtime *harness.Runtime) *Manager {
	manager := New(cfg, runtime.AcquireRole(harness.RoleChat), extensions.Runner{})
	manager.HarnessRouter = runtime
	return manager
}

type pinnedOwnerFixture struct {
	runtime   *harness.Runtime
	manager   *Manager
	registry  *harness.Registry
	spec      harness.RuntimeSpec
	chat      *fakeHarness
	owner     *availabilityHarness
	alternate *availabilityHarness
}

func newPinnedOwnerFixture(t *testing.T, cfg config.Config, includeOwner bool, ownerStart func() error) *pinnedOwnerFixture {
	t.Helper()
	fixture := &pinnedOwnerFixture{
		registry:  harness.NewRegistry(),
		chat:      newFakeRecipient(),
		owner:     &availabilityHarness{fakeHarness: newFakeRecipient(), start: ownerStart},
		alternate: &availabilityHarness{fakeHarness: newFakeRecipient()},
	}
	fixture.registry.Register("chat", func(harness.HarnessConfig) (harness.Harness, error) { return fixture.chat, nil })
	fixture.registry.Register("routed", func(cfg harness.HarnessConfig) (harness.Harness, error) {
		switch cfg.Model {
		case "owner":
			return fixture.owner, nil
		case "alternate":
			return fixture.alternate, nil
		default:
			return nil, errors.New("unexpected routed provider fixture")
		}
	})
	fixture.spec = harness.RuntimeSpec{
		Providers: map[harness.ProviderID]harness.HarnessConfig{
			"chat":    {Name: "chat"},
			"glm-dev": {Name: "routed", Model: "alternate"},
		},
		Roles: map[harness.Role]harness.ProviderID{
			harness.RoleChat:      "chat",
			harness.RoleDeveloper: "glm-dev",
		},
	}
	if includeOwner {
		fixture.spec.Providers["codex-dev"] = harness.HarnessConfig{Name: "routed", Model: "owner"}
	}
	var err error
	fixture.runtime, err = harness.NewRuntimeSpec(fixture.registry, fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.runtime.Close() })
	if err = fixture.runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.manager = providerOwnerManager(cfg, fixture.runtime)
	return fixture
}

func providerOwnerTask(t *testing.T, cfg config.Config, title, status string) string {
	t.Helper()
	path, err := Create(cfg, "tasks", title, "")
	if err != nil {
		t.Fatal(err)
	}
	if status == "todo" {
		return path
	}
	document, err := ReadDocument(path)
	if err != nil {
		t.Fatal(err)
	}
	document.FrontMatter["status"] = status
	if err = WriteDocument(path, document); err != nil {
		t.Fatal(err)
	}
	target := cfg.StatePath("tasks", status, filepath.Base(path))
	if err = os.Rename(path, target); err != nil {
		t.Fatal(err)
	}
	return target
}

func TestAdmissionPersistsProviderInstanceNotKind(t *testing.T) {
	cfg := providerOwnerConfig(t)
	providerOwnerTask(t, cfg, "persist provider instance", "todo")
	spec := harness.RuntimeSpec{
		Providers: map[harness.ProviderID]harness.HarnessConfig{
			"chat":      {Name: "chat"},
			"codex-dev": {Name: "routed"},
		},
		Roles: map[harness.Role]harness.ProviderID{
			harness.RoleChat:      "chat",
			harness.RoleDeveloper: "codex-dev",
		},
	}
	manager := providerOwnerManager(cfg, providerOwnerRuntime(t, spec))
	route := workflowRoutes()[0]
	if err := manager.scanPhaseQueue(context.Background(), route, cfg.Resolve(route.Source), cfg.Resolve(route.Working), phaseTaskImplementation); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 1 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	if leases[0].Provider != "codex-dev" || leases[0].Provider == "routed" {
		t.Fatalf("provider owner = %q, want instance codex-dev", leases[0].Provider)
	}
	raw, err := os.ReadFile(manager.leasePath(leases[0].ID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"provider": "codex-dev"`)) {
		t.Fatalf("lease JSON lacks exact provider instance:\n%s", raw)
	}
}

func TestSameKindInstancesPersistDistinctOwners(t *testing.T) {
	cfg := providerOwnerConfig(t)
	providerOwnerTask(t, cfg, "implementation owner", "todo")
	providerOwnerTask(t, cfg, "review owner", "review")
	spec := harness.RuntimeSpec{
		Providers: map[harness.ProviderID]harness.HarnessConfig{
			"chat":        {Name: "chat"},
			"claude-arch": {Name: "routed"},
			"claude-rev":  {Name: "routed"},
		},
		Roles: map[harness.Role]harness.ProviderID{
			harness.RoleChat:      "chat",
			harness.RoleDeveloper: "claude-arch",
			harness.RoleReviewer:  "claude-rev",
		},
	}
	manager := providerOwnerManager(cfg, providerOwnerRuntime(t, spec))
	route := workflowRoutes()[0]
	if err := manager.scanPhaseQueue(context.Background(), route, cfg.Resolve(route.Source), cfg.Resolve(route.Working), phaseTaskImplementation); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	base := filepath.Dir(cfg.Resolve(route.Source))
	if err := manager.scanPhaseQueue(context.Background(), route, filepath.Join(base, "review"), filepath.Join(base, "reviewing"), phaseTaskReview); err != nil {
		t.Fatal(err)
	}
	manager.Wait()
	leases, err := manager.loadLeases()
	if err != nil || len(leases) != 2 {
		t.Fatalf("leases = %#v, %v", leases, err)
	}
	owners := map[string]harness.ProviderID{}
	for _, lease := range leases {
		owners[normalizeLeasePhase(lease.Route, lease.Phase)] = lease.Provider
	}
	if owners[phaseTaskImplementation] != "claude-arch" || owners[phaseTaskReview] != "claude-rev" {
		t.Fatalf("same-kind owners collapsed: %#v", owners)
	}
	for phase, owner := range owners {
		if owner == "routed" {
			t.Fatalf("phase %s persisted harness kind instead of instance", phase)
		}
	}
}

func TestRecoveryPinsDurableOwnerAcrossRoleRemapAndSameKind(t *testing.T) {
	cfg := providerOwnerConfig(t)
	working := providerOwnerTask(t, cfg, "pinned recovery owner", "working")
	fixture := newPinnedOwnerFixture(t, cfg, true, nil)
	lease := Lease{
		ID: "pinned-owner", Route: "tasks", File: working, SessionKey: "pinned-owner-session",
		Phase: phaseTaskImplementation, State: "processing", Provider: "codex-dev", ThreadID: "existing-thread",
		StartedAt:   time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		HeartbeatAt: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := fixture.manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	if err := fixture.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.manager.Wait()
	if calls := fakeHarnessCalls(fixture.owner.fakeHarness); calls != 1 {
		t.Fatalf("durable owner dispatches = %d, want 1", calls)
	}
	if calls := fakeHarnessCalls(fixture.alternate.fakeHarness); calls != 0 {
		t.Fatalf("role-mapped alternate dispatches = %d, want 0", calls)
	}
	after, err := fixture.manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Provider != "codex-dev" || after.Provider == "routed" || after.Provider == "glm-dev" {
		t.Fatalf("recovery owner = %q, want codex-dev", after.Provider)
	}
}

func TestOwnedRecoveryUnavailableBlocksWithoutFallback(t *testing.T) {
	cfg := providerOwnerConfig(t)
	working := providerOwnerTask(t, cfg, "unavailable pinned owner", "working")
	fixture := newPinnedOwnerFixture(t, cfg, true, func() error { return errors.New("owner unavailable") })
	blockedAt := time.Date(2026, time.September, 18, 12, 0, 0, 0, time.UTC)
	fixture.manager.heartbeatNow = func() time.Time { return blockedAt }
	lease := Lease{
		ID: "unavailable-owner", Route: "tasks", File: working, SessionKey: "unavailable-owner-session",
		Phase: phaseTaskImplementation, State: "processing", Provider: "codex-dev", ThreadID: "preserved-thread",
		StartedAt:   time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		HeartbeatAt: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := fixture.manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	if err := fixture.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := fixture.manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Blocked == nil || first.Blocked.Reason != LeaseBlockedProviderUnavailable || !first.Blocked.Since.Equal(blockedAt) {
		t.Fatalf("unavailable owner block = %+v", first.Blocked)
	}
	expected := lease
	expected.Blocked = first.Blocked
	if !reflect.DeepEqual(first, expected) {
		t.Fatalf("unavailable owner changed lease fields beyond Blocked:\n got: %#v\nwant: %#v", first, expected)
	}
	bytesAfterFirst, err := os.ReadFile(fixture.manager.leasePath(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	fixture.manager.heartbeatNow = func() time.Time { return blockedAt.Add(time.Hour) }
	if err = fixture.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	bytesAfterSecond, err := os.ReadFile(fixture.manager.leasePath(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bytesAfterFirst, bytesAfterSecond) {
		t.Fatal("repeated unavailable owned recovery rewrote lease")
	}
	if fakeHarnessCalls(fixture.owner.fakeHarness) != 0 || fakeHarnessCalls(fixture.alternate.fakeHarness) != 0 {
		t.Fatal("unavailable owned recovery dispatched or fell back")
	}
}

func TestOwnedRecoveryFencedPreservesLeaseWithoutFallback(t *testing.T) {
	cfg := providerOwnerConfig(t)
	working := providerOwnerTask(t, cfg, "fenced pinned owner", "working")
	fixture := newPinnedOwnerFixture(t, cfg, true, nil)
	blockedAt := time.Date(2026, time.September, 18, 12, 0, 0, 0, time.UTC)
	lease := Lease{
		ID: "fenced-owner", Route: "tasks", File: working, SessionKey: "fenced-owner-session",
		Phase: phaseTaskImplementation, State: "processing", Provider: "codex-dev", ThreadID: "preserved-thread",
		StartedAt:   time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		HeartbeatAt: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		Blocked:     &LeaseBlock{Reason: LeaseBlockedProviderUnavailable, Since: blockedAt},
	}
	if err := fixture.manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(fixture.manager.leasePath(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	entered, proceed := make(chan struct{}), make(chan struct{})
	candidate := &availabilityHarness{fakeHarness: newFakeRecipient(), start: func() error {
		close(entered)
		<-proceed
		return nil
	}}
	fixture.registry.Register("routed", func(harness.HarnessConfig) (harness.Harness, error) { return candidate, nil })
	next := fixture.spec
	next.Providers = make(map[harness.ProviderID]harness.HarnessConfig, len(fixture.spec.Providers))
	for id, provider := range fixture.spec.Providers {
		next.Providers[id] = provider
	}
	ownerConfig := next.Providers["codex-dev"]
	ownerConfig.Sandbox = "read-only"
	next.Providers["codex-dev"] = ownerConfig
	done := make(chan error, 1)
	go func() { done <- fixture.runtime.Reconcile(context.Background(), next) }()
	<-entered
	recoverErr := fixture.manager.recoverStale(context.Background())
	close(proceed)
	reconcileErr := <-done
	if recoverErr != nil {
		t.Fatal(recoverErr)
	}
	if reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	after, err := os.ReadFile(fixture.manager.leasePath(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("fenced owned recovery mutated durable lease")
	}
	if fakeHarnessCalls(fixture.owner.fakeHarness) != 0 || fakeHarnessCalls(fixture.alternate.fakeHarness) != 0 {
		t.Fatal("fenced owned recovery dispatched or fell back")
	}
}

func TestOwnedRecoveryAbsentPreservesLeaseWithoutFallback(t *testing.T) {
	cfg := providerOwnerConfig(t)
	working := providerOwnerTask(t, cfg, "absent pinned owner", "working")
	fixture := newPinnedOwnerFixture(t, cfg, false, nil)
	lease := Lease{
		ID: "absent-owner", Route: "tasks", File: working, SessionKey: "absent-owner-session",
		Phase: phaseTaskImplementation, State: "processing", Provider: "codex-dev", ThreadID: "preserved-thread",
		StartedAt:   time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		HeartbeatAt: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := fixture.manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(fixture.manager.leasePath(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	if err = fixture.manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(fixture.manager.leasePath(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("absent owned recovery mutated durable lease")
	}
	loaded, err := fixture.manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Provider != lease.Provider || loaded.ThreadID != lease.ThreadID || loaded.Blocked != nil {
		t.Fatalf("absent owner changed lease: %#v", loaded)
	}
	if fakeHarnessCalls(fixture.alternate.fakeHarness) != 0 || fakeHarnessCalls(fixture.chat) != 0 {
		t.Fatal("absent owned recovery fell back")
	}
}

func TestFailedReservationDoesNotPersistProvider(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		name := "unavailable"
		if fenced {
			name = "fenced"
		}
		t.Run(name, func(t *testing.T) {
			cfg := providerOwnerConfig(t)
			working := providerOwnerTask(t, cfg, "existing legacy lease", "working")
			queued := providerOwnerTask(t, cfg, "queued admission", "todo")
			registry := harness.NewRegistry()
			chat := newFakeRecipient()
			provider := &availabilityHarness{fakeHarness: newFakeRecipient()}
			if !fenced {
				provider.start = func() error { return errors.New("provider unavailable") }
			}
			registry.Register("chat", func(harness.HarnessConfig) (harness.Harness, error) { return chat, nil })
			registry.Register("routed", func(harness.HarnessConfig) (harness.Harness, error) { return provider, nil })
			spec := harness.RuntimeSpec{
				Providers: map[harness.ProviderID]harness.HarnessConfig{"chat": {Name: "chat"}, "owner-instance": {Name: "routed"}},
				Roles:     map[harness.Role]harness.ProviderID{harness.RoleChat: "chat", harness.RoleDeveloper: "owner-instance"},
			}
			runtime, err := harness.NewRuntimeSpec(registry, spec)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			if err = runtime.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			var settle func()
			if fenced {
				entered, release := make(chan struct{}), make(chan struct{})
				done := make(chan error, 1)
				registry.Register("routed", func(harness.HarnessConfig) (harness.Harness, error) {
					return &availabilityHarness{fakeHarness: newFakeRecipient(), start: func() error { close(entered); <-release; return nil }}, nil
				})
				next := spec.Providers["owner-instance"]
				next.Sandbox = "read-only"
				spec.Providers["owner-instance"] = next
				go func() { done <- runtime.Reconcile(context.Background(), spec) }()
				<-entered
				settle = func() {
					close(release)
					if reconcileErr := <-done; reconcileErr != nil {
						t.Error(reconcileErr)
					}
				}
				defer settle()
			}
			manager := providerOwnerManager(cfg, runtime)
			lease := Lease{
				ID: "legacy-empty-owner", Route: "tasks", File: working, SessionKey: "legacy-recovery",
				Phase: phaseTaskImplementation, State: "processing", OwnerID: manager.ownerID,
				StartedAt:   time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
				HeartbeatAt: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
			}
			if err = manager.saveLease(lease); err != nil {
				t.Fatal(err)
			}
			route := workflowRoutes()[0]
			if err = manager.scanPhaseQueue(context.Background(), route, cfg.Resolve(route.Source), cfg.Resolve(route.Working), phaseTaskImplementation); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(queued); err != nil {
				t.Fatalf("failed reservation claimed queued task: %v", err)
			}
			if err = manager.recoverStale(context.Background()); err != nil {
				t.Fatal(err)
			}
			after, err := manager.loadLease(lease.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Provider != "" {
				t.Fatalf("failed reservation persisted provider %q", after.Provider)
			}
			if fenced {
				if after.Blocked != nil {
					t.Fatalf("fenced reservation blocked lease: %+v", after.Blocked)
				}
			} else if after.Blocked == nil || after.Blocked.Reason != LeaseBlockedProviderUnavailable {
				t.Fatalf("unavailable reservation lost C6 block: %+v", after.Blocked)
			}
			leases, err := manager.loadLeases()
			if err != nil || len(leases) != 1 {
				t.Fatalf("failed pre-claim admission wrote a lease: %#v, %v", leases, err)
			}
			if fakeHarnessCalls(provider.fakeHarness) != 0 || fakeHarnessCalls(chat) != 0 {
				t.Fatal("failed reservation dispatched or fell back")
			}
		})
	}
}

func TestLegacyLeaseAdoptsProviderOnSuccessfulRecovery(t *testing.T) {
	cfg := providerOwnerConfig(t)
	cfg.Orchestrator.MaxParallel = 1
	working := providerOwnerTask(t, cfg, "legacy provider adoption", "working")
	fixture := newPinnedOwnerFixture(t, cfg, true, nil)
	manager := fixture.manager
	blockedAt := time.Date(2026, time.September, 18, 12, 0, 0, 0, time.UTC)
	before := Lease{
		ID: "legacy-adoption", Route: "tasks", File: working, SessionKey: "legacy-adoption-session",
		Phase: phaseTaskImplementation, State: "processing", OwnerID: "prior-owner", ThreadID: "prior-thread",
		StartedAt: blockedAt.Add(-time.Hour), HeartbeatAt: blockedAt.Add(-time.Hour), RecoveryCount: 4,
		LastError: "preserved diagnostic", Blocked: &LeaseBlock{Reason: LeaseBlockedProviderUnavailable, Since: blockedAt},
	}
	legacyJSON, err := json.MarshalIndent(before, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(legacyJSON, []byte(`"provider"`)) {
		t.Fatalf("legacy fixture unexpectedly contains provider: %s", legacyJSON)
	}
	if err = os.MkdirAll(manager.leaseDirectory(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(manager.leasePath(before.ID), append(legacyJSON, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := manager.loadLease(before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Provider != "" {
		t.Fatalf("legacy lease provider = %q, want empty", loaded.Provider)
	}
	if !manager.acquireCapacity(context.Background()) {
		t.Fatal("failed to occupy dispatch capacity")
	}
	capacityHeld := true
	defer func() {
		if capacityHeld {
			manager.releaseCapacity()
		}
		manager.Wait()
	}()
	if err = manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := manager.loadLease(before.ID)
	if err != nil {
		t.Fatal(err)
	}
	expected := before
	expected.Provider = "glm-dev"
	expected.OwnerID = manager.ownerID
	expected.HeartbeatAt = after.HeartbeatAt
	expected.Blocked = nil
	if !reflect.DeepEqual(after, expected) {
		t.Fatalf("legacy recovery changed unrelated fields:\n got: %#v\nwant: %#v", after, expected)
	}
	if !after.HeartbeatAt.After(before.HeartbeatAt) {
		t.Fatalf("recovery heartbeat = %s, want after %s", after.HeartbeatAt, before.HeartbeatAt)
	}
	manager.releaseCapacity()
	capacityHeld = false
	manager.Wait()
	if calls := fakeHarnessCalls(fixture.alternate.fakeHarness); calls != 1 {
		t.Fatalf("legacy owner adoption dispatches = %d, want 1", calls)
	}
	if calls := fakeHarnessCalls(fixture.owner.fakeHarness); calls != 0 {
		t.Fatalf("legacy recovery used non-role provider %d times", calls)
	}
}

func TestEmptyReservationProviderIDLeavesLegacyOwnerEmpty(t *testing.T) {
	cfg := providerOwnerConfig(t)
	cfg.Orchestrator.MaxParallel = 1
	working := providerOwnerTask(t, cfg, "empty reservation leaves legacy owner empty", "working")
	target := newFakeRecipient()
	manager := New(cfg, target, extensions.Runner{})
	lease := Lease{
		ID: "legacy-empty-owner", Route: "tasks", File: working, SessionKey: "legacy-empty-owner-session",
		Phase: phaseTaskImplementation, State: "processing",
		StartedAt:   time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		HeartbeatAt: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	if !manager.acquireCapacity(context.Background()) {
		t.Fatal("failed to occupy dispatch capacity")
	}
	capacityHeld := true
	defer func() {
		if capacityHeld {
			manager.releaseCapacity()
		}
		manager.Wait()
	}()
	if err := manager.recoverStale(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.OwnerID != manager.ownerID || after.Blocked != nil {
		t.Fatalf("stale lease did not reach the successful recovery save: %#v", after)
	}
	if after.Provider != "" {
		t.Fatalf("empty reservation provider id invented durable owner %q", after.Provider)
	}
	manager.releaseCapacity()
	capacityHeld = false
	manager.Wait()
	if calls := fakeHarnessCalls(target); calls != 1 {
		t.Fatalf("recovery dispatch calls = %d, want 1", calls)
	}
	final, err := manager.loadLease(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Provider != "" {
		t.Fatalf("dispatch invented durable owner from empty reservation id: %q", final.Provider)
	}
}

func TestOrphanAndInterruptedClaimsPersistProvider(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "orphan"
		if interrupted {
			name = "interrupted"
		}
		t.Run(name, func(t *testing.T) {
			cfg := providerOwnerConfig(t)
			spec := harness.RuntimeSpec{
				Providers: map[harness.ProviderID]harness.HarnessConfig{"chat": {Name: "chat"}, "claim-owner": {Name: "routed"}},
				Roles:     map[harness.Role]harness.ProviderID{harness.RoleChat: "chat", harness.RoleDeveloper: "claim-owner"},
			}
			manager := providerOwnerManager(cfg, providerOwnerRuntime(t, spec))
			var leaseID string
			if interrupted {
				source := providerOwnerTask(t, cfg, "interrupted provider claim", "todo")
				target := cfg.StatePath("tasks", "working", filepath.Base(source))
				lease := Lease{
					ID: "interrupted-provider-claim", Route: "tasks", File: target, SourceFile: source,
					SessionKey: "interrupted-provider-session", Phase: phaseTaskImplementation, State: "claiming",
					ClaimAttempt: 1, StartedAt: time.Date(2026, time.September, 18, 12, 0, 0, 0, time.UTC),
				}
				if err := manager.saveLease(lease); err != nil {
					t.Fatal(err)
				}
				leaseID = lease.ID
				if err := manager.resumeInterruptedClaims(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				providerOwnerTask(t, cfg, "orphan provider claim", "working")
				if err := manager.recoverOrphanClaims(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			manager.Wait()
			if leaseID != "" {
				lease, err := manager.loadLease(leaseID)
				if err != nil {
					t.Fatal(err)
				}
				if lease.Provider != "claim-owner" {
					t.Fatalf("interrupted claim provider = %q", lease.Provider)
				}
				return
			}
			leases, err := manager.loadLeases()
			if err != nil || len(leases) != 1 {
				t.Fatalf("orphan leases = %#v, %v", leases, err)
			}
			if leases[0].Provider != "claim-owner" {
				t.Fatalf("orphan claim provider = %q", leases[0].Provider)
			}
		})
	}
}

type providerOwnerSendFailureHarness struct {
	*fakeHarness
}

func (h *providerOwnerSendFailureHarness) Send(context.Context, string, string, core.Emit) (string, bool, error) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	return "", false, errors.New("send failed")
}

func TestProviderSurvivesDispatchSaves(t *testing.T) {
	for _, sendFailure := range []bool{false, true} {
		name := "success"
		if sendFailure {
			name = "send-failure"
		}
		t.Run(name, func(t *testing.T) {
			cfg := providerOwnerConfig(t)
			working := providerOwnerTask(t, cfg, "provider survives "+name, "working")
			var target harness.Harness = newFakeRecipient()
			if sendFailure {
				target = &providerOwnerSendFailureHarness{fakeHarness: newFakeRecipient()}
			}
			registry := harness.NewRegistry()
			registry.Register("routed", func(harness.HarnessConfig) (harness.Harness, error) { return target, nil })
			runtime, err := harness.NewRuntimeSpec(registry, harness.RuntimeSpec{
				Providers: map[harness.ProviderID]harness.HarnessConfig{"durable-instance": {Name: "routed"}},
				Roles: map[harness.Role]harness.ProviderID{
					harness.RoleChat:      "durable-instance",
					harness.RoleDeveloper: "durable-instance",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			if err = runtime.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			manager := providerOwnerManager(cfg, runtime)
			lease := Lease{
				ID: "provider-survives-" + name, Route: "tasks", File: working, SessionKey: "provider-survives-session-" + name,
				Phase: phaseTaskImplementation, State: "processing", Provider: "durable-instance",
			}
			if err := manager.saveLease(lease); err != nil {
				t.Fatal(err)
			}
			manager.dispatch(context.Background(), workflowRoutes()[0], lease, false)
			manager.Wait()
			after, err := manager.loadLease(lease.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Provider != lease.Provider {
				t.Fatalf("dispatch changed provider from %q to %q", lease.Provider, after.Provider)
			}
			if sendFailure && (after.State != "error" || after.LastError == "") {
				t.Fatalf("send failure did not retain ordinary error behavior: %#v", after)
			}
		})
	}
}

func TestNilProviderJSONUnchanged(t *testing.T) {
	cfg := providerOwnerConfig(t)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	lease := Lease{ID: "empty-provider", Route: "tasks", File: "task.md", SessionKey: "task-session", State: "processing"}
	if err := manager.saveLease(lease); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(manager.leasePath(lease.ID))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"provider"`)) {
		t.Fatalf("empty provider changed lease JSON: %s", raw)
	}
	legacy := []byte("{\n  \"id\": \"legacy-provider\",\n  \"route\": \"tasks\",\n  \"file\": \"task.md\",\n  \"session_key\": \"legacy-session\",\n  \"state\": \"processing\",\n  \"recovery_count\": 0\n}\n")
	if err = os.WriteFile(manager.leasePath("legacy-provider"), legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := manager.loadLease("legacy-provider")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Provider != "" {
		t.Fatalf("legacy provider = %q, want empty", loaded.Provider)
	}
}

func TestDurableProviderOwnersAreExactDeterministicAndReadOnly(t *testing.T) {
	cfg := providerOwnerConfig(t)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	for _, lease := range []Lease{
		{ID: "empty", Provider: ""},
		{ID: "codex-one", Provider: "codex-dev"},
		{ID: "codex-two", Provider: "codex-dev"},
		{ID: "same-kind-review", Provider: "claude-rev"},
		{ID: "same-kind-architecture", Provider: "claude-arch"},
	} {
		if err := manager.saveLease(lease); err != nil {
			t.Fatal(err)
		}
	}
	before := make(map[string][]byte)
	entries, err := os.ReadDir(manager.leaseDirectory())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, readErr := os.ReadFile(filepath.Join(manager.leaseDirectory(), entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		before[entry.Name()] = data
	}
	owners, err := DurableProviderOwners(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []harness.ProviderID{"claude-arch", "claude-rev", "codex-dev"}
	if !reflect.DeepEqual(owners, want) {
		t.Fatalf("durable owners = %q, want %q", owners, want)
	}
	for name, wantBytes := range before {
		got, readErr := os.ReadFile(filepath.Join(manager.leaseDirectory(), name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !bytes.Equal(got, wantBytes) {
			t.Fatalf("owner enumeration rewrote %s", name)
		}
	}
}

func TestDurableProviderOwnersSkipMalformedLease(t *testing.T) {
	cfg := providerOwnerConfig(t)
	manager := New(cfg, newFakeRecipient(), extensions.Runner{})
	if err := manager.saveLease(Lease{ID: "valid-owner", Provider: "codex-dev"}); err != nil {
		t.Fatal(err)
	}
	corrupt := []byte("{not-json\n")
	corruptPath := filepath.Join(manager.leaseDirectory(), "corrupt.json")
	if err := os.WriteFile(corruptPath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	validPath := manager.leasePath("valid-owner")
	validBefore, err := os.ReadFile(validPath)
	if err != nil {
		t.Fatal(err)
	}
	owners, err := DurableProviderOwners(cfg)
	if err != nil {
		t.Fatalf("owner enumeration beside malformed lease failed: %v", err)
	}
	want := []harness.ProviderID{"codex-dev"}
	if !reflect.DeepEqual(owners, want) {
		t.Fatalf("durable owners beside malformed lease = %q, want %q", owners, want)
	}
	if data, readErr := os.ReadFile(corruptPath); readErr != nil || !bytes.Equal(data, corrupt) {
		t.Fatalf("malformed lease changed: err = %v", readErr)
	}
	if data, readErr := os.ReadFile(validPath); readErr != nil || !bytes.Equal(data, validBefore) {
		t.Fatalf("valid lease changed: err = %v", readErr)
	}
}
