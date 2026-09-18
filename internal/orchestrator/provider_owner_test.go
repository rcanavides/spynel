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
	spec := harness.RuntimeSpec{
		Providers: map[harness.ProviderID]harness.HarnessConfig{"chat": {Name: "chat"}, "legacy-owner": {Name: "routed"}},
		Roles:     map[harness.Role]harness.ProviderID{harness.RoleChat: "chat", harness.RoleDeveloper: "legacy-owner"},
	}
	manager := providerOwnerManager(cfg, providerOwnerRuntime(t, spec))
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
	expected.Provider = "legacy-owner"
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
}

func TestEmptyReservationProviderIDPreservesExistingOwner(t *testing.T) {
	cfg := providerOwnerConfig(t)
	cfg.Orchestrator.MaxParallel = 1
	working := providerOwnerTask(t, cfg, "empty reservation preserves owner", "working")
	target := newFakeRecipient()
	manager := New(cfg, target, extensions.Runner{})
	lease := Lease{
		ID: "existing-durable-owner", Route: "tasks", File: working, SessionKey: "existing-owner-session",
		Phase: phaseTaskImplementation, State: "processing", Provider: "existing-owner",
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
	if after.Provider != "existing-owner" {
		t.Fatalf("empty reservation provider id erased durable owner: got %q, want existing-owner", after.Provider)
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
	if final.Provider != "existing-owner" {
		t.Fatalf("recovery dispatch lost durable owner: got %q, want existing-owner", final.Provider)
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
			var target harness.ExecutionTarget = newFakeRecipient()
			if sendFailure {
				target = &providerOwnerSendFailureHarness{fakeHarness: newFakeRecipient()}
			}
			manager := New(cfg, target, extensions.Runner{})
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
