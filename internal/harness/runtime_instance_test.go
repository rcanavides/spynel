package harness

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
)

// Two claude-code instances plus an agent-zero chat provider prove that
// ProviderID is provider instance identity while HarnessConfig.Name stays the
// harness/catalog kind.
func instanceSpec() RuntimeSpec {
	return RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"a0":          {Name: "agent-zero", SessionsFile: "instance-a0.json"},
		"claude-arch": {Name: "claude-code", SessionsFile: "instance-arch.json", Model: "A"},
		"claude-rev":  {Name: "claude-code", SessionsFile: "instance-rev.json", Model: "B"},
	}, Roles: map[Role]ProviderID{RoleChat: "a0", RoleDeveloper: "claude-arch", RoleReviewer: "claude-rev"}}
}

// instanceFixtures registers one factory per distinct harness kind and records
// every created harness keyed by its SessionsFile. Registry.Create only sees
// HarnessConfig, so the session file is the deterministic TEST-ONLY
// discriminator between same-kind provider instances; production identity
// always comes from the topology key.
type instanceFixtures struct {
	r        *Runtime
	mu       sync.Mutex
	created  map[string][]*changeHarness
	failNext map[string]bool
}

func newInstanceFixtures(t *testing.T, spec RuntimeSpec) *instanceFixtures {
	t.Helper()
	f := &instanceFixtures{created: map[string][]*changeHarness{}, failNext: map[string]bool{}}
	kinds := make(map[string]bool)
	for _, cfg := range spec.Providers {
		kinds[cfg.Name] = true
	}
	registry := NewRegistry()
	for kind := range kinds {
		registry.Register(kind, func(cfg HarnessConfig) (Harness, error) {
			h := newChangeHarness(cfg.Name)
			f.mu.Lock()
			if f.failNext[cfg.SessionsFile] {
				delete(f.failNext, cfg.SessionsFile)
				h.startErr = errors.New("instance start failed")
			}
			f.created[cfg.SessionsFile] = append(f.created[cfg.SessionsFile], h)
			f.mu.Unlock()
			return h, nil
		})
	}
	r, err := NewRuntimeSpec(registry, spec)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	t.Cleanup(func() { _ = r.Close() })
	return f
}

func (f *instanceFixtures) start(t *testing.T) {
	t.Helper()
	if err := f.r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (f *instanceFixtures) harness(file string, index int) *changeHarness {
	f.mu.Lock()
	defer f.mu.Unlock()
	created := f.created[file]
	if index < 0 || index >= len(created) {
		return nil
	}
	return created[index]
}

func (f *instanceFixtures) failNextStart(file string) {
	f.mu.Lock()
	f.failNext[file] = true
	f.mu.Unlock()
}

func instanceFixturesNil(t *testing.T, h *changeHarness, name string) *changeHarness {
	t.Helper()
	if h == nil {
		t.Fatalf("missing %s fixture harness", name)
	}
	return h
}

// instanceUpdate rewrites one provider instance config because map values are
// not addressable.
func instanceUpdate(spec RuntimeSpec, id ProviderID, update func(cfg *HarnessConfig)) {
	cfg := spec.Providers[id]
	update(&cfg)
	spec.Providers[id] = cfg
}

func TestRuntimeProviderInstancesShareHarnessKind(t *testing.T) {
	f := newInstanceFixtures(t, instanceSpec())
	f.start(t)
	r := f.r
	a0 := instanceFixturesNil(t, f.harness("instance-a0.json", 0), "chat")
	arch := instanceFixturesNil(t, f.harness("instance-arch.json", 0), "claude-arch")
	rev := instanceFixturesNil(t, f.harness("instance-rev.json", 0), "claude-rev")
	archEntry, revEntry := r.providers["claude-arch"], r.providers["claude-rev"]
	if archEntry == revEntry || archEntry.supervisor == revEntry.supervisor {
		t.Fatal("same-kind instances share a provider entry or Supervisor")
	}
	if archEntry.id != "claude-arch" || revEntry.id != "claude-rev" {
		t.Fatalf("instance identities = %q %q", archEntry.id, revEntry.id)
	}
	if cfg := archEntry.supervisor.HarnessConfig(); cfg.Name != "claude-code" || cfg.Model != "A" {
		t.Fatalf("claude-arch kind/config = %+v", cfg)
	}
	if cfg := revEntry.supervisor.HarnessConfig(); cfg.Name != "claude-code" || cfg.Model != "B" {
		t.Fatalf("claude-rev kind/config = %+v", cfg)
	}

	devTarget, revTarget := r.AcquireRole(RoleDeveloper), r.AcquireRole(RoleReviewer)
	devID, devRelease, err := ReserveExecution(devTarget, "job:dev")
	if err != nil {
		t.Fatal(err)
	}
	revID, revRelease, err := ReserveExecution(revTarget, "job:rev")
	if err != nil {
		t.Fatal(err)
	}
	if devID != "claude-arch" || revID != "claude-rev" {
		t.Fatalf("reservations = %q %q", devID, revID)
	}
	for _, id := range []ProviderID{devID, revID} {
		if id == "claude-code" {
			t.Fatal("reservation reported the harness kind instead of the instance")
		}
	}
	devRelease()
	revRelease()

	// Binding identity and ExecutionProvider agree on the admitting instance.
	if got := bindingSend(t, devTarget, "conv:dev", "arch prompt"); got != "claude-code-thread" {
		t.Fatalf("dev thread=%s", got)
	}
	b := bindingFor(r, "conv:dev")
	if b == nil || b.id != "claude-arch" || b.provider != archEntry.supervisor {
		t.Fatalf("dev binding=%+v", b)
	}
	if got := ExecutionProvider(devTarget, "conv:dev"); got != b.id || got != "claude-arch" {
		t.Fatalf("ExecutionProvider=%s binding=%s", got, b.id)
	}

	// Sends stay instance-isolated.
	if got := bindingSend(t, revTarget, "conv:rev", "rev prompt"); got != "claude-code-thread" {
		t.Fatalf("rev thread=%s", got)
	}
	if prompts := bindingPrompts(rev, "conv:rev"); len(prompts) != 1 || prompts[0] != "rev prompt" {
		t.Fatalf("rev prompts=%q", prompts)
	}
	if prompts := bindingPrompts(arch, "conv:rev"); len(prompts) != 0 {
		t.Fatalf("arch received reviewer work: %q", prompts)
	}
	if prompts := bindingPrompts(a0, "conv:rev"); len(prompts) != 0 {
		t.Fatalf("chat received reviewer work: %q", prompts)
	}

	// Session state stays instance-local: the same logical key moves to the
	// other instance without state following it.
	bindingSend(t, devTarget, "handoff", "arch work")
	if !arch.IsActive("handoff") || rev.IsActive("handoff") {
		t.Fatal("session state leaked between instances")
	}
	arch.finish("handoff")
	bindingSend(t, revTarget, "handoff", "rev work")
	if arch.IsActive("handoff") || !rev.IsActive("handoff") {
		t.Fatal("session state leaked between instances")
	}
	if prompts := bindingPrompts(arch, "handoff"); len(prompts) != 1 || prompts[0] != "arch work" {
		t.Fatalf("arch handoff prompts=%q", prompts)
	}
	if prompts := bindingPrompts(rev, "handoff"); len(prompts) != 1 || prompts[0] != "rev work" {
		t.Fatalf("rev handoff prompts=%q", prompts)
	}
	rev.finish("handoff")

	// Resets stay instance-isolated.
	if err := devTarget.ResetSession("reset-key"); err != nil {
		t.Fatal(err)
	}
	if !containsBindingPrompt(arch.resetKeys, "reset-key") || containsBindingPrompt(rev.resetKeys, "reset-key") {
		t.Fatalf("reset reached the wrong instance: arch=%q rev=%q", arch.resetKeys, rev.resetKeys)
	}
	if err := revTarget.ResetSession("reset-key"); err != nil {
		t.Fatal(err)
	}
	if !containsBindingPrompt(rev.resetKeys, "reset-key") {
		t.Fatalf("rev reset=%q", rev.resetKeys)
	}

	arch.finish("conv:dev")
	rev.finish("conv:rev")

	// Start occurred exactly once per instance.
	for name, h := range map[string]*changeHarness{"chat": a0, "claude-arch": arch, "claude-rev": rev} {
		if h.startCalls.Load() != 1 {
			t.Fatalf("%s starts=%d", name, h.startCalls.Load())
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	for name, h := range map[string]*changeHarness{"chat": a0, "claude-arch": arch, "claude-rev": rev} {
		if h.closeCalls.Load() != 1 {
			t.Fatalf("%s closes=%d", name, h.closeCalls.Load())
		}
	}
}

func TestRuntimeInstanceBindingSurvivesSameKindRemap(t *testing.T) {
	f := newInstanceFixtures(t, instanceSpec())
	f.start(t)
	r := f.r
	a0 := instanceFixturesNil(t, f.harness("instance-a0.json", 0), "chat")
	arch := instanceFixturesNil(t, f.harness("instance-arch.json", 0), "claude-arch")
	rev := instanceFixturesNil(t, f.harness("instance-rev.json", 0), "claude-rev")
	archSuper := r.providers["claude-arch"].supervisor
	revSuper := r.providers["claude-rev"].supervisor
	devTarget := r.AcquireRole(RoleDeveloper)

	bindingSend(t, devTarget, "K", "first")
	if b := bindingFor(r, "K"); b == nil || b.id != "claude-arch" || b.provider != archSuper {
		t.Fatalf("initial binding=%+v", b)
	}
	remap := instanceSpec()
	remap.Roles[RoleDeveloper] = "claude-rev"
	if err := r.Reconcile(context.Background(), remap); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	route := r.roles[RoleDeveloper]
	r.mu.Unlock()
	if route.id != "claude-rev" || route.provider != revSuper {
		t.Fatalf("published developer route=%+v", route)
	}
	// K remains owned by the admitting instance.
	if b := bindingFor(r, "K"); b == nil || b.id != "claude-arch" || b.provider != archSuper {
		t.Fatal("same-kind remap stole the bound instance")
	}
	if got := ExecutionProvider(devTarget, "K"); got != "claude-arch" {
		t.Fatalf("bound ExecutionProvider=%s", got)
	}
	id, release, err := ReserveExecution(devTarget, "K")
	if err != nil || id != "claude-arch" {
		t.Fatalf("bound reservation=%s %v", id, err)
	}
	release()
	// A new key resolves to the remapped instance.
	if got := bindingSend(t, devTarget, "fresh", "work"); got != "claude-code-thread" {
		t.Fatalf("fresh thread=%s", got)
	}
	if b := bindingFor(r, "fresh"); b == nil || b.id != "claude-rev" || b.provider != revSuper {
		t.Fatalf("fresh binding=%+v", b)
	}
	if prompts := bindingPrompts(rev, "fresh"); !containsBindingPrompt(prompts, "work") {
		t.Fatalf("fresh work missed the remapped instance: %q", prompts)
	}
	if prompts := bindingPrompts(arch, "fresh"); len(prompts) != 0 {
		t.Fatalf("fresh work reached the stale instance: %q", prompts)
	}
	if prompts := bindingPrompts(arch, "K"); len(prompts) != 1 || prompts[0] != "first" {
		t.Fatalf("bound work migrated: %q", prompts)
	}
	// Neither Supervisor was unnecessarily replaced.
	if r.providers["claude-arch"].supervisor != archSuper || r.providers["claude-rev"].supervisor != revSuper {
		t.Fatal("role remap replaced a supervisor")
	}
	for name, h := range map[string]*changeHarness{"chat": a0, "claude-arch": arch, "claude-rev": rev} {
		if h.startCalls.Load() != 1 || h.closeCalls.Load() != 0 {
			t.Fatalf("%s starts=%d closes=%d", name, h.startCalls.Load(), h.closeCalls.Load())
		}
	}
	arch.finish("K")
	rev.finish("fresh")
}

func TestRuntimeInstanceSessionOwnership(t *testing.T) {
	registry := NewRegistry()
	// Same-kind instances cannot share one session file.
	shared := RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"a0":          {Name: "agent-zero", SessionsFile: "owner-a0.json"},
		"claude-arch": {Name: "claude-code", SessionsFile: "owner-shared.json"},
		"claude-rev":  {Name: "claude-code", SessionsFile: "owner-shared.json"},
	}, Roles: map[Role]ProviderID{RoleChat: "a0", RoleDeveloper: "claude-arch", RoleReviewer: "claude-rev"}}
	if r, err := NewRuntimeSpec(registry, shared); err == nil || !strings.Contains(err.Error(), "share a session file") {
		_ = r.Close()
		t.Fatalf("same-session instances accepted: %v", err)
	}
	// Alias-equivalent paths are the same owner.
	revCfg := shared.Providers["claude-rev"]
	revCfg.SessionsFile = "./owner-shared.json"
	shared.Providers["claude-rev"] = revCfg
	if r, err := NewRuntimeSpec(registry, shared); err == nil || !strings.Contains(err.Error(), "share a session file") {
		_ = r.Close()
		t.Fatalf("alias session accepted: %v", err)
	}
	// Distinct session files coexist.
	f := newInstanceFixtures(t, instanceSpec())
	f.start(t)
	// Empty session files retain the existing allowance.
	empty := RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"codex-fast": {Name: "codex"},
		"codex-slow": {Name: "codex"},
	}, Roles: map[Role]ProviderID{RoleChat: "codex-fast", RoleDeveloper: "codex-slow"}}
	if r, err := NewRuntimeSpec(NewRegistry(), empty); err != nil {
		t.Fatalf("empty session files rejected: %v", err)
	} else {
		_ = r.Close()
	}
	// Reconcile cannot introduce a duplicate session owner inside one spec.
	dup := instanceSpec()
	instanceUpdate(dup, "claude-rev", func(cfg *HarnessConfig) { cfg.SessionsFile = "instance-arch.json" })
	if err := f.r.Reconcile(context.Background(), dup); err == nil || !strings.Contains(err.Error(), "share a session file") {
		t.Fatalf("intra-spec duplicate accepted: %v", err)
	}
	// Nor claim a file still owned by an instance this reconciliation removes.
	claim := instanceSpec()
	delete(claim.Providers, "claude-arch")
	claim.Roles[RoleDeveloper] = "claude-rev"
	instanceUpdate(claim, "claude-rev", func(cfg *HarnessConfig) { cfg.SessionsFile = "instance-arch.json" })
	if err := f.r.Reconcile(context.Background(), claim); err == nil || !strings.Contains(err.Error(), "still owns session file") {
		t.Fatalf("cross-reconcile claim accepted: %v", err)
	}
	// Nothing changed on rejection.
	if f.r.providers["claude-arch"] == nil {
		t.Fatal("owner removed after rejection")
	}
	if cfg := f.r.providers["claude-rev"].supervisor.HarnessConfig(); cfg.SessionsFile != "instance-rev.json" {
		t.Fatalf("rev session file=%q", cfg.SessionsFile)
	}
	for name, file := range map[string]string{"chat": "instance-a0.json", "claude-arch": "instance-arch.json", "claude-rev": "instance-rev.json"} {
		h := f.harness(file, 0)
		if h == nil {
			t.Fatalf("missing %s fixture harness", name)
		}
		if h.startCalls.Load() != 1 || h.closeCalls.Load() != 0 {
			t.Fatalf("%s starts=%d closes=%d", name, h.startCalls.Load(), h.closeCalls.Load())
		}
	}
}

func TestRuntimeInstanceIndependentReconcileAndFence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newInstanceFixtures(t, instanceSpec())
		f.start(t)
		r := f.r
		arch := instanceFixturesNil(t, f.harness("instance-arch.json", 0), "claude-arch")
		rev0 := instanceFixturesNil(t, f.harness("instance-rev.json", 0), "claude-rev")
		archSuper := r.providers["claude-arch"].supervisor
		revSuper := r.providers["claude-rev"].supervisor
		devTarget := r.AcquireRole(RoleDeveloper)
		revTarget := r.AcquireRole(RoleReviewer)

		// A live binding on claude-arch does not disturb claude-rev changes.
		bindingSend(t, devTarget, "K", "work")
		next := instanceSpec()
		instanceUpdate(next, "claude-rev", func(cfg *HarnessConfig) { cfg.Model = "C" })
		if err := r.Reconcile(context.Background(), next); err != nil {
			t.Fatal(err)
		}
		rev1 := instanceFixturesNil(t, f.harness("instance-rev.json", 1), "claude-rev candidate")
		if r.providers["claude-arch"].supervisor != archSuper || r.providers["claude-rev"].supervisor != revSuper {
			t.Fatal("independent reconcile replaced a Supervisor")
		}
		if cfg := r.providers["claude-rev"].supervisor.HarnessConfig(); cfg.Model != "C" {
			t.Fatalf("rev model=%q", cfg.Model)
		}
		if rev1.startCalls.Load() != 1 || rev0.closeCalls.Load() != 1 {
			t.Fatalf("rev replacement starts=%d previous closes=%d", rev1.startCalls.Load(), rev0.closeCalls.Load())
		}
		if arch.startCalls.Load() != 1 || arch.closeCalls.Load() != 0 {
			t.Fatalf("arch disturbed: starts=%d closes=%d", arch.startCalls.Load(), arch.closeCalls.Load())
		}
		if got := ExecutionProvider(devTarget, "K"); got != "claude-arch" {
			t.Fatalf("bound instance=%s", got)
		}

		// The structural fence covers only the affected instance.
		candidate := newChangeHarness("claude-code")
		entered, release := make(chan struct{}), make(chan struct{})
		candidate.onStart = func() { close(entered); <-release }
		r.registry.Register("claude-code", func(cfg HarnessConfig) (Harness, error) {
			if cfg.SessionsFile == "instance-rev.json" {
				return candidate, nil
			}
			return newChangeHarness(cfg.Name), nil
		})
		fenceSpec := instanceSpec()
		instanceUpdate(fenceSpec, "claude-rev", func(cfg *HarnessConfig) { cfg.Model = "D" })
		done := make(chan error, 1)
		go func() { done <- r.Reconcile(context.Background(), fenceSpec) }()
		<-entered
		if _, _, err := ReserveExecution(revTarget, "fenced"); !errors.Is(err, ErrProviderFenced) {
			t.Fatalf("affected instance reservation=%v", err)
		}
		id, unaffectedRelease, err := ReserveExecution(devTarget, "unaffected")
		if err != nil || id != "claude-arch" {
			t.Fatalf("unaffected instance reservation=%s %v", id, err)
		}
		unaffectedRelease()
		close(release)
		if err := awaitChange(t, done); err != nil {
			t.Fatal(err)
		}
		if candidate.startCalls.Load() != 1 || rev1.closeCalls.Load() != 1 {
			t.Fatalf("fenced replacement candidate starts=%d previous closes=%d", candidate.startCalls.Load(), rev1.closeCalls.Load())
		}

		// A live binding fences structural replacement only for its instance.
		blocked := instanceSpec()
		instanceUpdate(blocked, "claude-rev", func(cfg *HarnessConfig) { cfg.Model = "D" })
		instanceUpdate(blocked, "claude-arch", func(cfg *HarnessConfig) { cfg.Model = "A2" })
		if err := r.Reconcile(context.Background(), blocked); !errors.Is(err, ErrProviderFenced) {
			t.Fatalf("bound instance change=%v", err)
		}
		if r.providers["claude-arch"].supervisor != archSuper {
			t.Fatal("fenced instance replaced")
		}
		if cfg := archSuper.HarnessConfig(); cfg.Model != "A" {
			t.Fatalf("fenced change published: %q", cfg.Model)
		}
		if arch.closeCalls.Load() != 0 {
			t.Fatalf("fenced instance closed %d times", arch.closeCalls.Load())
		}
		if cfg := revSuper.HarnessConfig(); cfg.Model != "D" {
			t.Fatalf("rejected change disturbed rev: %q", cfg.Model)
		}

		arch.finish("K")
	})
}

func TestRuntimeMappedUnavailableInstanceNoFallback(t *testing.T) {
	f := newInstanceFixtures(t, instanceSpec())
	f.failNextStart("instance-rev.json")
	f.start(t)
	r := f.r
	a0 := instanceFixturesNil(t, f.harness("instance-a0.json", 0), "chat")
	arch := instanceFixturesNil(t, f.harness("instance-arch.json", 0), "claude-arch")
	revTarget := r.AcquireRole(RoleReviewer)
	if ready, _ := revTarget.(Availability).Available(); ready {
		t.Fatal("unavailable instance reports available")
	}
	if _, _, err := ReserveExecution(revTarget, "rev-job"); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("unavailable reservation=%v", err)
	}
	if _, _, err := revTarget.Send(context.Background(), "rev-job", "prompt", nil); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("unavailable send=%v", err)
	}
	// The reservation must not fall back to chat or developer.
	rev := instanceFixturesNil(t, f.harness("instance-rev.json", 0), "claude-rev")
	for name, h := range map[string]*changeHarness{"chat": a0, "claude-arch": arch, "claude-rev": rev} {
		if prompts := bindingPrompts(h, "rev-job"); len(prompts) != 0 {
			t.Fatalf("%s received the reviewer's work: %q", name, prompts)
		}
	}
	if got := ExecutionProvider(revTarget, "rev-job"); got != "claude-rev" {
		t.Fatalf("unmapped role identity=%s", got)
	}
	// Mapped instances and chat keep working independently.
	if got := bindingSend(t, r.AcquireRole(RoleChat), "chat-works", "hello"); got != "agent-zero-thread" {
		t.Fatalf("chat thread=%s", got)
	}
	if got := bindingSend(t, r.AcquireRole(RoleDeveloper), "dev-works", "hello"); got != "claude-code-thread" {
		t.Fatalf("developer thread=%s", got)
	}
	a0.finish("chat-works")
	arch.finish("dev-works")
}

func TestRuntimeSpecInstanceValidation(t *testing.T) {
	registry := NewRegistry()
	// A provider instance identity distinct from the harness kind is accepted.
	accepted := RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		" CODEX-DEV ": {Name: " Codex "},
	}, Roles: map[Role]ProviderID{RoleChat: "codex-dev"}}
	r, err := NewRuntimeSpec(registry, accepted)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.providers) != 1 || r.providers["codex-dev"] == nil || r.providers["codex-dev"].id != "codex-dev" {
		t.Fatal("instance identity was not normalized")
	}
	if cfg := r.providers["codex-dev"].supervisor.HarnessConfig(); cfg.Name != "codex" {
		t.Fatalf("harness kind=%q", cfg.Name)
	}
	_ = r.Close()

	reject := func(name string, spec RuntimeSpec, want string) {
		t.Helper()
		if _, err := NewRuntimeSpec(registry, spec); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: err=%v", name, err)
		}
	}
	reject("empty identity", RuntimeSpec{Providers: map[ProviderID]HarnessConfig{"": {Name: "codex"}}, Roles: map[Role]ProviderID{RoleChat: ""}}, "provider identity is empty")
	reject("whitespace identity", RuntimeSpec{Providers: map[ProviderID]HarnessConfig{"  ": {Name: "codex"}}, Roles: map[Role]ProviderID{RoleChat: "  "}}, "provider identity is empty")
	reject("empty harness name", RuntimeSpec{Providers: map[ProviderID]HarnessConfig{"codex-dev": {Name: "   "}}, Roles: map[Role]ProviderID{RoleChat: "codex-dev"}}, "empty harness name")
	reject("duplicate identity after normalization", RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"claude-arch":   {Name: "claude-code", SessionsFile: "validation-arch.json"},
		" CLAUDE-ARCH ": {Name: "claude-code", SessionsFile: "validation-rev.json"},
	}, Roles: map[Role]ProviderID{RoleChat: "claude-arch"}}, "duplicate provider")
	// The legacy no-harness-selected marker stays constructible so a
	// harness-less service remains reachable without a coding harness.
	if r, err := NewRuntimeSpec(registry, RuntimeSpec{Providers: map[ProviderID]HarnessConfig{" ": {Name: " "}}, Roles: map[Role]ProviderID{RoleChat: " "}}); err != nil {
		t.Fatalf("no-harness marker rejected: %v", err)
	} else {
		_ = r.Close()
	}

	// Custom ACP validation keys on the harness kind, not the instance ID.
	reject("acp kind without command", RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"a0":       {Name: "agent-zero"},
		"acp-main": {Name: "acp"},
	}, Roles: map[Role]ProviderID{RoleChat: "a0", RoleDeveloper: "acp-main"}}, "custom ACP requires a command")
	reject("second acp instance without command", RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"acp-chat": {Name: "acp"},
		"acp-dev":  {Name: "acp"},
	}, Roles: map[Role]ProviderID{RoleChat: "acp-chat", RoleDeveloper: "acp-dev"}}, "custom ACP requires a command")
	reject("whitespace acp command", RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"a0":      {Name: "agent-zero"},
		"acp-dev": {Name: "acp", Command: "   "},
	}, Roles: map[Role]ProviderID{RoleChat: "a0", RoleDeveloper: "acp-dev"}}, "custom ACP requires a command")

	// A non-acp kind under the literal "acp" identity needs no command.
	if r, err := NewRuntimeSpec(registry, RuntimeSpec{Providers: map[ProviderID]HarnessConfig{
		"a0":  {Name: "agent-zero"},
		"acp": {Name: "codex"},
	}, Roles: map[Role]ProviderID{RoleChat: "a0", RoleDeveloper: "acp"}}); err != nil {
		t.Fatalf("instance ID confused with harness kind: %v", err)
	} else {
		_ = r.Close()
	}
	// The chat-provider exemption is preserved for the mapped chat instance.
	for _, spec := range []RuntimeSpec{
		{Providers: map[ProviderID]HarnessConfig{"acp": {Name: "acp"}}, Roles: map[Role]ProviderID{RoleChat: " ACP "}},
		{Providers: map[ProviderID]HarnessConfig{"acp-chat": {Name: "acp"}, "acp-dev": {Name: "acp", Command: "run-adapter"}}, Roles: map[Role]ProviderID{RoleChat: "acp-chat", RoleDeveloper: "acp-dev"}},
	} {
		if r, err := NewRuntimeSpec(registry, spec); err != nil {
			t.Fatalf("chat ACP exemption rejected: %v", err)
		} else {
			_ = r.Close()
		}
	}
}

func TestRuntimeLegacyProviderIDEqualsHarnessName(t *testing.T) {
	registry := NewRegistry()
	old, next := newChangeHarness("old"), newChangeHarness("new")
	registry.Register("old", func(HarnessConfig) (Harness, error) { return old, nil })
	registry.Register("new", func(HarnessConfig) (Harness, error) { return next, nil })
	r := NewRuntime(registry, HarnessConfig{Name: "OLD"})
	defer r.Close()
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	chat := r.AcquireRole(RoleChat)
	id, release, err := ReserveExecution(chat, "legacy")
	if err != nil || id != "old" {
		t.Fatalf("legacy reservation=%s %v", id, err)
	}
	release()
	if r.HarnessConfig().Name != "old" {
		t.Fatalf("legacy harness=%q", r.HarnessConfig().Name)
	}
	if err := r.Reconfigure(HarnessConfig{Name: "NEW"}); err != nil {
		t.Fatal(err)
	}
	id, release, err = ReserveExecution(chat, "legacy-next")
	if err != nil || id != "new" {
		t.Fatalf("reconfigured reservation=%s %v", id, err)
	}
	release()
	if r.HarnessConfig().Name != "new" {
		t.Fatalf("reconfigured harness=%q", r.HarnessConfig().Name)
	}
	if old.closeCalls.Load() != 1 || next.startCalls.Load() != 1 {
		t.Fatalf("legacy replacement: old closes=%d next starts=%d", old.closeCalls.Load(), next.startCalls.Load())
	}

	// Legacy topology specs report instance identity equal to the kind.
	r2, _ := topologyFixture(t, topologySpec())
	r2DevID, r2DevRelease, err := ReserveExecution(r2.AcquireRole(RoleDeveloper), "dev")
	if err != nil || r2DevID != "codex" {
		t.Fatalf("legacy developer reservation=%s %v", r2DevID, err)
	}
	r2DevRelease()
	r2RevID, r2RevRelease, err := ReserveExecution(r2.AcquireRole(RoleReviewer), "rev")
	if err != nil || r2RevID != "claude-code" {
		t.Fatalf("legacy reviewer reservation=%s %v", r2RevID, err)
	}
	r2RevRelease()

	// A bare Supervisor has no instance identity: the free-function fallback
	// still reports HarnessConfig().Name.
	sreg := NewRegistry()
	sreg.Register("codex", func(HarnessConfig) (Harness, error) { return newChangeHarness("codex"), nil })
	bare := NewSupervisor(sreg, HarnessConfig{Name: "codex"})
	if err := bare.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer bare.Close()
	bareID, _, err := ReserveExecution(bare, "bare")
	if err != nil || bareID != "codex" {
		t.Fatalf("bare supervisor reservation=%s %v", bareID, err)
	}
	if got := ExecutionProvider(bare, "bare"); got != "codex" {
		t.Fatalf("bare supervisor identity=%s", got)
	}
}
