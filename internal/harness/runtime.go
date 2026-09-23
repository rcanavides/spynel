package harness

import (
	"context"
	"strings"
	"sync"

	"github.com/agent0ai/spynel/internal/core"
)

// ExecutionTarget is the operational boundary. Lifecycle belongs to its owner.
// Optional capabilities remain discoverable through the existing interfaces.
type ExecutionTarget interface {
	Send(context.Context, string, string, core.Emit) (string, bool, error)
	Interrupt(context.Context, string) (bool, error)
	ResetSession(string) error
	ThreadID(string) string
	IsActive(string) bool
}

// Runtime exclusively owns every provider and its stable operational role targets.
// Lifecycle reservations serialize topology work without holding mu over I/O.
type Runtime struct {
	registry   *Registry
	supervisor *Supervisor // current chat provider; compatibility for local fixtures
	mu         sync.Mutex
	providers  map[ProviderID]*providerEntry
	roles      map[Role]providerRoute
	targets    map[Role]*runtimeTarget
	bindings   map[string]*binding
	closed     bool
	lifecycle  chan struct{}
	ctx        context.Context
	// shutdown is the runtime-owned lifetime. Close cancels it before any
	// lifecycle wait so in-flight Start/Reconcile provider work whose context
	// was joined with it unblocks deterministically instead of holding the
	// shutdown orchestration forever. It is internal; no caller configures it.
	shutdown context.Context
	// cancelLife cancels shutdown; idempotent, so Close may fire it on every
	// caller while exactly one orchestration performs the actual shutdown.
	cancelLife context.CancelFunc
	// lifetimeCancel releases the latest joined lifetime stored as ctx. The
	// join registrations themselves release when shutdown cancels, so an
	// earlier superseded lifetime never leaks a goroutine.
	lifetimeCancel context.CancelFunc
	// closeErr stores the one shutdown orchestration's result so every
	// concurrent or repeated Close caller observes the same outcome.
	closeErr error
	changed  chan struct{}
	ready    chan struct{}
	version  uint64
}

// providerRoute retains the concrete owner behind one role mapping: the
// provider instance identity from the topology key plus its Supervisor. The
// instance ID stays the admission identity even when several instances share
// one harness kind.
type providerRoute struct {
	id       ProviderID
	provider supervisorOperations
}

// A binding retains the exact provider instance that admitted an execution:
// its topology instance ID plus the actual Supervisor, rather than
// re-resolving a role. Keeping both references preserves exact attribution
// across role remaps and Commit 2's single-Supervisor structural replacement.
// Supervisor.IsActive is the sole authority for logical execution lifetime.
type binding struct {
	id        ProviderID
	provider  supervisorOperations
	admitting int
}

// Only operational methods are retained in ownership records. Production
// entries are Supervisors; tests can instrument this boundary without changing
// Supervisor's authoritative activity semantics.
type supervisorOperations interface {
	ExecutionTarget
	ConversationSender
	ControlSender
	ModelProvider
	Availability
	ActiveTurnReporter
	ConversationAdmission(string) string
	CommitInference(InferenceSelection, func() error) error
	CommitModel(string, func() error) error
	HarnessConfig() HarnessConfig
	Readiness() (uint64, <-chan struct{})
}

type runtimeTarget struct {
	runtime *Runtime
	role    Role
}

func NewRuntime(registry *Registry, cfg HarnessConfig) *Runtime {
	cfg.Name = strings.ToLower(strings.TrimSpace(cfg.Name))
	r, err := NewRuntimeSpec(registry, RuntimeSpec{Providers: map[ProviderID]HarnessConfig{ProviderID(cfg.Name): cfg}, Roles: map[Role]ProviderID{RoleChat: ProviderID(cfg.Name)}})
	if err != nil {
		panic(err)
	}
	return r
}
func (r *Runtime) AcquireRole(role Role) ExecutionTarget {
	r.mu.Lock()
	defer r.mu.Unlock()
	if target := r.targets[role]; target != nil {
		return target
	}
	target := &runtimeTarget{runtime: r, role: role}
	r.targets[role] = target
	return target
}
func (r *Runtime) HarnessForRole(role Role) ExecutionTarget { return r.AcquireRole(role) }
func (r *Runtime) Available() (bool, string) {
	return r.AcquireRole(RoleChat).(Availability).Available()
}
func (r *Runtime) ReadyEvents() <-chan struct{} { return r.ready }
func (r *Runtime) Readiness() (uint64, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.version, r.changed
}
func (r *Runtime) HarnessConfig() HarnessConfig {
	return r.AcquireRole(RoleChat).(*runtimeTarget).HarnessConfig()
}
func (r *Runtime) Reconfigure(cfg HarnessConfig) error {
	cfg.Name = strings.ToLower(strings.TrimSpace(cfg.Name))
	return r.Reconcile(context.Background(), RuntimeSpec{Providers: map[ProviderID]HarnessConfig{ProviderID(cfg.Name): cfg}, Roles: map[Role]ProviderID{RoleChat: ProviderID(cfg.Name)}})
}
func (r *Runtime) ConfigureUnavailable(cfg HarnessConfig, cause error) error {
	if ok, _ := r.Available(); ok {
		return cause
	}
	return r.Reconfigure(cfg)
}

// joinShutdown couples a caller context with the runtime-owned shutdown
// lifetime: work started under the joined context unblocks when either side
// cancels. The returned cleanup releases the AfterFunc registration; callers
// that store the joined context as a long-lived lifetime may defer cleanup
// only while nothing depends on the context outliving their call.
func (r *Runtime) joinShutdown(ctx context.Context) (context.Context, context.CancelFunc) {
	joined, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.shutdown, cancel)
	return joined, func() {
		stop()
		cancel()
	}
}

// releaseLifetime cancels the latest joined lifetime stored as ctx and drops
// the stored cancel. It runs after cancelLife in Close: shutdown propagation
// already cancelled the same context through its AfterFunc, so this is
// deterministic registration cleanup, not a second state change.
func (r *Runtime) releaseLifetime() {
	r.mu.Lock()
	cancel := r.lifetimeCancel
	r.lifetimeCancel = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
