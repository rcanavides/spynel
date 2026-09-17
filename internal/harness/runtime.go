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
	roles      map[Role]supervisorOperations
	targets    map[Role]*runtimeTarget
	bindings   map[string]*binding
	closed     bool
	lifecycle  chan struct{}
	ctx        context.Context
	changed    chan struct{}
	ready      chan struct{}
	version    uint64
}

// A binding retains the actual provider Supervisor, rather than re-resolving a
// role. Its catalog identity is Supervisor.HarnessConfig().Name. Keeping the
// reference also preserves Commit 2's single-Supervisor structural replacement.
// Supervisor.IsActive is the sole authority for logical execution lifetime.
type binding struct {
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
