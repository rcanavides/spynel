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

// Runtime owns one stable Supervisor. All roles currently share that provider.
// It deliberately has no configurable role topology.
type Runtime struct {
	supervisor *Supervisor
	mu         sync.Mutex
	roles      map[Role]supervisorOperations // production has only the chat fallback; tests can remap
	targets    map[Role]*runtimeTarget
	bindings   map[string]*binding
	closed     bool
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
	s := NewSupervisor(registry, cfg)
	return &Runtime{supervisor: s, roles: map[Role]supervisorOperations{RoleChat: s}, targets: make(map[Role]*runtimeTarget), bindings: make(map[string]*binding)}
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
func (r *Runtime) Start(ctx context.Context) error          { return r.supervisor.Start(ctx) }
func (r *Runtime) Close() error {
	r.mu.Lock()
	r.closed = true
	clear(r.bindings)
	r.mu.Unlock()
	return r.supervisor.Close()
}
func (r *Runtime) Available() (bool, string)            { return r.supervisor.Available() }
func (r *Runtime) ReadyEvents() <-chan struct{}         { return r.supervisor.ReadyEvents() }
func (r *Runtime) Readiness() (uint64, <-chan struct{}) { return r.supervisor.Readiness() }
func (r *Runtime) HarnessConfig() HarnessConfig         { return r.supervisor.HarnessConfig() }

// Reconfigure preserves the existing publication and retirement boundary.
// Settings persistence/rollback remains owned by the application transaction.
func (r *Runtime) Reconfigure(cfg HarnessConfig) error {
	cfg.Name = strings.ToLower(strings.TrimSpace(cfg.Name))
	return r.supervisor.Reconfigure(cfg)
}
func (r *Runtime) ConfigureUnavailable(cfg HarnessConfig, cause error) error {
	cfg.Name = strings.ToLower(strings.TrimSpace(cfg.Name))
	return r.supervisor.ConfigureUnavailable(cfg, cause)
}
