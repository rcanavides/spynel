package harness

import (
	"context"
	"strings"

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
	target     *runtimeTarget
}

// supervisorOperations lists the Supervisor capabilities applications may use.
// Embedding this interface, rather than *Supervisor, prevents lifecycle methods
// from escaping through type assertions on the operational target.
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
type runtimeTarget struct{ supervisorOperations }

func NewRuntime(registry *Registry, cfg HarnessConfig) *Runtime {
	cfg.Name = strings.ToLower(strings.TrimSpace(cfg.Name))
	s := NewSupervisor(registry, cfg)
	return &Runtime{supervisor: s, target: &runtimeTarget{supervisorOperations: s}}
}
func (r *Runtime) AcquireRole(Role) ExecutionTarget     { return r.target }
func (r *Runtime) Start(ctx context.Context) error      { return r.supervisor.Start(ctx) }
func (r *Runtime) Close() error                         { return r.supervisor.Close() }
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
