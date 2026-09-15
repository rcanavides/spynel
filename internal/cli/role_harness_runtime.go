package cli

import (
	"context"
	"errors"
	"sync"

	"github.com/agent0ai/spynel/internal/app"
	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/harness"
)

// routedHarnessRuntime owns the role-specific harness topology.
//
// The primary/chat supervisor remains stable and continues to be owned by
// app.Service. Routed supervisors may be replaced when live harness settings
// change, while callers keep stable role handles.
type routedHarnessRuntime struct {
	mu sync.RWMutex

	registry     *harness.Registry
	primary      harness.Harness
	version      string
	runtimeState *app.Runtime

	current *harness.RoleSet
	handles map[harness.Role]*routedRoleHandle

	ctx     context.Context
	started bool
	closed  bool
}

type routedRoleHandle struct {
	runtime *routedHarnessRuntime
	role    harness.Role
}

func newRoutedHarnessRuntime(
	primary harness.Harness,
	registry *harness.Registry,
	cfg config.Config,
	version string,
	runtimeState *app.Runtime,
) *routedHarnessRuntime {
	runtime := &routedHarnessRuntime{
		registry:     registry,
		primary:      primary,
		version:      version,
		runtimeState: runtimeState,
		handles:      make(map[harness.Role]*routedRoleHandle),
	}
	runtime.current = runtime.compose(cfg)
	return runtime
}

func (r *routedHarnessRuntime) compose(cfg config.Config) *harness.RoleSet {
	routes := make(map[harness.Role]harness.Harness)
	byName := make(map[string]harness.Harness)
	additional := make([]harness.Harness, 0)

	roles := []harness.Role{
		harness.RoleDeveloper,
		harness.RoleReviewer,
		harness.RoleNotification,
		harness.RoleHeartbeat,
	}

	for _, role := range roles {
		name := cfg.Harness.NameForRole(role)

		if name == "" || name == cfg.Harness.Name {
			continue
		}

		target, exists := byName[name]
		if !exists {
			target = newHarnessSupervisor(
				r.registry,
				cfg,
				name,
				r.version,
				r.runtimeState,
				false,
			)
			byName[name] = target
			additional = append(additional, target)
		}

		routes[role] = target
	}

	return harness.NewRoleSet(r.primary, routes, additional...)
}

// HarnessForRole returns one stable logical-role handle.
//
// The handle resolves the concrete provider only when an operation is
// admitted, so a caller may safely retain it across live topology changes.
func (r *routedHarnessRuntime) HarnessForRole(role harness.Role) harness.Harness {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.handles == nil {
		r.handles = make(map[harness.Role]*routedRoleHandle)
	}

	if handle := r.handles[role]; handle != nil {
		return handle
	}

	handle := &routedRoleHandle{
		runtime: r,
		role:    role,
	}
	r.handles[role] = handle
	return handle
}

// targetForRole exposes the current concrete target internally for lifecycle
// and structural tests. Runtime callers should use HarnessForRole.
func (r *routedHarnessRuntime) targetForRole(role harness.Role) harness.Harness {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.targetForRoleLocked(role)
}

func (r *routedHarnessRuntime) targetForRoleLocked(role harness.Role) harness.Harness {
	if r.current != nil {
		if target := r.current.HarnessForRole(role); target != nil {
			return target
		}
	}
	return r.primary
}

func (r *routedHarnessRuntime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("routed harness runtime is closed")
	}
	if r.started {
		return nil
	}

	if r.current != nil {
		if err := r.current.Start(ctx); err != nil {
			return err
		}
	}

	r.ctx = ctx
	r.started = true
	return nil
}

func (r *routedHarnessRuntime) ReconfigureRoleHarnesses(cfg config.Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("routed harness runtime is closed")
	}

	if r.current != nil {
		if err := r.current.EnsureIdle(); err != nil {
			return err
		}
	}

	next := r.compose(cfg)

	if r.started {
		if err := next.Start(r.ctx); err != nil {
			_ = next.Close()
			return err
		}
	}

	previous := r.current
	r.current = next

	if previous != nil {
		_ = previous.Close()
	}

	return nil
}

func (r *routedHarnessRuntime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true

	if r.current == nil {
		return nil
	}
	return r.current.Close()
}

// ------------------------------------------------------------
// Stable logical-role handle
// ------------------------------------------------------------

// Start is intentionally a no-op. Lifecycle ownership remains with
// routedHarnessRuntime/RoleSet.
func (h *routedRoleHandle) Start(context.Context) error {
	return nil
}

func (h *routedRoleHandle) Send(
	ctx context.Context,
	key string,
	prompt string,
	emit core.Emit,
) (string, bool, error) {
	r := h.runtime
	if r == nil {
		return "", false, errors.New("routed harness runtime is unavailable")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return "", false, errors.New("routed harness runtime is closed")
	}

	target := r.targetForRoleLocked(h.role)
	if target == nil {
		return "", false, errors.New("routed harness role has no target")
	}

	return target.Send(ctx, key, prompt, emit)
}

func (h *routedRoleHandle) Interrupt(
	ctx context.Context,
	key string,
) (bool, error) {
	r := h.runtime
	if r == nil {
		return false, errors.New("routed harness runtime is unavailable")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return false, errors.New("routed harness runtime is closed")
	}

	target := r.targetForRoleLocked(h.role)
	if target == nil {
		return false, errors.New("routed harness role has no target")
	}

	return target.Interrupt(ctx, key)
}

func (h *routedRoleHandle) ResetSession(key string) error {
	r := h.runtime
	if r == nil {
		return errors.New("routed harness runtime is unavailable")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return errors.New("routed harness runtime is closed")
	}

	target := r.targetForRoleLocked(h.role)
	if target == nil {
		return errors.New("routed harness role has no target")
	}

	return target.ResetSession(key)
}

func (h *routedRoleHandle) ThreadID(key string) string {
	r := h.runtime
	if r == nil {
		return ""
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return ""
	}

	target := r.targetForRoleLocked(h.role)
	if target == nil {
		return ""
	}

	return target.ThreadID(key)
}

func (h *routedRoleHandle) IsActive(key string) bool {
	r := h.runtime
	if r == nil {
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return false
	}

	target := r.targetForRoleLocked(h.role)
	if target == nil {
		return false
	}

	return target.IsActive(key)
}

// Close is intentionally a no-op. The handle borrows the current provider;
// routedHarnessRuntime owns provider shutdown.
func (h *routedRoleHandle) Close() error {
	return nil
}
