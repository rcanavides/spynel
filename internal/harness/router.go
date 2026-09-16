package harness

// Role identifies the logical agent role requesting a harness.
// It is provider-neutral: callers choose a role, never a concrete provider.
type Role string

const (
	RoleChat         Role = "chat"
	RoleDeveloper    Role = "developer"
	RoleReviewer     Role = "reviewer"
	RoleNotification Role = "notification"
	RoleHeartbeat    Role = "heartbeat"
)

// RoleRouter resolves the harness assigned to a logical agent role.
type RoleRouter interface {
	HarnessForRole(Role) ExecutionTarget
}

// StaticRoleRouter provides an immutable role-to-harness mapping with
// a fallback harness for roles that do not have an explicit assignment.
type StaticRoleRouter struct {
	fallback Harness
	routes   map[Role]Harness
}

// NewStaticRoleRouter creates a provider-neutral role router.
// The supplied routes are copied so later caller mutations cannot change
// the active routing table.
func NewStaticRoleRouter(fallback Harness, routes map[Role]Harness) *StaticRoleRouter {
	copied := make(map[Role]Harness, len(routes))
	for role, target := range routes {
		if target != nil {
			copied[role] = target
		}
	}

	return &StaticRoleRouter{
		fallback: fallback,
		routes:   copied,
	}
}

// HarnessForRole returns the explicit harness for role when configured.
// Otherwise it returns the fallback harness.
func (r *StaticRoleRouter) HarnessForRole(role Role) ExecutionTarget {
	if r == nil {
		return nil
	}

	if target := r.routes[role]; target != nil {
		return target
	}

	return r.fallback
}
