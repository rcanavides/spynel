package harness

import (
	"context"
	"errors"
	"sync"
)

// RoleSet owns the additional harnesses used by role routing while leaving the
// application's primary/chat harness under its existing Service lifecycle.
//
// The fallback harness is resolved through the embedded role router but is not
// started or closed by RoleSet. Service continues to own that harness exactly
// as it did in the legacy single-harness runtime.
type RoleSet struct {
	router *StaticRoleRouter

	mu         sync.Mutex
	additional []Harness
	started    int
	closed     bool
}

// NewRoleSet creates a provider-neutral role runtime.
//
// additional must contain each independently owned non-primary harness exactly
// once. Routes may safely point several roles at the same harness.
func NewRoleSet(fallback Harness, routes map[Role]Harness, additional ...Harness) *RoleSet {
	owned := make([]Harness, 0, len(additional))
	for _, target := range additional {
		if target != nil {
			owned = append(owned, target)
		}
	}

	return &RoleSet{
		router:     NewStaticRoleRouter(fallback, routes),
		additional: owned,
	}
}

// HarnessForRole implements RoleRouter.
func (s *RoleSet) HarnessForRole(role Role) Harness {
	if s == nil || s.router == nil {
		return nil
	}
	return s.router.HarnessForRole(role)
}

// Start starts every additional harness. The primary/chat harness remains
// owned by Service and is deliberately excluded.
//
// If one additional harness fails to start, previously started harnesses are
// closed in reverse order before the error is returned.
func (s *RoleSet) Start(ctx context.Context) error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("routed harness set is closed")
	}
	if s.started > 0 {
		return nil
	}

	for index, target := range s.additional {
		if err := target.Start(ctx); err != nil {
			var rollback []error
			for closeIndex := s.started - 1; closeIndex >= 0; closeIndex-- {
				if closeErr := s.additional[closeIndex].Close(); closeErr != nil {
					rollback = append(rollback, closeErr)
				}
			}
			s.started = 0
			return errors.Join(
				errors.New("start routed harness"),
				err,
				errors.Join(rollback...),
			)
		}
		s.started = index + 1
	}

	return nil
}

// Close closes additional harnesses in reverse startup order. It attempts every
// close and returns the combined errors, if any.
func (s *RoleSet) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	var errs []error
	for index := s.started - 1; index >= 0; index-- {
		if err := s.additional[index].Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.started = 0

	return errors.Join(errs...)
}
