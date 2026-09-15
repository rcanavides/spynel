package orchestrator

import (
	"context"
	"testing"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/harness"
)

type roleRoutingHarnessFixture struct{}

func (*roleRoutingHarnessFixture) Start(context.Context) error {
	return nil
}

func (*roleRoutingHarnessFixture) Send(
	context.Context,
	string,
	string,
	core.Emit,
) (string, bool, error) {
	return "", false, nil
}

func (*roleRoutingHarnessFixture) Interrupt(context.Context, string) (bool, error) {
	return false, nil
}

func (*roleRoutingHarnessFixture) ResetSession(string) error {
	return nil
}

func (*roleRoutingHarnessFixture) ThreadID(string) string {
	return ""
}

func (*roleRoutingHarnessFixture) IsActive(string) bool {
	return false
}

func (*roleRoutingHarnessFixture) Close() error {
	return nil
}

func TestHarnessForPhaseRoutesByRole(t *testing.T) {
	fallback := &roleRoutingHarnessFixture{}
	developer := &roleRoutingHarnessFixture{}
	reviewer := &roleRoutingHarnessFixture{}

	manager := &Manager{
		Harness: fallback,
		HarnessRouter: harness.NewStaticRoleRouter(
			fallback,
			map[harness.Role]harness.Harness{
				harness.RoleDeveloper: developer,
				harness.RoleReviewer:  reviewer,
			},
		),
	}

	tests := []struct {
		name  string
		phase string
		want  harness.Harness
	}{
		{
			name:  "task implementation",
			phase: phaseTaskImplementation,
			want:  developer,
		},
		{
			name:  "goal planning",
			phase: phaseGoalPlanning,
			want:  developer,
		},
		{
			name:  "task review",
			phase: phaseTaskReview,
			want:  reviewer,
		},
		{
			name:  "goal review",
			phase: phaseGoalReview,
			want:  reviewer,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := manager.harnessForPhase(test.phase); got != test.want {
				t.Fatalf("harnessForPhase(%q) returned wrong harness", test.phase)
			}
		})
	}
}

func TestHarnessForPhaseFallsBackToLegacyHarness(t *testing.T) {
	fallback := &roleRoutingHarnessFixture{}

	manager := &Manager{
		Harness: fallback,
	}

	if got := manager.harnessForPhase(phaseTaskReview); got != fallback {
		t.Fatal("expected legacy harness fallback")
	}
}
