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
	notification := &roleRoutingHarnessFixture{}
	heartbeat := &roleRoutingHarnessFixture{}

	manager := &Manager{
		Harness: fallback,
		HarnessRouter: harness.NewStaticRoleRouter(
			fallback,
			map[harness.Role]harness.Harness{
				harness.RoleDeveloper:    developer,
				harness.RoleReviewer:     reviewer,
				harness.RoleNotification: notification,
				harness.RoleHeartbeat:    heartbeat,
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
		{
			name:  "notification",
			phase: "notification",
			want:  notification,
		},
		{
			name:  "semantic heartbeat",
			phase: "semantic_heartbeat",
			want:  heartbeat,
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
	replacement := &roleRoutingHarnessFixture{}

	manager := &Manager{
		Harness: fallback,
	}

	if got := manager.harnessForPhase(phaseTaskReview); got != fallback {
		t.Fatal("expected legacy harness fallback")
	}

	// Legacy callers may replace Manager.Harness after construction.
	// Without an explicit role router, phase routing must observe the
	// currently assigned harness rather than a captured startup value.
	manager.Harness = replacement

	if got := manager.harnessForPhase(phaseTaskReview); got != replacement {
		t.Fatal("expected current legacy harness after replacement")
	}
}

func TestHarnessForPhaseUsesRuntimeOwnershipTargets(t *testing.T) {
	registry := harness.NewRegistry()
	created := 0
	registry.Register("provider", func(harness.HarnessConfig) (harness.Harness, error) {
		created++
		return &roleRoutingHarnessFixture{}, nil
	})
	runtime := harness.NewRuntime(registry, harness.HarnessConfig{Name: "provider"})
	defer runtime.Close()
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{Harness: runtime.AcquireRole(harness.RoleChat), HarnessRouter: runtime}
	target := manager.harnessForPhase(phaseTaskImplementation)
	if _, _, err := target.Send(context.Background(), "shared", "prompt", nil); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		phase string
		role  harness.Role
	}{
		{phaseTaskImplementation, harness.RoleDeveloper}, {phaseGoalPlanning, harness.RoleDeveloper},
		{phaseTaskReview, harness.RoleReviewer}, {phaseGoalReview, harness.RoleReviewer},
		{"notification", harness.RoleNotification}, {"semantic_heartbeat", harness.RoleHeartbeat},
	} {
		got := manager.harnessForPhase(test.phase)
		if got != runtime.AcquireRole(test.role) {
			t.Fatalf("phase %s bypasses runtime target", test.phase)
		}
		if !got.IsActive("shared") {
			t.Fatalf("phase %s lost shared provider activity", test.phase)
		}
		if _, ok := got.(harness.ControlSender); !ok {
			t.Fatal("control capability lost")
		}
		if _, ok := got.(interface{ Close() error }); ok {
			t.Fatal("role target exposes lifecycle")
		}
	}
	if created != 1 {
		t.Fatalf("constructed %d providers", created)
	}
}
