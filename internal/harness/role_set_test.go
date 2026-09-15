package harness

import (
	"context"
	"errors"
	"testing"

	"github.com/agent0ai/spynel/internal/core"
)

type roleSetFixture struct {
	starts   int
	closes   int
	startErr error
	closeErr error
}

func (f *roleSetFixture) Start(context.Context) error {
	f.starts++
	return f.startErr
}

func (*roleSetFixture) Send(
	context.Context,
	string,
	string,
	core.Emit,
) (string, bool, error) {
	return "", false, nil
}

func (*roleSetFixture) Interrupt(context.Context, string) (bool, error) {
	return false, nil
}

func (*roleSetFixture) ResetSession(string) error {
	return nil
}

func (*roleSetFixture) ThreadID(string) string {
	return ""
}

func (*roleSetFixture) IsActive(string) bool {
	return false
}

func (f *roleSetFixture) Close() error {
	f.closes++
	return f.closeErr
}

func TestRoleSetRoutesWithoutOwningPrimaryHarness(t *testing.T) {
	primary := &roleSetFixture{}
	developer := &roleSetFixture{}
	reviewer := &roleSetFixture{}

	set := NewRoleSet(
		primary,
		map[Role]Harness{
			RoleChat:      primary,
			RoleDeveloper: developer,
			RoleReviewer:  reviewer,
		},
		developer,
		reviewer,
	)

	if got := set.HarnessForRole(RoleChat); got != primary {
		t.Fatal("chat did not resolve to primary harness")
	}
	if got := set.HarnessForRole(RoleDeveloper); got != developer {
		t.Fatal("developer did not resolve to developer harness")
	}
	if got := set.HarnessForRole(RoleReviewer); got != reviewer {
		t.Fatal("reviewer did not resolve to reviewer harness")
	}

	if err := set.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	if primary.starts != 0 {
		t.Fatalf("primary starts = %d, want 0", primary.starts)
	}
	if developer.starts != 1 || reviewer.starts != 1 {
		t.Fatalf(
			"additional starts = developer:%d reviewer:%d",
			developer.starts,
			reviewer.starts,
		)
	}

	if err := set.Close(); err != nil {
		t.Fatal(err)
	}

	if primary.closes != 0 {
		t.Fatalf("primary closes = %d, want 0", primary.closes)
	}
	if developer.closes != 1 || reviewer.closes != 1 {
		t.Fatalf(
			"additional closes = developer:%d reviewer:%d",
			developer.closes,
			reviewer.closes,
		)
	}
}

func TestRoleSetRollsBackStartedHarnessesOnStartFailure(t *testing.T) {
	primary := &roleSetFixture{}
	first := &roleSetFixture{}
	second := &roleSetFixture{startErr: errors.New("boom")}
	third := &roleSetFixture{}

	set := NewRoleSet(
		primary,
		nil,
		first,
		second,
		third,
	)

	if err := set.Start(context.Background()); err == nil {
		t.Fatal("expected routed harness startup failure")
	}

	if first.starts != 1 || first.closes != 1 {
		t.Fatalf(
			"first lifecycle = starts:%d closes:%d",
			first.starts,
			first.closes,
		)
	}
	if second.starts != 1 {
		t.Fatalf("second starts = %d, want 1", second.starts)
	}
	if third.starts != 0 {
		t.Fatalf("third starts = %d, want 0", third.starts)
	}

	// Startup rollback already released every successfully started harness.
	// A later service shutdown must not close them again or touch harnesses
	// that never started.
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}

	if first.closes != 1 {
		t.Fatalf("first closes after final Close = %d, want 1", first.closes)
	}
	if second.closes != 0 {
		t.Fatalf("failed harness closes = %d, want 0", second.closes)
	}
	if third.closes != 0 {
		t.Fatalf("unstarted harness closes = %d, want 0", third.closes)
	}
}

func TestRoleSetCloseJoinsErrorsAndContinues(t *testing.T) {
	first := &roleSetFixture{closeErr: errors.New("first")}
	second := &roleSetFixture{closeErr: errors.New("second")}

	set := NewRoleSet(nil, nil, first, second)

	if err := set.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	err := set.Close()
	if err == nil {
		t.Fatal("expected combined close error")
	}

	if first.closes != 1 || second.closes != 1 {
		t.Fatalf(
			"close calls = first:%d second:%d",
			first.closes,
			second.closes,
		)
	}
}
