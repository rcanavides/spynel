package harness

import (
	"context"
	"testing"

	"github.com/agent0ai/spynel/internal/core"
)

type routerFixture struct {
	name string
}

func (*routerFixture) Start(context.Context) error {
	return nil
}

func (*routerFixture) Send(
	context.Context,
	string,
	string,
	core.Emit,
) (string, bool, error) {
	return "", false, nil
}

func (*routerFixture) Interrupt(context.Context, string) (bool, error) {
	return false, nil
}

func (*routerFixture) ResetSession(string) error {
	return nil
}

func (*routerFixture) ThreadID(string) string {
	return ""
}

func (*routerFixture) IsActive(string) bool {
	return false
}

func (*routerFixture) Close() error {
	return nil
}

func TestStaticRoleRouterSelectsConfiguredRole(t *testing.T) {
	fallback := &routerFixture{name: "fallback"}
	developer := &routerFixture{name: "developer"}

	router := NewStaticRoleRouter(fallback, map[Role]Harness{
		RoleDeveloper: developer,
	})

	if got := router.HarnessForRole(RoleDeveloper); got != developer {
		t.Fatalf("developer route = %v, want configured developer harness", got)
	}
}

func TestStaticRoleRouterFallsBack(t *testing.T) {
	fallback := &routerFixture{name: "fallback"}
	developer := &routerFixture{name: "developer"}

	router := NewStaticRoleRouter(fallback, map[Role]Harness{
		RoleDeveloper: developer,
	})

	if got := router.HarnessForRole(RoleReviewer); got != fallback {
		t.Fatalf("reviewer route = %v, want fallback harness", got)
	}
}

func TestStaticRoleRouterCopiesRoutes(t *testing.T) {
	fallback := &routerFixture{name: "fallback"}
	developer := &routerFixture{name: "developer"}
	replacement := &routerFixture{name: "replacement"}

	routes := map[Role]Harness{
		RoleDeveloper: developer,
	}

	router := NewStaticRoleRouter(fallback, routes)

	routes[RoleDeveloper] = replacement

	if got := router.HarnessForRole(RoleDeveloper); got != developer {
		t.Fatalf("route changed after caller mutation: got %v", got)
	}
}
