package harness

import "sync"

// SessionWorkspace is the provider-neutral execution workspace bound to one
// session key: the directory provider processes execute in, plus additional
// sandbox writable roots.
//
// The binding changes execution CWD, protocol CWD, and sandbox roots only.
// It never moves session persistence locations, and it never creates a
// provider instance per task: supervisors and adapters stay shared, and only
// the session-scoped protocol points consult the binding.
type SessionWorkspace struct {
	Dir           string   `json:"dir"`
	WritableRoots []string `json:"writable_roots,omitempty"`
}

var (
	sessionWorkspacesMu sync.RWMutex
	sessionWorkspaces   = map[string]SessionWorkspace{}
)

// BindSessionWorkspace binds one session key to its execution workspace. The
// caller owns the binding's lifetime: it must release it when the isolated
// launch is superseded or finished, and a replacement launch binds its own
// new session key.
func BindSessionWorkspace(key string, workspace SessionWorkspace) {
	if key == "" || workspace.Dir == "" {
		return
	}
	sessionWorkspacesMu.Lock()
	defer sessionWorkspacesMu.Unlock()
	sessionWorkspaces[key] = workspace
}

// ReleaseSessionWorkspace removes one session's binding. Releasing an absent
// binding is a safe no-op.
func ReleaseSessionWorkspace(key string) {
	if key == "" {
		return
	}
	sessionWorkspacesMu.Lock()
	defer sessionWorkspacesMu.Unlock()
	delete(sessionWorkspaces, key)
}

// SessionWorkspaceFor reports the execution workspace bound to one session
// key. Adapters consult it only at session-scoped protocol points.
func SessionWorkspaceFor(key string) (SessionWorkspace, bool) {
	if key == "" {
		return SessionWorkspace{}, false
	}
	sessionWorkspacesMu.RLock()
	defer sessionWorkspacesMu.RUnlock()
	workspace, ok := sessionWorkspaces[key]
	return workspace, ok
}
