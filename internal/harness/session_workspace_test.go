package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

// recordCount snapshots how many fixture records a phase starts from so
// assertions only inspect that phase's own invocations.
func recordCount(t *testing.T, path string) int {
	t.Helper()
	return len(readFixtureRecords(t, path))
}

// WS8: adapters apply the session-bound execution workspace at their
// session-scoped protocol points, and ReleaseSessionWorkspace restores the
// configured workspace root. Session persistence never moves with a binding.

func boundDir(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	bound := filepath.Join(t.TempDir(), "isolated worktree ü")
	if err := os.MkdirAll(bound, 0o700); err != nil {
		t.Fatal(err)
	}
	return root, bound
}

func waitFinal(t *testing.T, events chan core.Event) core.Event {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the fixture provider")
		return core.Event{}
	}
}

// WS8 Codex: thread/start and turn/start carry the bound directory and its
// writable root; releasing the binding restores the configured root.
func TestWS8CodexSessionWorkspaceBoundCwd(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "codex-lifecycle")
	definition, _ := Lookup("codex")
	adapter, err := definition.factory(HarnessConfig{Command: command, Cwd: root, ApprovalPolicy: "plan", Sandbox: "workspace-write", SessionsFile: filepath.Join(root, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := adapter.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	events := make(chan core.Event, 4)
	baseline := recordCount(t, logPath)
	BindSessionWorkspace("orchestrator:tasks:task_implementation:1:ln-iso", SessionWorkspace{Dir: filepath.Join(root, "wt-codex")})
	if err := os.MkdirAll(filepath.Join(root, "wt-codex"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := adapter.Send(ctx, "orchestrator:tasks:task_implementation:1:ln-iso", "probe", func(event core.Event) {
		if event.Done {
			events <- event
		}
	}); err != nil {
		t.Fatal(err)
	}
	waitFinal(t, events)

	boundSeen, rootsSeen := false, false
	for _, record := range readFixtureRecords(t, logPath)[baseline:] {
		if record.Method != "thread/start" && record.Method != "turn/start" {
			continue
		}
		var params map[string]any
		if err := json.Unmarshal(record.Params, &params); err != nil {
			t.Fatal(err)
		}
		if params["cwd"] != filepath.Join(root, "wt-codex") {
			t.Fatalf("%s cwd = %v, want the bound worktree", record.Method, params["cwd"])
		}
		boundSeen = true
		policy, _ := params["sandboxPolicy"].(map[string]any)
		roots, _ := policy["writableRoots"].([]any)
		for _, root0 := range roots {
			if root0 == filepath.Join(root, "wt-codex") {
				rootsSeen = true
			}
		}
	}
	if !boundSeen {
		t.Fatal("no session-scoped codex call observed the binding")
	}
	if !rootsSeen {
		t.Fatal("turn/start sandboxPolicy must include the bound writable root")
	}

	// Releasing the binding restores the configured workspace root.
	ReleaseSessionWorkspace("orchestrator:tasks:task_implementation:1:ln-iso")
	releaseBaseline := recordCount(t, logPath)
	events = make(chan core.Event, 4)
	if _, _, err := adapter.Send(ctx, "unbound-session", "probe", func(event core.Event) {
		if event.Done {
			events <- event
		}
	}); err != nil {
		t.Fatal(err)
	}
	waitFinal(t, events)
	for _, record := range readFixtureRecords(t, logPath)[releaseBaseline:] {
		if record.Method != "turn/start" {
			continue
		}
		var params map[string]any
		if err := json.Unmarshal(record.Params, &params); err != nil {
			t.Fatal(err)
		}
		if params["cwd"] == filepath.Join(root, "wt-codex") {
			t.Fatalf("released binding still applied to %s", record.Method)
		}
	}
}

// WS8 Claude: the per-turn process runs in the bound directory.
func TestWS8ClaudeSessionWorkspaceProcessDir(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "claude-stream")
	definition, _ := Lookup("claude-code")
	adapter, err := definition.factory(HarnessConfig{Command: command, Cwd: root, ApprovalPolicy: "plan", SessionsFile: filepath.Join(root, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := adapter.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	bound := filepath.Join(root, "wt-claude")
	if err := os.MkdirAll(bound, 0o700); err != nil {
		t.Fatal(err)
	}
	key := "orchestrator:tasks:task_review:1:ln-iso-review"
	baseline := recordCount(t, logPath)
	BindSessionWorkspace(key, SessionWorkspace{Dir: bound})
	events := make(chan core.Event, 4)
	if _, _, err := adapter.Send(ctx, key, "probe", func(event core.Event) {
		if event.Done {
			events <- event
		}
	}); err != nil {
		t.Fatal(err)
	}
	waitFinal(t, events)
	ReleaseSessionWorkspace(key)

	invocations := 0
	for _, record := range readFixtureRecords(t, logPath)[baseline:] {
		if record.Kind == "invocation" {
			invocations++
			if record.Cwd != bound {
				t.Fatalf("claude process cwd = %q, want %q", record.Cwd, bound)
			}
		}
	}
	if invocations == 0 {
		t.Fatal("no claude process invocation recorded")
	}
}

// WS8 Pi: the RPC process runs in the bound directory while session storage
// stays at the configured SessionsFile location.
func TestWS8PiSessionWorkspaceProcessDirKeepsSessionStorage(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "pi-lifecycle")
	definition, _ := Lookup("pi")
	sessionsFile := filepath.Join(root, "sessions.json")
	adapter, err := definition.factory(HarnessConfig{Command: command, Cwd: root, ApprovalPolicy: "plan", SessionsFile: sessionsFile})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := adapter.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	bound := filepath.Join(root, "wt-pi")
	if err := os.MkdirAll(bound, 0o700); err != nil {
		t.Fatal(err)
	}
	key := "orchestrator:tasks:task_implementation:2:ln-iso"
	baseline := recordCount(t, logPath)
	BindSessionWorkspace(key, SessionWorkspace{Dir: bound})
	events := make(chan core.Event, 4)
	if _, _, err := adapter.Send(ctx, key, "probe", func(event core.Event) {
		if event.Done {
			events <- event
		}
	}); err != nil {
		t.Fatal(err)
	}
	waitFinal(t, events)
	ReleaseSessionWorkspace(key)

	sawBoundProcess, sawOriginalSessionDir := false, false
	wantSessionDir := filepath.Join(filepath.Dir(sessionsFile), "pi-sessions")
	for _, record := range readFixtureRecords(t, logPath)[baseline:] {
		if record.Kind != "invocation" {
			continue
		}
		if record.Cwd == bound {
			sawBoundProcess = true
		}
		for _, arg := range record.Args {
			if arg == wantSessionDir {
				sawOriginalSessionDir = true
			}
			if arg == filepath.Join(bound, ".spynel", "runtime", "pi-sessions") {
				t.Fatal("session storage must never move with the bound directory")
			}
		}
	}
	if !sawBoundProcess {
		t.Fatal("pi process did not run in the bound directory")
	}
	if !sawOriginalSessionDir {
		t.Fatal("pi session storage must stay at the configured sessions location")
	}
}

// WS8 ACP: session/new carries the bound directory; the session policy
// includes it so a workspace change never resumes a stale session.
func TestWS8ACPSessionWorkspaceSessionCwd(t *testing.T) {
	command, root, logPath := portableHarnessFixture(t, "acp-lifecycle")
	definition, _ := Lookup("agent-zero")
	adapter, err := definition.factory(HarnessConfig{Command: command, Cwd: root, Args: []string{"acp"}, SessionsFile: filepath.Join(root, "sessions.json")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := adapter.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	bound := filepath.Join(root, "wt-acp")
	if err := os.MkdirAll(bound, 0o700); err != nil {
		t.Fatal(err)
	}
	key := "orchestrator:tasks:task_implementation:3:ln-iso"
	baseline := recordCount(t, logPath)
	BindSessionWorkspace(key, SessionWorkspace{Dir: bound})
	events := make(chan core.Event, 4)
	if _, _, err := adapter.Send(ctx, key, "probe", func(event core.Event) {
		if event.Done {
			events <- event
		}
	}); err != nil {
		t.Fatal(err)
	}
	waitFinal(t, events)

	sawBoundSession := false
	for _, record := range readFixtureRecords(t, logPath)[baseline:] {
		if record.Method != "session/new" {
			continue
		}
		var params map[string]any
		if err := json.Unmarshal(record.Params, &params); err != nil {
			t.Fatal(err)
		}
		if params["cwd"] != bound {
			t.Fatalf("session/new cwd = %v, want %q", params["cwd"], bound)
		}
		sawBoundSession = true
	}
	if !sawBoundSession {
		t.Fatal("no session/new observed the binding")
	}

	// A second launch with a different bound directory must not resume the
	// first session: the policy participates in session identity.
	other := filepath.Join(root, "wt-acp-two")
	if err := os.MkdirAll(other, 0o700); err != nil {
		t.Fatal(err)
	}
	replacementKey := "orchestrator:tasks:task_implementation:3:ln-iso-two"
	replacementBaseline := recordCount(t, logPath)
	BindSessionWorkspace(replacementKey, SessionWorkspace{Dir: other})
	events = make(chan core.Event, 4)
	if _, _, err := adapter.Send(ctx, replacementKey, "probe", func(event core.Event) {
		if event.Done {
			events <- event
		}
	}); err != nil {
		t.Fatal(err)
	}
	waitFinal(t, events)
	ReleaseSessionWorkspace(key)
	ReleaseSessionWorkspace(replacementKey)
	for _, record := range readFixtureRecords(t, logPath)[replacementBaseline:] {
		if record.Method == "session/resume" {
			t.Fatal("a different bound directory must start a fresh session, never resume the old one")
		}
	}
}
