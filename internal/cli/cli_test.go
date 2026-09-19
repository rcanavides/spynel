package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/app"
	"github.com/agent0ai/spynel/internal/channel"
	"github.com/agent0ai/spynel/internal/channel/tui"
	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/history"
	"github.com/agent0ai/spynel/internal/instance"
	"github.com/agent0ai/spynel/internal/localapi"
	"github.com/agent0ai/spynel/internal/orchestrator"
	"github.com/agent0ai/spynel/internal/updater"
	"github.com/agent0ai/spynel/internal/workspace"
)

type heldCLIHarness struct {
	mu      sync.Mutex
	emits   map[string]core.Emit
	prompts map[string][]string
	threads map[string]string
}

func TestRecordCommandFailurePersistsGenericEvidenceWithoutErrorContent(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	configPath := config.PathForRoot(root)
	recordCommandFailure([]string{"serve", "--config", configPath}, errors.New("authorization: Bearer must-not-persist"))
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "command-failure-reader")
	defer runtimeState.Close()
	found := false
	for _, entry := range runtimeState.Logs() {
		found = found || entry.Component == "process" && entry.Event == "command_failed"
		if strings.Contains(entry.Text, "must-not-persist") {
			t.Fatalf("top-level error content leaked into runtime log: %#v", entry)
		}
	}
	if !found {
		t.Fatalf("command failure evidence missing: %#v", runtimeState.Logs())
	}
}

func TestConfigPathArgument(t *testing.T) {
	for _, test := range []struct {
		args []string
		want string
	}{
		{args: []string{"status"}, want: ""},
		{args: []string{"status", "--config", "/tmp/project/.spynel/config.yaml"}, want: "/tmp/project/.spynel/config.yaml"},
		{args: []string{"send", "--config=/tmp/other.yaml", "hello"}, want: "/tmp/other.yaml"},
	} {
		if got := configPathArgument(test.args); got != test.want {
			t.Errorf("configPathArgument(%q) = %q, want %q", test.args, got, test.want)
		}
	}
}

func TestBareWorkspaceDiscoveryDistinguishesLocalAncestorAndSymlink(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(root, "nested", "child")
	if err := os.MkdirAll(child, 0o755); err != nil {
		t.Fatal(err)
	}
	discovery, err := discoverBareWorkspace(child)
	if err != nil {
		t.Fatal(err)
	}
	if !discovery.ancestorFound || discovery.parentRoot != root || discovery.discoveredConfig != config.PathForRoot(root) || discovery.localConfig != config.PathForRoot(child) {
		t.Fatalf("ancestor discovery = %#v", discovery)
	}

	if err := workspace.Init(child, false); err != nil {
		t.Fatal(err)
	}
	discovery, err = discoverBareWorkspace(child)
	if err != nil {
		t.Fatal(err)
	}
	if discovery.ancestorFound || discovery.discoveredConfig != config.PathForRoot(child) {
		t.Fatalf("local discovery = %#v", discovery)
	}

	linkParent := t.TempDir()
	link := filepath.Join(linkParent, "workspace-link")
	if err := os.Symlink(child, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	canonical, err := canonicalDirectory(link)
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(child)
	if err != nil {
		t.Fatal(err)
	}
	if canonical != want {
		t.Fatalf("canonical symlink directory = %q, want %q", canonical, want)
	}
}

func TestBareInteractiveStartupOrdersChoiceDirectoryAndServerConstruction(t *testing.T) {
	const (
		launchRoot   = "/canonical/parent/child"
		parentRoot   = "/canonical/parent"
		localConfig  = "/canonical/parent/child/.spynel/config.yaml"
		parentConfig = "/canonical/parent/.spynel/config.yaml"
	)
	tests := []struct {
		name       string
		choice     tui.WorkspaceChoice
		wantEvents []string
	}{
		{
			name: "parent changes directory before server construction", choice: tui.WorkspaceChoiceUseParent,
			wantEvents: []string{"canonical", "discover:" + launchRoot, "choice:" + launchRoot + ":" + parentRoot, "chdir:" + parentRoot, "server:" + parentConfig},
		},
		{
			name: "initialize creates only launch workspace before directory and server", choice: tui.WorkspaceChoiceInitializeHere,
			wantEvents: []string{"canonical", "discover:" + launchRoot, "choice:" + launchRoot + ":" + parentRoot, "init:" + launchRoot, "chdir:" + launchRoot, "server:" + localConfig},
		},
		{
			name:       "exit has no filesystem or server effects",
			choice:     tui.WorkspaceChoiceExit,
			wantEvents: []string{"canonical", "discover:" + launchRoot, "choice:" + launchRoot + ":" + parentRoot},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var events []string
			runtime := bareInteractiveRuntime{
				canonicalLaunchDirectory: func() (string, error) {
					events = append(events, "canonical")
					return launchRoot, nil
				},
				discoverWorkspace: func(root string) (bareWorkspaceDiscovery, error) {
					events = append(events, "discover:"+root)
					return bareWorkspaceDiscovery{localConfig: localConfig, discoveredConfig: parentConfig, parentRoot: parentRoot, ancestorFound: true}, nil
				},
				runParentChoice: func(_ context.Context, launch, parent string, initialize func() error) (tui.WorkspaceChoice, error) {
					events = append(events, "choice:"+launch+":"+parent)
					if test.choice == tui.WorkspaceChoiceInitializeHere {
						if err := initialize(); err != nil {
							return tui.WorkspaceChoiceExit, err
						}
					}
					return test.choice, nil
				},
				initializeWorkspace: func(root string, force bool) error {
					if force {
						t.Fatal("startup initialization unexpectedly forced an overwrite")
					}
					events = append(events, "init:"+root)
					return nil
				},
				changeDirectory: func(root string) error {
					events = append(events, "chdir:"+root)
					return nil
				},
				startServer: func(path string, withTUI bool, version string, restartArgs []string) error {
					if !withTUI || version != "test" || restartArgs != nil {
						t.Fatalf("server arguments = %q, %t, %q, %#v", path, withTUI, version, restartArgs)
					}
					events = append(events, "server:"+path)
					return nil
				},
			}
			if err := runBareInteractiveWithRuntime("test", runtime); err != nil {
				t.Fatal(err)
			}
			if strings.Join(events, "\n") != strings.Join(test.wantEvents, "\n") {
				t.Fatalf("startup events = %#v, want %#v", events, test.wantEvents)
			}
		})
	}
}

func TestBareInteractiveStartupHandlesLocalUninitializedAndInitializationFailure(t *testing.T) {
	const launchRoot = "/canonical/new"
	const localConfig = "/canonical/new/.spynel/config.yaml"
	initFailure := errors.New("cannot initialize")
	for _, test := range []struct {
		name        string
		initialized bool
		initErr     error
		wantEvents  []string
		wantErr     error
	}{
		{name: "successful initialization", initialized: true, wantEvents: []string{"screen", "init", "chdir", "server"}},
		{name: "cancelled initialization", wantEvents: []string{"screen"}},
		{name: "failed initialization", initErr: initFailure, wantEvents: []string{"screen", "init"}, wantErr: initFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			var events []string
			runtime := bareInteractiveRuntime{
				canonicalLaunchDirectory: func() (string, error) { return launchRoot, nil },
				discoverWorkspace: func(string) (bareWorkspaceDiscovery, error) {
					return bareWorkspaceDiscovery{localConfig: localConfig}, nil
				},
				runInitialization: func(_ context.Context, root string, initialize func() error) (bool, error) {
					events = append(events, "screen")
					if test.initErr != nil || test.initialized {
						if err := initialize(); err != nil {
							return false, err
						}
					}
					return test.initialized, nil
				},
				initializeWorkspace: func(root string, force bool) error {
					events = append(events, "init")
					return test.initErr
				},
				changeDirectory: func(string) error { events = append(events, "chdir"); return nil },
				startServer:     func(string, bool, string, []string) error { events = append(events, "server"); return nil },
			}
			err := runBareInteractiveWithRuntime("test", runtime)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("startup error = %v, want %v", err, test.wantErr)
			}
			if strings.Join(events, "\n") != strings.Join(test.wantEvents, "\n") {
				t.Fatalf("startup events = %#v, want %#v", events, test.wantEvents)
			}
		})
	}
}

func TestBareInteractiveChoiceAlignsRealProcessDirectoryBeforeServer(t *testing.T) {
	original, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(original) })
	parent := t.TempDir()
	child := filepath.Join(parent, "child")
	if err := os.Mkdir(child, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		choice tui.WorkspaceChoice
		want   string
	}{
		{name: "parent", choice: tui.WorkspaceChoiceUseParent, want: parent},
		{name: "initialized child", choice: tui.WorkspaceChoiceInitializeHere, want: child},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := os.Chdir(original); err != nil {
				t.Fatal(err)
			}
			runtime := bareInteractiveRuntime{
				canonicalLaunchDirectory: func() (string, error) { return child, nil },
				discoverWorkspace: func(string) (bareWorkspaceDiscovery, error) {
					return bareWorkspaceDiscovery{
						localConfig: config.PathForRoot(child), discoveredConfig: config.PathForRoot(parent),
						parentRoot: parent, ancestorFound: true,
					}, nil
				},
				runParentChoice: func(_ context.Context, _, _ string, initialize func() error) (tui.WorkspaceChoice, error) {
					if test.choice == tui.WorkspaceChoiceInitializeHere {
						if err := initialize(); err != nil {
							return tui.WorkspaceChoiceExit, err
						}
					}
					return test.choice, nil
				},
				initializeWorkspace: func(root string, force bool) error {
					if root != child || force {
						t.Fatalf("initialize request = %q, force %t", root, force)
					}
					return nil
				},
				changeDirectory: os.Chdir,
				startServer: func(string, bool, string, []string) error {
					got, err := os.Getwd()
					if err != nil {
						return err
					}
					if got != test.want {
						return fmt.Errorf("server CWD = %q, want %q", got, test.want)
					}
					return nil
				},
			}
			if err := runBareInteractiveWithRuntime("test", runtime); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBareInteractiveInitializedRootStartsWithoutChoice(t *testing.T) {
	const root = "/canonical/local"
	const configPath = "/canonical/local/.spynel/config.yaml"
	choiceCalled := false
	serverCalled := false
	runtime := bareInteractiveRuntime{
		canonicalLaunchDirectory: func() (string, error) { return root, nil },
		discoverWorkspace: func(string) (bareWorkspaceDiscovery, error) {
			return bareWorkspaceDiscovery{localConfig: configPath, discoveredConfig: configPath}, nil
		},
		runParentChoice: func(context.Context, string, string, func() error) (tui.WorkspaceChoice, error) {
			choiceCalled = true
			return tui.WorkspaceChoiceExit, nil
		},
		startServer: func(path string, withTUI bool, version string, restartArgs []string) error {
			serverCalled = path == configPath && withTUI && version == "test" && restartArgs == nil
			return nil
		},
	}
	if err := runBareInteractiveWithRuntime("test", runtime); err != nil {
		t.Fatal(err)
	}
	if choiceCalled || !serverCalled {
		t.Fatalf("choice called = %t, server called correctly = %t", choiceCalled, serverCalled)
	}
}

func TestOnlyBareCommandRequestsInteractiveWorkspaceChoice(t *testing.T) {
	if !bareInteractiveRequested(nil) {
		t.Fatal("bare command did not request interactive startup")
	}
	for _, args := range [][]string{
		{"serve"}, {"serve", "--tui"}, {"serve", "--config", "/explicit/.spynel/config.yaml"},
		{"init", "--dir", "/explicit"}, {"status", "--config", "/explicit/.spynel/config.yaml"},
	} {
		if bareInteractiveRequested(args) {
			t.Fatalf("explicit or noninteractive command unexpectedly requested workspace choice: %#v", args)
		}
	}
}

func TestBareFailureLoggingDoesNotAdoptAncestorBeforeChoice(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(root, "child")
	if err := os.Mkdir(child, 0o755); err != nil {
		t.Fatal(err)
	}
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(child); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previous) })
	if path, ok := failureConfigPath(nil); ok || path != "" {
		t.Fatalf("bare failure path = %q, %t; ancestor must remain untouched", path, ok)
	}
}

func TestWorkflowListAliasPreservesSharedAndListOptions(t *testing.T) {
	for _, test := range []struct {
		input []string
		want  []string
	}{
		{input: []string{"--limit", "50"}, want: []string{"open", "--limit", "50"}},
		{input: []string{"--config", "workspace.yml", "--days", "14", "--detail"}, want: []string{"--config", "workspace.yml", "open", "--days", "14", "--detail"}},
		{input: []string{"--json", "waiting", "--limit", "2"}, want: []string{"--json", "waiting", "--limit", "2"}},
		{input: []string{"review", "--days", "7"}, want: []string{"review", "--days", "7"}},
		{input: []string{"failed", "--detail"}, want: []string{"failed", "--detail"}},
	} {
		if got := workflowListAliasArgs(test.input); strings.Join(got, "\x00") != strings.Join(test.want, "\x00") {
			t.Errorf("workflowListAliasArgs(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}

func TestWorkflowListAliasesAreDocumentedForExternalPrograms(t *testing.T) {
	for _, want := range []string{
		"spynel tasks [flags] [VIEW]",
		"spynel goals [flags] [VIEW]",
		"open|recent|active|review|waiting|done|failed|all",
		"--config PATH",
		"--conversation NAME",
		"--days N",
		"--limit N",
		"--detail",
		"shared response event as NDJSON",
	} {
		if !strings.Contains(helpText, want) {
			t.Fatalf("CLI help does not expose %q:\n%s", want, helpText)
		}
	}
}

func TestInstructionsCommandReportsValidationWithoutContents(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfgPath := config.PathForRoot(root)
	cfg, _ := config.Load(cfgPath)
	secret := "do-not-print-in-status"
	if err := os.WriteFile(cfg.StatePath("instructions", "agent-chat.md"), []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	if err := runInstructionsCommand([]string{"--config", cfgPath}, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), secret) || !strings.Contains(output.String(), "chat: .spynel/instructions/agent-chat.md — valid") {
		t.Fatalf("instruction inspection output = %q", output.String())
	}
}

func TestInstructionsCommandRejectsEscapingInstructionsDirectory(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfgPath := config.PathForRoot(root)
	instructionsPath := filepath.Join(root, ".spynel", "instructions")
	outsidePath := filepath.Join(t.TempDir(), "external-instructions")
	if err := os.Rename(instructionsPath, outsidePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsidePath, instructionsPath); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	var output strings.Builder
	err := runInstructionsCommand([]string{"--config", cfgPath}, &output)
	if err == nil || !strings.Contains(err.Error(), "unsafe or invalid") {
		t.Fatalf("symlinked instructions command error = %v", err)
	}
	if strings.Count(output.String(), "invalid (.spynel/instructions path must not be a symbolic link)") != 5 {
		t.Fatalf("symlinked instructions command output = %q", output.String())
	}
}

func TestTaskInspectShowsEffectiveFailSafeReviewPolicy(t *testing.T) {
	for _, test := range []struct {
		name  string
		front string
		want  []string
	}{
		{name: "explicit false", front: "review_required: false\n", want: []string{"Review required: false"}},
		{name: "missing", front: "id: task\n", want: []string{"Review required: true"}},
		{name: "malformed", front: "review_required: nope\n", want: []string{"Review required: true", "Policy warning:", "treated as review required"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "task.md")
			if err := os.WriteFile(path, []byte("---\n"+test.front+"---\n# Task\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := inspectTaskPolicy(path, &output); err != nil {
				t.Fatal(err)
			}
			for _, want := range test.want {
				if !strings.Contains(output.String(), want) {
					t.Fatalf("inspection = %q, missing %q", output.String(), want)
				}
			}
		})
	}
}

func TestTaskInspectAppliesWorkspaceReviewMode(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Reviews = config.TaskReviewsNever
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.StatePath("tasks", "todo"), "inspect.md")
	if err := os.WriteFile(path, []byte("---\nid: task\nreview_required: true\n---\n# Task\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := inspectTaskPolicy(path, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Configured task review mode: never") || !strings.Contains(output.String(), "Review required: false") {
		t.Fatalf("inspection = %q", output.String())
	}
}

func TestNotifyCommandUsesDurableHistoryWithoutHarness(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	store := history.New(cfg.StatePath("history"))
	if _, err := store.Append("cli", "alerts", history.Entry{Role: "user", Content: "known"}); err != nil {
		t.Fatal(err)
	}
	if err := runNotifyCommand([]string{"--config", cfg.Path, "--origin", "cli/alerts", "complete"}, "test"); err != nil {
		t.Fatal(err)
	}
	entries, _, err := store.RecentEntries("cli", "alerts", 10, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[1].Sender != "Spy" || entries[1].Content != "complete" {
		t.Fatalf("history = %#v", entries)
	}
}

func TestNotifyCommandSupportsConcreteAgentArguments(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	store := history.New(cfg.StatePath("history"))
	if _, err := store.Append("cli", "alerts", history.Entry{Role: "user", Content: "known"}); err != nil {
		t.Fatal(err)
	}
	if err := runNotifyCommand([]string{"--workdir", root, "--origin", "cli/alerts", "--message", "concrete delivery"}, "test"); err != nil {
		t.Fatal(err)
	}
	entries, _, err := store.RecentEntries("cli", "alerts", 10, 1000)
	if err != nil || len(entries) != 2 || entries[1].Content != "concrete delivery" {
		t.Fatalf("concrete notification history = %#v, %v", entries, err)
	}
}

func TestNotifyCommandRecentAuthorizedIsExplicitAndMutuallyExclusive(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.PathForRoot(root))
	store := history.New(cfg.StatePath("history"))
	if _, err := store.Append("cli", "recent", history.Entry{At: time.Now().UTC(), Role: "user", Content: "known"}); err != nil {
		t.Fatal(err)
	}
	if err := runNotifyCommand([]string{"--config", cfg.Path, "--recent-authorized", "--message", "recent delivery"}, "test"); err != nil {
		t.Fatal(err)
	}
	entries, _, err := store.RecentEntries("cli", "recent", 10, 1000)
	if err != nil || len(entries) != 2 || entries[1].Content != "recent delivery" {
		t.Fatalf("recent notification history = %#v, %v", entries, err)
	}
	for _, args := range [][]string{
		{"--config", cfg.Path, "--message", "missing mode"},
		{"--config", cfg.Path, "--origin", "cli/recent", "--recent-authorized", "--message", "conflict"},
	} {
		if err := runNotifyCommand(args, "test"); err == nil || !strings.Contains(err.Error(), "exactly one") {
			t.Fatalf("routing mode conflict %v = %v", args, err)
		}
	}
}

func TestNotifyMessageRejectsEmptyStdinAndAmbiguousInputs(t *testing.T) {
	if _, err := notificationMessageText(nil, true, false, "", strings.NewReader("")); err == nil || !strings.Contains(err.Error(), "standard input is empty") {
		t.Fatalf("historical empty-stdin failure = %v", err)
	}
	text, err := notificationMessageText(nil, false, true, "Hello there", strings.NewReader(""))
	if err != nil || text != "Hello there" {
		t.Fatalf("generated --message path = %q, %v", text, err)
	}
	for _, test := range []struct {
		args       []string
		stdin      bool
		messageSet bool
		message    string
	}{
		{args: []string{"positional"}, messageSet: true, message: "flag"},
		{stdin: true, messageSet: true, message: "flag"},
		{messageSet: true, message: "   "},
	} {
		if _, err := notificationMessageText(test.args, test.stdin, test.messageSet, test.message, strings.NewReader("stdin")); err == nil {
			t.Fatalf("ambiguous notification input accepted: %#v", test)
		}
	}
}

func TestNotifyCommandRejectsWorkdirConfigCombination(t *testing.T) {
	err := runNotifyCommand([]string{"--workdir", "/workspace", "--config", "/workspace/.spynel/config.yaml", "--origin", "tui/local", "--message", "no"}, "test")
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("workdir/config conflict = %v", err)
	}
}

func newHeldCLIHarness() *heldCLIHarness {
	return &heldCLIHarness{
		emits: map[string]core.Emit{}, prompts: map[string][]string{}, threads: map[string]string{},
	}
}

func (h *heldCLIHarness) Start(context.Context) error { return nil }

func (h *heldCLIHarness) Send(_ context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	h.mu.Lock()
	previous := h.emits[key]
	steered := previous != nil
	thread := h.threads[key]
	if thread == "" {
		thread = "thread-" + key
		h.threads[key] = thread
	}
	h.prompts[key] = append(h.prompts[key], prompt)
	h.emits[key] = emit
	h.mu.Unlock()
	if previous != nil {
		previous(core.Event{Kind: core.EventStatus, Done: true, ThreadID: thread})
	}
	emit(core.Event{Kind: core.EventStatus, Text: "active", ThreadID: thread})
	return thread, steered, nil
}

func (h *heldCLIHarness) Interrupt(_ context.Context, key string) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.emits[key] == nil {
		return false, nil
	}
	delete(h.emits, key)
	return true, nil
}

func (h *heldCLIHarness) ResetSession(key string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.threads, key)
	return nil
}

func (h *heldCLIHarness) ThreadID(key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.threads[key]
}

func (h *heldCLIHarness) IsActive(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.emits[key] != nil
}

func (h *heldCLIHarness) Close() error { return nil }

func (h *heldCLIHarness) finish(key, text string) {
	h.mu.Lock()
	emit := h.emits[key]
	delete(h.emits, key)
	h.mu.Unlock()
	if emit != nil {
		emit(core.Event{Kind: core.EventFinal, Text: text, Done: true})
	}
}

func TestCompleteRunReplacesProcessForRestartRequest(t *testing.T) {
	want := []string{"serve", "--tui", "--config", "/tmp/project/.spynel/config.yaml"}
	request := &restartRequest{args: append([]string(nil), want...)}
	called := false
	err := completeRun(fmt.Errorf("server stopped: %w", request), func(args []string) error {
		called = true
		if strings.Join(args, "\x00") != strings.Join(want, "\x00") {
			t.Fatalf("restart arguments = %#v, want %#v", args, want)
		}
		args[0] = "mutated"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("restart process function was not called")
	}
	if request.args[0] != "serve" {
		t.Fatal("restart request arguments were exposed to mutation")
	}
}

func TestCompleteRunPreservesOrdinaryErrors(t *testing.T) {
	want := errors.New("failed")
	called := false
	got := completeRun(want, func([]string) error {
		called = true
		return nil
	})
	if !errors.Is(got, want) || called {
		t.Fatalf("completeRun error = %v, restart called = %t", got, called)
	}
}

func TestNPMUpdateRequestReturnsLauncherExitCodeWithoutReplacingProcess(t *testing.T) {
	called := false
	err := completeRun(&updateRequest{}, func([]string) error {
		called = true
		return nil
	})
	exit, ok := err.(interface{ ExitCode() int })
	if !ok || exit.ExitCode() != npmUpdateExitCode || called {
		t.Fatalf("update request = %T %v, restart called = %t", err, err, called)
	}
}

func TestNPMUpdateRequestPublishesInitContinuationArguments(t *testing.T) {
	placeholder, err := os.CreateTemp(os.TempDir(), "spynel-update-")
	if err != nil {
		t.Fatal(err)
	}
	statePath := placeholder.Name()
	if err := placeholder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(statePath) })
	t.Setenv("SPYNEL_NPM_UPDATE_STATE", statePath)
	want := []string{"serve", "--tui", "--config", "/tmp/project/.spynel/config.yaml"}
	request := &updateRequest{args: want}
	if code := request.ExitCode(); code != npmUpdateExitCode {
		t.Fatalf("update exit code = %d", code)
	}
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		Args []string `json:"args"`
	}
	if err := json.Unmarshal(data, &state); err != nil || strings.Join(state.Args, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("update state = %#v, %v", state, err)
	}
}

func TestOfflineUpdateInstallReturnsControlToNPMLauncher(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	packageRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(packageRoot, "npm", "vendor"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packageRoot, "package.json"), []byte(`{"name":"spynel","version":"1.2.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packageRoot, "npm", "vendor", ".installed.json"), []byte(`{"version":"1.2.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Link(executable, filepath.Join(packageRoot, "npm", "vendor", "spynel")); err != nil {
		t.Fatal(err)
	}
	registry := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"name":"spynel","version":"1.3.0"}`))
	}))
	defer registry.Close()
	t.Setenv("SPYNEL_NPM_PACKAGE_ROOT", packageRoot)
	t.Setenv("SPYNEL_NPM_LAUNCHER_MANAGED", "1")
	t.Setenv("SPYNEL_NPM_REGISTRY_URL", registry.URL)
	t.Setenv("SPYNEL_NPM_COORDINATED_UPDATES", "")
	var output bytes.Buffer
	err = runFrameworkMessageMode(config.PathForRoot(root), "updates", "/update install", "1.2.0", messageRunOptions{Output: &output})
	if err == nil || !strings.Contains(err.Error(), "launcher does not support coordinated updates") {
		t.Fatalf("older launcher accepted an update: %v", err)
	}
	t.Setenv("SPYNEL_NPM_COORDINATED_UPDATES", "1")
	err = runFrameworkMessageMode(config.PathForRoot(root), "updates", "/update install", "1.2.0", messageRunOptions{Output: &output})
	exit, ok := err.(interface{ ExitCode() int })
	if !ok || exit.ExitCode() != npmUpdateExitCode || !strings.Contains(output.String(), "Updating Spynel") {
		t.Fatalf("offline update = %T %v, output %q", err, err, output.String())
	}
}

func TestInitialConnectionStatusesReflectConfiguration(t *testing.T) {
	cfg := config.Default()
	cfg.Channels.Telegram.Enabled = true

	statuses := initialConnectionStatuses(cfg)
	if len(statuses) != 2 {
		t.Fatalf("status count = %d", len(statuses))
	}
	if statuses[0].Name != "telegram" || statuses[0].State != channel.ConnectionConnecting {
		t.Fatalf("Telegram status = %#v", statuses[0])
	}
	if statuses[1].Name != "whatsapp" || statuses[1].State != channel.ConnectionUnconfigured {
		t.Fatalf("WhatsApp status = %#v", statuses[1])
	}
}

func TestInitNoStartCreatesWorkspaceWithoutEnteringTUI(t *testing.T) {
	root := filepath.Join(t.TempDir(), "new-workspace")
	if err := Run([]string{"init", "--no-start", "--dir", root}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(config.PathForRoot(root)); err != nil {
		t.Fatalf("initialized config: %v", err)
	}
	if !strings.Contains(helpText, "--no-start") || !strings.Contains(helpText, "continue into the TUI") {
		t.Fatalf("init continuation is not documented in help:\n%s", helpText)
	}
}

func TestSendCommandValidatesScriptableArguments(t *testing.T) {
	if err := run([]string{"send"}, "test"); err == nil || !strings.Contains(err.Error(), "usage: spynel send") {
		t.Fatalf("missing send text error = %v", err)
	}
	if err := run([]string{"send", "--conversation", "", "hello"}, "test"); err == nil || !strings.Contains(err.Error(), "cannot be empty") {
		t.Fatalf("empty conversation error = %v", err)
	}
	if !strings.Contains(helpText, "spynel send") || !strings.Contains(helpText, "--conversation") {
		t.Fatalf("send command is not documented in CLI help:\n%s", helpText)
	}
}

func TestCLIMessageSupportsStdinStreamingAndJSONEvents(t *testing.T) {
	text, err := cliMessageText(nil, true, strings.NewReader("first line\nsecond line\n"))
	if err != nil || text != "first line\nsecond line" {
		t.Fatalf("stdin message = %q, %v", text, err)
	}
	if _, err := cliMessageText([]string{"body"}, true, strings.NewReader("stdin")); err == nil {
		t.Fatal("CLI accepted both positional text and --stdin")
	}
	if _, err := cliMessageText(nil, true, strings.NewReader(strings.Repeat("x", maxCLIInputBytes+1))); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized stdin error = %v", err)
	}

	handler := func(_ context.Context, message core.Message, emit core.Emit) error {
		if message.Conversation != "stream-test" || !message.FollowupOnly {
			t.Fatalf("message = %#v", message)
		}
		emit(core.Event{Kind: core.EventStatus, Text: "steered"})
		emit(core.Event{Kind: core.EventDelta, Text: "routine preamble\nhello "})
		emit(core.Event{Kind: core.EventDelta, Text: "world"})
		finalText := "hello world"
		emit(core.Event{Kind: core.EventFinal, Text: "routine preamble\nhello world", FinalText: &finalText, Done: true})
		return nil
	}
	var finalOnly bytes.Buffer
	if err := runMessageWithOutput(context.Background(), handler, "stream-test", "follow", messageRunOptions{FollowupOnly: true, Output: &finalOnly}); err != nil {
		t.Fatal(err)
	}
	if finalOnly.String() != "hello world\n" {
		t.Fatalf("final-only output = %q", finalOnly.String())
	}
	var streamed bytes.Buffer
	if err := runMessageWithOutput(context.Background(), handler, "stream-test", "follow", messageRunOptions{Stream: true, FollowupOnly: true, Output: &streamed}); err != nil {
		t.Fatal(err)
	}
	if streamed.String() != "routine preamble\nhello world\n" {
		t.Fatalf("streamed output = %q", streamed.String())
	}

	var events bytes.Buffer
	if err := runMessageWithOutput(context.Background(), handler, "stream-test", "follow", messageRunOptions{JSON: true, FollowupOnly: true, Output: &events}); err != nil {
		t.Fatal(err)
	}
	var decoded []core.Event
	for _, line := range strings.Split(strings.TrimSpace(events.String()), "\n") {
		var event core.Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		decoded = append(decoded, event)
	}
	if len(decoded) != 4 || decoded[1].Kind != core.EventDelta || decoded[3].Kind != core.EventFinal || !decoded[3].Done || decoded[3].FinalText == nil || *decoded[3].FinalText != "hello world" {
		t.Fatalf("NDJSON events = %#v", decoded)
	}
}

func TestCLIMessageCopiesRepeatableAttachmentsIntoWorkspace(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(source, []byte("attachment body"), 0o600); err != nil {
		t.Fatal(err)
	}
	text, err := addCLIAttachments(context.Background(), cfg, "inspect this", []string{source})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "inspect this\n\n[Attachment notes.txt]") || !strings.Contains(text, filepath.ToSlash(cfg.StatePath("attachments", "cli"))) {
		t.Fatalf("attachment message = %q", text)
	}
	matches, err := filepath.Glob(cfg.StatePath("attachments", "cli", "notes*.txt"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("copied attachments = %#v, %v", matches, err)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil || string(data) != "attachment body" {
		t.Fatalf("copied attachment = %q, %v", data, err)
	}
}

func TestConversationCLIListsShowsAndBranchesDiskBackedHistory(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	store := history.New(cfg.StatePath("history"))
	if _, err := store.Append("telegram", "TG-42", history.Entry{At: time.Now().UTC(), Role: "user", Sender: "alice", Content: "inspect this"}); err != nil {
		t.Fatal(err)
	}

	var listed bytes.Buffer
	if err := listCLIConversations([]string{"--config", cfg.Path, "--json"}, &listed); err != nil {
		t.Fatal(err)
	}
	var records []conversationListRecord
	if err := json.Unmarshal(listed.Bytes(), &records); err != nil || len(records) != 1 || records[0].Channel != "telegram" || records[0].Conversation != "TG-42" {
		t.Fatalf("conversation list = %#v, %v", records, err)
	}

	var shown bytes.Buffer
	if err := showCLIConversation([]string{"--config", cfg.Path, "--json", "telegram", "TG-42"}, &shown); err != nil {
		t.Fatal(err)
	}
	var tail conversationTail
	if err := json.Unmarshal(shown.Bytes(), &tail); err != nil || len(tail.Entries) != 1 || tail.Entries[0].Content != "inspect this" {
		t.Fatalf("conversation tail = %#v, %v", tail, err)
	}

	var resumed bytes.Buffer
	if err := resumeCLIConversation([]string{"--config", cfg.Path, "--json", "telegram", "TG-42"}, &resumed); err != nil {
		t.Fatal(err)
	}
	var branch conversationBranch
	if err := json.Unmarshal(resumed.Bytes(), &branch); err != nil || branch.Channel != "cli" || !strings.HasPrefix(branch.Conversation, "resume-") {
		t.Fatalf("conversation branch = %#v, %v", branch, err)
	}
	entries, _, err := store.Entries("cli", branch.Conversation)
	if err != nil || len(entries) != 1 || entries[0].Content != "inspect this" {
		t.Fatalf("branched entries = %#v, %v", entries, err)
	}
}

func TestStatusCLIEmitsStructuredNonSecretState(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	waitingPath := filepath.Join(root, ".spynel", "tasks", "waiting", "waiting.md")
	if err := os.WriteFile(waitingPath, []byte("---\nid: waiting\nstatus: waiting\n---\n# Waiting\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runStatusCLICommand([]string{"--config", cfg.Path, "--conversation", "automation", "--json"}, "test", &output); err != nil {
		t.Fatal(err)
	}
	var status app.StatusSnapshot
	if err := json.Unmarshal(output.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Title == "" || status.Sandbox == "" || status.HarnessState == "" || len(status.Connections) != 2 {
		t.Fatalf("CLI status = %#v", status)
	}
	if status.TasksActive != 1 || status.TasksWaiting != 1 {
		t.Fatalf("CLI durable waiting count = active %d waiting %d", status.TasksActive, status.TasksWaiting)
	}
	if strings.Contains(output.String(), "token") {
		t.Fatalf("CLI status exposed configuration secrets: %s", output.String())
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["theme"]; ok {
		t.Fatalf("structured CLI status retained theme: %s", output.String())
	}
	if _, ok := fields["thread"]; ok {
		t.Fatalf("structured CLI status retained thread: %s", output.String())
	}
	for _, field := range []string{"tasks_active", "tasks_waiting", "goals_active", "heartbeat_state"} {
		if _, ok := fields[field]; !ok {
			t.Fatalf("structured CLI status is missing %q: %s", field, output.String())
		}
	}
}

func TestOfflineFrameworkCommandDoesNotRequireStartedHarness(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runFrameworkMessageMode(config.PathForRoot(root), "framework", "/help commands", "test", messageRunOptions{Output: &output}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "/status") || !strings.Contains(output.String(), "/extension list") {
		t.Fatalf("framework command output = %q", output.String())
	}
}

func TestOfflineCLIExtensionCanHandleMessageWithoutAHarness(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell extension fixture")
	}
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = ""
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}
	extension := filepath.Join(cfg.Resolve(cfg.Extensions.Directory), "offline-tool")
	if err := os.MkdirAll(extension, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := "name: offline-tool\nhooks:\n  message.received: [\"./hook.sh\"]\n"
	script := "#!/bin/sh\nIFS= read -r input\ncase \"$input\" in\n  *\"handle offline\"*) printf '%s\\n' '{\"cancel\":true,\"message\":\"handled without harness\"}' ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(extension, ".spynel-extension.yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(extension, "hook.sh"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := runMessageMode(cfg.Path, "offline", "handle offline", "test", messageRunOptions{Output: &output}); err != nil {
		t.Fatal(err)
	}
	if output.String() != "handled without harness\n" {
		t.Fatalf("offline extension output = %q", output.String())
	}
	if err := runMessageMode(cfg.Path, "offline", "requires a harness", "test", messageRunOptions{Output: &bytes.Buffer{}}); err == nil || !strings.Contains(err.Error(), "harness unavailable") {
		t.Fatalf("unhandled offline message error = %v", err)
	}
}

func TestCLIJoinsWorkspaceOwnerAndStrictlySteersActiveConversation(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		extension := filepath.Join(cfg.Resolve(cfg.Extensions.Directory), "cli-tool")
		if err := os.MkdirAll(extension, 0o700); err != nil {
			t.Fatal(err)
		}
		manifest := "name: cli-tool\nhooks:\n  message.received: [\"./hook.sh\"]\n"
		script := "#!/bin/sh\ninput=$(cat)\ncase \"$input\" in\n  *\"invoke custom tool\"*) printf '%s\\n' '{\"cancel\":true,\"message\":\"extension handled CLI\"}' ;;\nesac\n"
		if err := os.WriteFile(filepath.Join(extension, ".spynel-extension.yaml"), []byte(manifest), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(extension, "hook.sh"), []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	target := newHeldCLIHarness()
	service := app.New(cfg, target)
	election, err := instance.New(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	service.SetPrimaryInstanceID(election.ID())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	token, err := election.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := election.TryAcquire(listener.Addr().String(), token); err != nil || !acquired {
		t.Fatalf("acquire workspace owner = %t, %v", acquired, err)
	}
	serverContext, stopServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- (&localapi.Server{Service: service, Token: token}).Serve(serverContext, listener)
	}()
	defer func() {
		stopServer()
		if err := <-serverDone; err != nil {
			t.Errorf("local API: %v", err)
		}
		_ = election.Release(token)
	}()
	readyContext, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopWaiting()
	if _, err := localapi.NewClient(election).WaitReady(readyContext); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		var toolOutput bytes.Buffer
		if err := runMessageMode(cfg.Path, "tool", "invoke custom tool", "test", messageRunOptions{Output: &toolOutput}); err != nil {
			t.Fatal(err)
		}
		if toolOutput.String() != "extension handled CLI\n" || target.IsActive("chat:cli:tool") {
			t.Fatalf("CLI extension output = %q, harness active = %t", toolOutput.String(), target.IsActive("chat:cli:tool"))
		}
	}

	var firstOutput bytes.Buffer
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runMessageMode(cfg.Path, "active", "start the work", "test", messageRunOptions{Stream: true, Output: &firstOutput})
	}()
	key := "chat:cli:active"
	deadline := time.Now().Add(5 * time.Second)
	for !target.IsActive(key) {
		if time.Now().After(deadline) {
			t.Fatal("initial CLI turn did not become active through the owner")
		}
		time.Sleep(5 * time.Millisecond)
	}
	var jobsOutput bytes.Buffer
	if err := runFrameworkMessageMode(cfg.Path, "inspect", "/jobs", "test", messageRunOptions{Output: &jobsOutput}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jobsOutput.String(), "conversation") || !strings.Contains(jobsOutput.String(), "1▶ · running · cli") || strings.Contains(jobsOutput.String(), "cli/active") || strings.Contains(jobsOutput.String(), "start the work") || strings.Contains(jobsOutput.String(), "↻") || !strings.Contains(jobsOutput.String(), "Use `/job info <number>`") || !strings.Contains(jobsOutput.String(), "Use `/job kill <number>`") {
		t.Fatalf("plain CLI live jobs output = %q", jobsOutput.String())
	}
	jobs := service.Runtime.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("owner jobs = %#v", jobs)
	}
	var infoOutput bytes.Buffer
	if err := runFrameworkMessageMode(cfg.Path, "inspect", fmt.Sprintf("/job info %d", jobs[0].ID), "test", messageRunOptions{Output: &infoOutput}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(infoOutput.String(), "Provider steps (▶): 1 (live conversation)") || strings.Contains(infoOutput.String(), "Implementation attempts") {
		t.Fatalf("plain CLI live job info = %q", infoOutput.String())
	}

	var followupOutput bytes.Buffer
	followupDone := make(chan error, 1)
	go func() {
		followupDone <- runMessageMode(cfg.Path, "active", "focus on the API", "test", messageRunOptions{FollowupOnly: true, Stream: true, Output: &followupOutput})
	}()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follow-up did not release the first CLI response")
	}
	target.mu.Lock()
	prompts := append([]string(nil), target.prompts[key]...)
	target.mu.Unlock()
	if len(prompts) != 2 || !strings.Contains(prompts[1], "focus on the API") {
		t.Fatalf("owner harness prompts = %#v", prompts)
	}
	target.finish(key, "follow-up complete")
	select {
	case err := <-followupDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follow-up CLI did not receive the final response")
	}
	if firstOutput.String() != "" || followupOutput.String() != "follow-up complete\n" {
		t.Fatalf("CLI outputs = first %q, follow-up %q", firstOutput.String(), followupOutput.String())
	}
	var recentOutput bytes.Buffer
	if err := runFrameworkMessageMode(cfg.Path, "inspect", "/jobs recent", "test", messageRunOptions{Output: &recentOutput}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(recentOutput.String(), fmt.Sprintf("Job %d", jobs[0].Number)) || strings.Contains(recentOutput.String(), jobs[0].StableID) || strings.Contains(recentOutput.String(), "start the work") {
		t.Fatalf("plain CLI archived jobs output = %q", recentOutput.String())
	}
	var archivedOutput bytes.Buffer
	if err := runFrameworkMessageMode(cfg.Path, "inspect", fmt.Sprintf("/job output %d", jobs[0].Number), "test", messageRunOptions{Output: &archivedOutput}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(archivedOutput.String(), "follow-up complete") || !strings.Contains(archivedOutput.String(), fmt.Sprintf("Job output %d", jobs[0].Number)) || strings.Contains(archivedOutput.String(), jobs[0].StableID) {
		t.Fatalf("plain CLI archived job output = %q", archivedOutput.String())
	}
	if err := runMessageMode(cfg.Path, "active", "too late", "test", messageRunOptions{FollowupOnly: true, Output: &bytes.Buffer{}}); err == nil || !strings.Contains(err.Error(), "no active execution") {
		t.Fatalf("inactive follow-up error = %v", err)
	}
}

func TestOwnerlessCLICleanupFencesConcurrentPrimaryStartup(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	old := time.Now().UTC().Add(-8 * 24 * time.Hour)
	if _, err := service.History.Append("cli", "old-ownerless", history.Entry{At: old, Role: "assistant", Content: "old"}); err != nil {
		t.Fatal(err)
	}

	inside := make(chan struct{})
	allowCleanup := make(chan struct{})
	cleanupDone := make(chan error, 1)
	go func() {
		ran, runErr := runOwnerlessCleanup(cfg, func() error {
			close(inside)
			<-allowCleanup
			return runMessageWithOutput(context.Background(), service.Handle, "manual", "/cleanup 7", messageRunOptions{Output: &bytes.Buffer{}})
		})
		if runErr == nil && !ran {
			runErr = errors.New("fallback cleanup did not run")
		}
		cleanupDone <- runErr
	}()
	<-inside

	owner, err := instance.New(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	token, err := owner.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	ownerDone := make(chan error, 1)
	go func() {
		_, acquired, acquireErr := owner.TryAcquire("127.0.0.1:10001", token)
		if acquireErr == nil && !acquired {
			acquireErr = errors.New("concurrent primary was not acquired")
		}
		ownerDone <- acquireErr
	}()
	select {
	case err := <-ownerDone:
		t.Fatalf("primary started before fallback cleanup completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(allowCleanup)
	if err := <-cleanupDone; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(service.History.Path("cli", "old-ownerless")); !os.IsNotExist(err) {
		t.Fatalf("eligible history remains after fenced cleanup: %v", err)
	}
	if err := <-ownerDone; err != nil {
		t.Fatal(err)
	}
}

func TestOwnerlessCLICleanupWaitsForIdleSecondaryAfterOwnerRelease(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	old := time.Now().UTC().Add(-8 * 24 * time.Hour)
	if _, err := service.History.Append("tui", "idle-secondary", history.Entry{At: old, Role: "assistant", Content: "still open"}); err != nil {
		t.Fatal(err)
	}

	owner, err := instance.New(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	secondary, err := instance.New(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	ownerToken, _ := owner.NewToken()
	if _, acquired, err := owner.TryAcquire("127.0.0.1:10001", ownerToken); err != nil || !acquired {
		t.Fatalf("owner acquire = %t, %v", acquired, err)
	}
	if err := owner.Release(ownerToken); err != nil {
		t.Fatal(err)
	}

	cleanupCalled := false
	ran, err := runOwnerlessCleanup(cfg, func() error {
		cleanupCalled = true
		return runMessageWithOutput(context.Background(), service.Handle, "manual", "/cleanup 7", messageRunOptions{Output: &bytes.Buffer{}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if ran || cleanupCalled {
		t.Fatalf("fallback cleanup crossed owner-release gap: ran=%t called=%t", ran, cleanupCalled)
	}
	if _, err := os.Stat(service.History.Path("tui", "idle-secondary")); err != nil {
		t.Fatalf("idle secondary history was removed: %v", err)
	}

	secondaryToken, _ := secondary.NewToken()
	if _, acquired, err := secondary.TryAcquire("127.0.0.1:10002", secondaryToken); err != nil || !acquired {
		t.Fatalf("secondary acquire = %t, %v", acquired, err)
	}
}

func TestRunMessageCompletesWhenFollowUpReleasesItsEmitter(t *testing.T) {
	handler := func(_ context.Context, _ core.Message, emit core.Emit) error {
		emit(core.Event{Kind: core.EventStatus, Done: true})
		return nil
	}
	if err := runMessageWithHandler(context.Background(), handler, "follow-up", "message"); err != nil {
		t.Fatal(err)
	}
}

func TestRunMessageCompletesWhenExtensionCancelsWithoutReply(t *testing.T) {
	handler := func(context.Context, core.Message, core.Emit) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runMessageWithOutput(ctx, handler, "cancelled", "message", messageRunOptions{Output: &bytes.Buffer{}}); err != nil {
		t.Fatal(err)
	}
}

func TestBuildServiceUsesConfiguredHarnessSandbox(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Sandbox = "read-only"
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if _, ok := service.Harness.(interface{ Start(context.Context) error }); ok {
		t.Fatal("single-provider operational target exposes Start")
	}
	if _, ok := service.Harness.(interface{ Close() error }); ok {
		t.Fatal("single-provider operational target exposes Close")
	}
	runtimeHarness, ok := service.Harness.(interface {
		HarnessConfig() harness.HarnessConfig
	})
	if !ok || runtimeHarness.HarnessConfig().Sandbox != "read-only" {
		t.Fatalf("runtime sandbox = %#v, configurable = %t", runtimeHarness, ok)
	}
}

func TestTUIStartupResumesHistoryOnlyForInitialElectionWinner(t *testing.T) {
	store := history.New(t.TempDir())
	if _, err := store.Append("tui", "local-old", history.Entry{At: time.Now().Add(-time.Hour), Role: "user", Content: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append("tui", "local-latest", history.Entry{At: time.Now(), Role: "assistant", Content: "latest"}); err != nil {
		t.Fatal(err)
	}
	ownerLease := instance.Lease{InstanceID: "winner"}
	if !shouldResumeTUIHistory(false, ownerLease, "winner") {
		t.Fatal("initial election winner did not qualify to resume TUI history")
	}
	for _, test := range []struct {
		name       string
		hadPrimary bool
		lease      instance.Lease
	}{
		{name: "existing owner", hadPrimary: true, lease: ownerLease},
		{name: "initial race loser", lease: instance.Lease{InstanceID: "other"}},
		{name: "handoff record", lease: instance.Lease{InstanceID: "winner", HandoffTo: "target"}},
	} {
		if shouldResumeTUIHistory(test.hadPrimary, test.lease, "winner") {
			t.Fatalf("%s unexpectedly qualified to resume TUI history", test.name)
		}
	}
	conversation, err := selectTUIConversation(store, "winner", true)
	if err != nil || conversation != "local-latest" {
		t.Fatalf("resumed conversation = %q, %v", conversation, err)
	}
	conversation, err = selectTUIConversation(store, "secondary", false)
	if err != nil || conversation != "local-secondary" {
		t.Fatalf("secondary conversation = %q, %v", conversation, err)
	}
}

func TestStartupConnectionStatusIsVisibleBoundedAndOptional(t *testing.T) {
	var output bytes.Buffer
	status := newStartupConnectionStatus(&output, true)
	status.connecting()
	status.connected()
	want := "Connecting to the existing Spynel primary…\nConnected to the existing Spynel primary.\n"
	if output.String() != want {
		t.Fatalf("successful startup status = %q", output.String())
	}

	output.Reset()
	status.failed(fmt.Errorf("%w: another host/container environment; bearer-secret", localapi.ErrForeignLoopback))
	if !strings.Contains(output.String(), "another host/container environment") || strings.Contains(output.String(), "bearer-secret") {
		t.Fatalf("foreign startup status = %q", output.String())
	}

	output.Reset()
	status.failed(errors.New("open /private/workspace: credential-secret"))
	if strings.Contains(output.String(), "/private/workspace") || strings.Contains(output.String(), "credential-secret") || !strings.Contains(output.String(), "retry or exit") {
		t.Fatalf("sanitized startup status = %q", output.String())
	}

	output.Reset()
	disabled := newStartupConnectionStatus(&output, false)
	disabled.connecting()
	disabled.connected()
	disabled.failed(errors.New("ignored"))
	if output.Len() != 0 {
		t.Fatalf("nonterminal startup output = %q", output.String())
	}
}

func TestOwnerElectionRunsOneServerAndHandsOffOnExit(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Root = root
	cfg.Path = config.PathForRoot(root)
	cfg.Harness.Name = ""
	cfg.Channels.Telegram.Enabled = false
	cfg.Channels.WhatsApp.Enabled = false
	cfg.Orchestrator.Enabled = false
	cfg.Extensions.Enabled = false
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}
	first, err := instance.New(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	second, err := instance.New(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	firstContext, stopFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- runOwnerElection(firstContext, cfg, "test", first, func() {}, func(updater.Result) {})
	}()
	waitContext, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopWaiting()
	client := localapi.NewClient(first)
	if _, err := client.WaitReady(waitContext); err != nil {
		stopFirst()
		t.Fatal(err)
	}
	lease, err := first.Current()
	if err != nil || lease.InstanceID != first.ID() {
		stopFirst()
		t.Fatalf("first lease = %#v, %v", lease, err)
	}

	secondContext, stopSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- runOwnerElection(secondContext, cfg, "test", second, func() {}, func(updater.Result) {})
	}()
	time.Sleep(2 * instance.RetryInterval)
	lease, err = second.Current()
	if err != nil || lease.InstanceID != first.ID() {
		stopFirst()
		stopSecond()
		t.Fatalf("second contender displaced healthy owner: %#v, %v", lease, err)
	}
	updated := cfg
	updated.Channels.TUI.Title = "Reloaded by successor"
	if err := config.Save(updated); err != nil {
		stopFirst()
		stopSecond()
		t.Fatal(err)
	}

	stopFirst()
	if err := <-firstDone; err != nil {
		stopSecond()
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		lease, err = second.Current()
		if err == nil && lease.InstanceID == second.ID() {
			break
		}
		if time.Now().After(deadline) {
			stopSecond()
			t.Fatalf("secondary did not take over: %#v, %v", lease, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	state, err := client.WaitReady(waitContext)
	if err != nil {
		stopSecond()
		t.Fatal(err)
	}
	if state.Title != "Reloaded by successor" {
		stopSecond()
		t.Fatalf("successor used stale startup config: %#v", state)
	}
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestOwnerElectionPromotesAfterObservedPrimaryBecomesStale(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Root = root
	cfg.Path = config.PathForRoot(root)
	cfg.Harness.Name = ""
	cfg.Channels.Telegram.Enabled = false
	cfg.Channels.WhatsApp.Enabled = false
	cfg.Orchestrator.Enabled = false
	cfg.Extensions.Enabled = false
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}

	stateDirectory := cfg.StatePath()
	if err := os.MkdirAll(filepath.Join(stateDirectory, "runtime"), 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	stalled := instance.Lease{
		InstanceID:  "stalled-primary",
		PID:         1,
		Endpoint:    "127.0.0.1:1",
		Token:       "stalled-primary-token",
		StartedAt:   now.Add(-time.Hour),
		HeartbeatAt: now.Add(-instance.StaleAfter + 250*time.Millisecond),
	}
	data, err := json.Marshal(stalled)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDirectory, "runtime", "primary.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}

	contender, err := instance.New(stateDirectory)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runOwnerElection(ctx, cfg, "test", contender, func() {}, func(updater.Result) {}) }()

	waitContext, stopWaiting := context.WithTimeout(context.Background(), 6*time.Second)
	defer stopWaiting()
	if _, err := localapi.NewClient(contender).WaitReady(waitContext); err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	lease, err := contender.Current()
	if err != nil || lease.InstanceID != contender.ID() {
		cancel()
		<-done
		t.Fatalf("promoted lease = %#v, %v", lease, err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPrimaryCommandHandsOwnershipToRequestingTUI(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Root = root
	cfg.Path = config.PathForRoot(root)
	cfg.Harness.Name = ""
	cfg.Channels.Telegram.Enabled = false
	cfg.Channels.WhatsApp.Enabled = false
	cfg.Orchestrator.Enabled = false
	cfg.Extensions.Enabled = false
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}
	first, err := instance.New(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	second, err := instance.New(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	firstContext, stopFirst := context.WithCancel(context.Background())
	secondContext, stopSecond := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		firstDone <- runOwnerElection(firstContext, cfg, "test", first, func() {}, func(updater.Result) {})
	}()
	waitContext, stopWaiting := context.WithTimeout(context.Background(), 8*time.Second)
	defer stopWaiting()
	if _, err := localapi.NewClient(first).WaitReady(waitContext); err != nil {
		stopFirst()
		t.Fatal(err)
	}
	go func() {
		secondDone <- runOwnerElection(secondContext, cfg, "test", second, func() {}, func(updater.Result) {})
	}()
	defer func() {
		stopFirst()
		stopSecond()
		if err := <-firstDone; err != nil {
			t.Errorf("first election: %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Errorf("second election: %v", err)
		}
	}()
	client := localapi.NewClient(second)
	var response core.Event
	if err := client.Handle(waitContext, core.Message{Channel: "tui", Conversation: "local-" + second.ID(), Text: "/primary"}, func(event core.Event) {
		if event.Kind == core.EventFinal {
			response = event
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(response.Text, "Primary handoff requested") {
		t.Fatalf("primary response = %#v", response)
	}
	for {
		lease, err := second.Current()
		if err == nil && lease.InstanceID == second.ID() && lease.HandoffTo == "" {
			break
		}
		select {
		case <-waitContext.Done():
			t.Fatalf("requesting TUI did not become primary: lease %#v, error %v", lease, err)
		case <-time.After(25 * time.Millisecond):
		}
	}
	if _, err := client.WaitReady(waitContext); err != nil {
		t.Fatalf("new primary API did not become ready: %v", err)
	}
}

func TestPromotionRecordsConfigurationReloadFailure(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.Root = root
	cfg.Path = config.PathForRoot(root)
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}
	election, err := instance.New(cfg.StatePath())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.WriteFile(cfg.Path, []byte("invalid: [\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	term, startErr := startPrimaryTerm(context.Background(), cfg, "test", election, listener, "unused", func() {}, func(updater.Result) {})
	if term != nil || startErr == nil {
		t.Fatalf("promotion with invalid config = %#v, %v", term, startErr)
	}
	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "reload-reader")
	defer runtimeState.Close()
	found := false
	for _, entry := range runtimeState.Logs() {
		found = found || entry.Component == "config" && entry.Event == "reload_failed"
	}
	if !found {
		t.Fatalf("configuration reload failure evidence missing: %#v", runtimeState.Logs())
	}
}

func TestStandaloneUpdateRestartAndProactiveEligibility(t *testing.T) {
	for _, args := range [][]string{nil, {"version"}, {"version", "--quiet"}, {"serve", "--tui", "--config", "/project/.spynel/config.yaml"}} {
		called := false
		err := completeRun(&updateRequest{args: args, standalone: true, manager: &updater.Manager{InstallRoot: t.TempDir()}}, func(got []string) error {
			called = true
			want := args
			if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
				t.Fatalf("restart args: %q", got)
			}
			return nil
		})
		if err != nil || !called {
			t.Fatalf("restart: %v, %t", err, called)
		}
	}
	t.Setenv("SPYNEL_SKIP_UPDATE_CHECK", "")
	if !standaloneChecksEligible(nil, true) || standaloneChecksEligible(nil, false) || standaloneChecksEligible([]string{"serve", "--automatic-startup"}, true) {
		t.Fatal("incorrect standalone check eligibility")
	}
	t.Setenv("SPYNEL_SKIP_UPDATE_CHECK", "1")
	if standaloneChecksEligible(nil, true) {
		t.Fatal("ignored check suppression")
	}
}

func TestBuildServiceWithoutRoutingKeepsSingleHarness(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}

	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

}

func TestBuildServiceConstructsUniqueRoutedHarnesses(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}

	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Model = ""
	cfg.Harness.ReasoningEffort = ""
	cfg.Harness.ServiceMode = ""
	cfg.Harness.Sandbox = "danger-full-access"
	cfg.Harness.Routing = &config.HarnessRouting{
		Developer: "codex",
		Reviewer:  "claude-code",
		Heartbeat: "codex",
	}

	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	roleRuntime := service.Orchestrator.HarnessRouter

	developer := roleRuntime.HarnessForRole(harness.RoleDeveloper)
	reviewer := roleRuntime.HarnessForRole(harness.RoleReviewer)
	heartbeat := roleRuntime.HarnessForRole(harness.RoleHeartbeat)
	notification := roleRuntime.HarnessForRole(harness.RoleNotification)

	if developer == service.Harness {
		t.Fatal("developer unexpectedly resolved to primary harness")
	}
	if reviewer == service.Harness {
		t.Fatal("reviewer unexpectedly resolved to primary harness")
	}
	if targetHarnessConfig(developer).Name != targetHarnessConfig(heartbeat).Name {
		t.Fatal("roles using the same provider did not share one supervisor")
	}
	if targetHarnessConfig(notification).Name != targetHarnessConfig(service.Harness).Name {
		t.Fatal("unconfigured notification role did not fall back to primary harness")
	}
	if developer == reviewer {
		t.Fatal("different routed providers unexpectedly share one supervisor")
	}

	developerConfigurable, ok := developer.(interface {
		HarnessConfig() harness.HarnessConfig
	})
	if !ok {
		t.Fatal("developer supervisor does not expose HarnessConfig")
	}

	developerConfig := developerConfigurable.HarnessConfig()
	if developerConfig.Name != "codex" {
		t.Fatalf("developer harness name = %q, want codex", developerConfig.Name)
	}
	if developerConfig.Model != "" {
		t.Fatalf("developer inherited primary model %q", developerConfig.Model)
	}
	if developerConfig.Effort != "" {
		t.Fatalf("developer inherited primary effort %q", developerConfig.Effort)
	}
	if developerConfig.ServiceMode != "" {
		t.Fatalf("developer inherited primary service mode %q", developerConfig.ServiceMode)
	}
	if developerConfig.Sandbox != "danger-full-access" {
		t.Fatalf("developer sandbox = %q, want shared execution policy", developerConfig.Sandbox)
	}

	reviewerConfigurable, ok := reviewer.(interface {
		HarnessConfig() harness.HarnessConfig
	})
	if !ok {
		t.Fatal("reviewer supervisor does not expose HarnessConfig")
	}

	if got := reviewerConfigurable.HarnessConfig().Name; got != "claude-code" {
		t.Fatalf("reviewer harness name = %q, want claude-code", got)
	}

	primaryConfigurable, ok := service.Harness.(interface {
		HarnessConfig() harness.HarnessConfig
	})
	if !ok {
		t.Fatal("primary supervisor does not expose HarnessConfig")
	}

	primaryConfig := primaryConfigurable.HarnessConfig()
	if primaryConfig.Name != "agent-zero" {
		t.Fatalf("primary harness name = %q, want agent-zero", primaryConfig.Name)
	}
	if primaryConfig.Sandbox != "danger-full-access" {
		t.Fatalf("primary sandbox = %q, want preserved value", primaryConfig.Sandbox)
	}
}

func TestNewRoutedHarnessDoesNotInheritPrimaryInference(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}

	// A valid inference-capable primary may have explicit provider settings.
	// A different routed provider must not inherit those values.
	cfg.Harness.Name = "codex"
	cfg.Harness.Model = "primary-model"
	cfg.Harness.ReasoningEffort = "high"
	cfg.Harness.ServiceMode = ""

	runtimeState := app.NewRuntimeAt(
		cfg.StatePath("runtime", "logs"),
		"routed-inference-test",
	)
	defer runtimeState.Close()

	runtimeConfig := harnessSupervisorConfig(
		cfg,
		"claude-code",
		"test",
		runtimeState,
		false,
	)

	if runtimeConfig.Name != "claude-code" {
		t.Fatalf("routed harness name = %q, want claude-code", runtimeConfig.Name)
	}
	if runtimeConfig.Model != "" {
		t.Fatalf("routed harness inherited primary model %q", runtimeConfig.Model)
	}
	if runtimeConfig.Effort != "" {
		t.Fatalf("routed harness inherited primary effort %q", runtimeConfig.Effort)
	}
	if runtimeConfig.ServiceMode != "" {
		t.Fatalf("routed harness inherited primary service mode %q", runtimeConfig.ServiceMode)
	}
}

func TestBuildServiceLiveSandboxPropagatesToRoutedHarnesses(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}

	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Model = ""
	cfg.Harness.ReasoningEffort = ""
	cfg.Harness.ServiceMode = ""
	cfg.Harness.Sandbox = "danger-full-access"
	cfg.Harness.Routing = &config.HarnessRouting{
		Developer: "codex",
		Reviewer:  "claude-code",
	}

	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	if _, err := service.ApplySettings(map[string]string{
		"harness.sandbox": "workspace-write",
	}); err != nil {
		t.Fatal(err)
	}

	primary, ok := service.Harness.(interface{ HarnessConfig() harness.HarnessConfig })
	if !ok {
		t.Fatalf("primary harness type = %T, want *harness.Supervisor", service.Harness)
	}

	roleRuntime := service.Orchestrator.HarnessRouter

	developer, ok := roleRuntime.HarnessForRole(harness.RoleDeveloper).(interface{ HarnessConfig() harness.HarnessConfig })
	if !ok {
		t.Fatalf(
			"developer harness type = %T, want *harness.Supervisor",
			roleRuntime.HarnessForRole(harness.RoleDeveloper),
		)
	}

	reviewer, ok := roleRuntime.HarnessForRole(harness.RoleReviewer).(interface{ HarnessConfig() harness.HarnessConfig })
	if !ok {
		t.Fatalf(
			"reviewer harness type = %T, want *harness.Supervisor",
			roleRuntime.HarnessForRole(harness.RoleReviewer),
		)
	}

	for name, target := range map[string]interface{ HarnessConfig() harness.HarnessConfig }{
		"primary":   primary,
		"developer": developer,
		"reviewer":  reviewer,
	} {
		if got := target.HarnessConfig().Sandbox; got != "workspace-write" {
			t.Fatalf("%s sandbox = %q, want workspace-write", name, got)
		}
	}
}

func TestBuildServiceLivePrimaryChangeReusesPrimaryForMatchingRoute(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}

	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Model = ""
	cfg.Harness.ReasoningEffort = ""
	cfg.Harness.ServiceMode = ""
	cfg.Harness.Routing = &config.HarnessRouting{
		Developer: "codex",
		Reviewer:  "claude-code",
	}

	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	primary := service.Harness

	roleRuntime := service.Orchestrator.HarnessRouter

	before := roleRuntime.HarnessForRole(harness.RoleDeveloper)
	if before == primary {
		t.Fatal("developer unexpectedly used primary before harness.name changed")
	}

	if _, err := service.ApplySettings(map[string]string{
		"harness.name": "codex",
	}); err != nil {
		t.Fatal(err)
	}

	after := roleRuntime.HarnessForRole(harness.RoleDeveloper)
	if targetHarnessConfig(after).Name != targetHarnessConfig(primary).Name {
		t.Fatalf(
			"developer route still uses stale additional harness %T after primary changed to codex",
			after,
		)
	}
}

func TestBuildServiceLiveACPArgsPropagateToRoutedACP(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Model = ""
	cfg.Harness.ReasoningEffort = ""
	cfg.Harness.ServiceMode = ""
	cfg.Harness.ACPCommand = executable
	cfg.Harness.ACPArgs = []string{"before"}
	cfg.Harness.Routing = &config.HarnessRouting{
		Developer: "acp",
	}

	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	roleRuntime := service.Orchestrator.HarnessRouter

	before, ok := roleRuntime.HarnessForRole(
		harness.RoleDeveloper,
	).(interface{ HarnessConfig() harness.HarnessConfig })
	if !ok {
		t.Fatalf(
			"developer harness type = %T, want *harness.Supervisor",
			roleRuntime.HarnessForRole(harness.RoleDeveloper),
		)
	}

	if got := before.HarnessConfig().Args; !reflect.DeepEqual(got, []string{"before"}) {
		t.Fatalf("initial routed ACP args = %#v, want [before]", got)
	}

	if _, err := service.ApplySettings(map[string]string{
		"harness.acp_args": "after",
	}); err != nil {
		t.Fatal(err)
	}

	after, ok := roleRuntime.HarnessForRole(
		harness.RoleDeveloper,
	).(interface{ HarnessConfig() harness.HarnessConfig })
	if !ok {
		t.Fatalf(
			"developer harness type after change = %T, want *harness.Supervisor",
			roleRuntime.HarnessForRole(harness.RoleDeveloper),
		)
	}

	if got := after.HarnessConfig().Args; !reflect.DeepEqual(got, []string{"after"}) {
		t.Fatalf(
			"routed ACP args after live update = %#v, want [after]",
			got,
		)
	}
}

// normalizeHarnessSpecForComparison removes the per-call stderr writer, a
// non-semantic composition field, so golden comparisons can use DeepEqual.
func normalizeHarnessSpecForComparison(spec harness.RuntimeSpec) harness.RuntimeSpec {
	for id, cfg := range spec.Providers {
		cfg.Stderr = nil
		spec.Providers[id] = cfg
	}
	return spec
}

// expectedLegacyHarnessCommand reproduces today's command resolution order,
// including the catalog fallback when resolution fails, so golden tests pin
// composition semantics instead of machine installation state.
func expectedLegacyHarnessCommand(name, customCommand string) string {
	command, err := harness.ResolveConfiguredCommand(name, customCommand, nil)
	if err != nil {
		if definition, ok := harness.Lookup(name); ok {
			return definition.Command
		}
	}
	return command
}

// expectedLegacyHarnessConfig is the independent golden expectation for one
// legacy provider composition: primary providers keep the full historical
// inference configuration, routed providers inherit only the global sandbox
// and their per-kind session path, and only the custom ACP kind reuses the
// global command/argument configuration.
func expectedLegacyHarnessConfig(cfg config.Config, name, version string, primary bool) harness.HarnessConfig {
	expected := harness.HarnessConfig{
		Name:           name,
		Cwd:            cfg.Root,
		ApprovalPolicy: "never",
		Sandbox:        cfg.Harness.Sandbox,
		Network:        false,
		SessionsFile:   cfg.HarnessSessionsPath(name),
		Version:        version,
	}
	if primary {
		expected.Model = cfg.Harness.Model
		expected.Effort = cfg.Harness.ReasoningEffort
		expected.LegacyEffort = cfg.Harness.UsesLegacyReasoningEffort()
		expected.ServiceMode = cfg.Harness.ServiceMode
		expected.Command = expectedLegacyHarnessCommand(name, cfg.Harness.ACPCommand)
		expected.Args = harness.CommandArgs(name, cfg.Harness.ACPArgs)
		return expected
	}
	if name == "acp" {
		expected.Command = expectedLegacyHarnessCommand(name, cfg.Harness.ACPCommand)
		expected.Args = harness.CommandArgs(name, cfg.Harness.ACPArgs)
		return expected
	}
	expected.Command = expectedLegacyHarnessCommand(name, "")
	expected.Args = harness.CommandArgs(name, nil)
	return expected
}

func durableOwnerCLIConfig(t *testing.T) config.Config {
	t.Helper()
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Providers = map[string]config.ProviderProfile{
		"codex-dev": {Harness: "codex", Model: "OWNER"},
		"glm-dev":   {Harness: "codex", Model: "CURRENT"},
	}
	cfg.Harness.Routing = &config.HarnessRouting{Developer: "glm-dev"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func writeDurableOwnerLease(t *testing.T, cfg config.Config, id string, owner harness.ProviderID) (string, []byte) {
	t.Helper()
	lease := orchestrator.Lease{ID: id, Provider: owner, Route: "tasks", File: "task.md", SessionKey: id, State: "processing"}
	data, err := json.MarshalIndent(lease, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	directory := cfg.StatePath("runtime", "leases")
	if err = os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, id+".json")
	if err = os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, data
}

func serviceProviderRuntime(t *testing.T, service *app.Service) *harness.Runtime {
	t.Helper()
	runtime, ok := service.Orchestrator.HarnessRouter.(*harness.Runtime)
	if !ok {
		t.Fatalf("orchestrator router = %T, want *harness.Runtime", service.Orchestrator.HarnessRouter)
	}
	return runtime
}

func assertOwnedProviderPresentButUnstarted(t *testing.T, runtime *harness.Runtime, owner harness.ProviderID) {
	t.Helper()
	release, err := harness.ReserveOwnedExecution(runtime.AcquireRole(harness.RoleDeveloper), "retained:"+string(owner), owner)
	if release != nil {
		release()
	}
	if errors.Is(err, harness.ErrProviderAbsent) || !errors.Is(err, harness.ErrProviderUnavailable) {
		t.Fatalf("owned reservation for %q = %v, want present but unavailable", owner, err)
	}
}

func TestBuildServiceRetainsDurableProviderOwnerAtStartup(t *testing.T) {
	cfg := durableOwnerCLIConfig(t)
	writeDurableOwnerLease(t, cfg, "startup-owner", "codex-dev")
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	owners, err := orchestrator.DurableProviderOwners(cfg)
	if err != nil {
		t.Fatal(err)
	}
	spec := harnessRuntimeSpec(cfg, "test", service.Runtime, owners...)
	if _, ok := spec.Providers["codex-dev"]; !ok {
		t.Fatalf("startup spec omitted durable owner: %#v", spec.Providers)
	}
	if _, ok := spec.Providers["glm-dev"]; !ok {
		t.Fatalf("startup spec omitted current role provider: %#v", spec.Providers)
	}
	if spec.Providers["codex-dev"].Name != "codex" || spec.Providers["glm-dev"].Name != "codex" {
		t.Fatalf("same-kind providers collapsed: %#v", spec.Providers)
	}
	if got := spec.Roles[harness.RoleDeveloper]; got != "glm-dev" {
		t.Fatalf("developer role = %q, want glm-dev", got)
	}
	for role, provider := range spec.Roles {
		if provider == "codex-dev" {
			t.Fatalf("retained owner gained synthetic role %q", role)
		}
	}
	runtime := serviceProviderRuntime(t, service)
	if got := harness.ExecutionProvider(runtime.AcquireRole(harness.RoleDeveloper), "role-probe"); got != "glm-dev" {
		t.Fatalf("published developer role = %q", got)
	}
	assertOwnedProviderPresentButUnstarted(t, runtime, "codex-dev")
}

func TestReconfigureProvidersUsesCurrentDurableOwnersAndReleasesLastOwner(t *testing.T) {
	cfg := durableOwnerCLIConfig(t)
	cfg.Harness.Routing.Developer = "codex-dev"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	runtime := serviceProviderRuntime(t, service)

	leasePath, _ := writeDurableOwnerLease(t, cfg, "live-owner", "codex-dev")
	next := cfg
	routing := *cfg.Harness.Routing
	routing.Developer = "glm-dev"
	next.Harness.Routing = &routing
	if err = service.ReconfigureProviders(next); err != nil {
		t.Fatal(err)
	}
	if got := harness.ExecutionProvider(runtime.AcquireRole(harness.RoleDeveloper), "remapped-role"); got != "glm-dev" {
		t.Fatalf("remapped developer role = %q", got)
	}
	assertOwnedProviderPresentButUnstarted(t, runtime, "codex-dev")

	if err = os.Remove(leasePath); err != nil {
		t.Fatal(err)
	}
	if err = service.ReconfigureProviders(next); err != nil {
		t.Fatal(err)
	}
	if release, reserveErr := harness.ReserveOwnedExecution(runtime.AcquireRole(harness.RoleDeveloper), "retired-owner", "codex-dev"); release != nil || !errors.Is(reserveErr, harness.ErrProviderAbsent) {
		if release != nil {
			release()
		}
		t.Fatalf("reservation after final lease removal = %v, want ErrProviderAbsent", reserveErr)
	}
}

func TestBuildServiceDoesNotSynthesizeUnresolvableDurableOwner(t *testing.T) {
	cfg := durableOwnerCLIConfig(t)
	delete(cfg.Harness.Providers, "codex-dev")
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	leasePath, before := writeDurableOwnerLease(t, cfg, "missing-owner", "codex-dev")
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	runtime := serviceProviderRuntime(t, service)
	if got := harness.ExecutionProvider(runtime.AcquireRole(harness.RoleDeveloper), "missing-role-probe"); got != "glm-dev" {
		t.Fatalf("developer role = %q, want glm-dev", got)
	}
	if release, reserveErr := harness.ReserveOwnedExecution(runtime.AcquireRole(harness.RoleDeveloper), "missing-owner", "codex-dev"); release != nil || !errors.Is(reserveErr, harness.ErrProviderAbsent) {
		if release != nil {
			release()
		}
		t.Fatalf("unresolvable owner reservation = %v, want ErrProviderAbsent", reserveErr)
	}
	after, err := os.ReadFile(leasePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("unresolvable owner composition mutated durable lease")
	}
}

func TestBuildServiceSkipsMalformedDurableOwnerLease(t *testing.T) {
	cfg := durableOwnerCLIConfig(t)
	writeDurableOwnerLease(t, cfg, "valid-owner", "codex-dev")
	corrupt := []byte("{not-json\n")
	corruptPath := filepath.Join(cfg.StatePath("runtime", "leases"), "corrupt.json")
	if err := os.WriteFile(corruptPath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	runtime := serviceProviderRuntime(t, service)
	assertOwnedProviderPresentButUnstarted(t, runtime, "codex-dev")
	if data, readErr := os.ReadFile(corruptPath); readErr != nil || !bytes.Equal(data, corrupt) {
		t.Fatalf("malformed lease changed: err = %v", readErr)
	}
}

func TestBuildServiceOmitsLegacyACPOwnerWithoutCommand(t *testing.T) {
	cfg := durableOwnerCLIConfig(t)
	if _, ok := cfg.Harness.Providers["acp"]; ok {
		t.Fatal("test scenario requires no named acp provider profile")
	}
	if strings.TrimSpace(cfg.Harness.ACPCommand) != "" {
		t.Fatal("test scenario requires an empty global acp_command")
	}
	leasePath, before := writeDurableOwnerLease(t, cfg, "acp-owner", "acp")
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	runtime := serviceProviderRuntime(t, service)
	owners, err := orchestrator.DurableProviderOwners(cfg)
	if err != nil {
		t.Fatal(err)
	}
	spec := harnessRuntimeSpec(cfg, "test", service.Runtime, owners...)
	if _, ok := spec.Providers["acp"]; ok {
		t.Fatalf("legacy acp owner without command retained: %#v", spec.Providers)
	}
	for role, provider := range spec.Roles {
		if provider == "acp" {
			t.Fatalf("role %q points to unresolvable acp owner", role)
		}
	}
	if release, reserveErr := harness.ReserveOwnedExecution(runtime.AcquireRole(harness.RoleDeveloper), "acp-owner", "acp"); release != nil || !errors.Is(reserveErr, harness.ErrProviderAbsent) {
		if release != nil {
			release()
		}
		t.Fatalf("legacy acp owner reservation = %v, want ErrProviderAbsent", reserveErr)
	}
	if err = service.ReconfigureProviders(cfg); err != nil {
		t.Fatalf("reconfigure with legacy acp owner failed: %v", err)
	}
	for _, role := range []harness.Role{harness.RoleChat, harness.RoleDeveloper, harness.RoleReviewer, harness.RoleNotification, harness.RoleHeartbeat} {
		if got := harness.ExecutionProvider(runtime.AcquireRole(role), "acp-role-probe"); got == "acp" {
			t.Fatalf("published role %q routes to unresolvable acp owner", role)
		}
	}
	if release, reserveErr := harness.ReserveOwnedExecution(runtime.AcquireRole(harness.RoleDeveloper), "acp-owner-reconciled", "acp"); release != nil || !errors.Is(reserveErr, harness.ErrProviderAbsent) {
		if release != nil {
			release()
		}
		t.Fatalf("legacy acp owner reservation after reconfigure = %v, want ErrProviderAbsent", reserveErr)
	}
	after, err := os.ReadFile(leasePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("unresolvable acp composition mutated durable lease")
	}
}

func TestBuildServiceOmitsRetainedACPOwnerWithUnresolvableCommand(t *testing.T) {
	cfg := durableOwnerCLIConfig(t)
	cfg.Harness.Providers["gone-acp"] = config.ProviderProfile{Harness: "acp", ACPCommand: "definitely-not-installed-xyz"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	leasePath, before := writeDurableOwnerLease(t, cfg, "gone-acp-owner", "gone-acp")
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	runtime := serviceProviderRuntime(t, service)
	owners, err := orchestrator.DurableProviderOwners(cfg)
	if err != nil {
		t.Fatal(err)
	}
	spec := harnessRuntimeSpec(cfg, "test", service.Runtime, owners...)
	if _, ok := spec.Providers["gone-acp"]; ok {
		t.Fatalf("retained acp owner with unresolvable command retained: %#v", spec.Providers)
	}
	for role, provider := range spec.Roles {
		if provider == "gone-acp" {
			t.Fatalf("role %q points to unresolvable acp owner", role)
		}
	}
	if release, reserveErr := harness.ReserveOwnedExecution(runtime.AcquireRole(harness.RoleDeveloper), "gone-acp-owner", "gone-acp"); release != nil || !errors.Is(reserveErr, harness.ErrProviderAbsent) {
		if release != nil {
			release()
		}
		t.Fatalf("unresolvable acp owner reservation = %v, want ErrProviderAbsent", reserveErr)
	}
	if err = service.ReconfigureProviders(cfg); err != nil {
		t.Fatalf("reconfigure with unresolvable acp owner failed: %v", err)
	}
	for _, role := range []harness.Role{harness.RoleChat, harness.RoleDeveloper, harness.RoleReviewer, harness.RoleNotification, harness.RoleHeartbeat} {
		if got := harness.ExecutionProvider(runtime.AcquireRole(role), "gone-acp-role-probe"); got == "gone-acp" {
			t.Fatalf("published role %q routes to unresolvable acp owner", role)
		}
	}
	if release, reserveErr := harness.ReserveOwnedExecution(runtime.AcquireRole(harness.RoleDeveloper), "gone-acp-owner-reconciled", "gone-acp"); release != nil || !errors.Is(reserveErr, harness.ErrProviderAbsent) {
		if release != nil {
			release()
		}
		t.Fatalf("unresolvable acp owner reservation after reconfigure = %v, want ErrProviderAbsent", reserveErr)
	}
	after, err := os.ReadFile(leasePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("unresolvable acp composition mutated durable lease")
	}
}

func TestHarnessRuntimeSpecOmitsLegacyACPOwnerWithUnresolvableCommand(t *testing.T) {
	cfg := durableOwnerCLIConfig(t)
	cfg.Harness.ACPCommand = "definitely-not-installed-xyz"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "legacy-acp-command-spec")
	defer runtimeState.Close()
	spec := harnessRuntimeSpec(cfg, "test", runtimeState, "acp")
	if _, ok := spec.Providers["acp"]; ok {
		t.Fatalf("legacy acp owner with unresolvable command retained: %#v", spec.Providers)
	}
	registry := harness.NewBuiltinRegistry()
	runtime, err := harness.NewRuntimeSpec(registry, spec)
	if err != nil {
		t.Fatalf("spec with unresolvable legacy acp owner rejected: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBuildServiceRetainsResolvedACPOwner(t *testing.T) {
	cfg := durableOwnerCLIConfig(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Providers["live-acp"] = config.ProviderProfile{Harness: "acp", ACPCommand: executable}
	cfg.Harness.ACPCommand = executable
	if err = cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	writeDurableOwnerLease(t, cfg, "profile-acp-owner", "live-acp")
	writeDurableOwnerLease(t, cfg, "legacy-acp-owner", "acp")
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	owners, err := orchestrator.DurableProviderOwners(cfg)
	if err != nil {
		t.Fatal(err)
	}
	spec := harnessRuntimeSpec(cfg, "test", service.Runtime, owners...)
	for _, owner := range []harness.ProviderID{"live-acp", "acp"} {
		provider, ok := spec.Providers[owner]
		if !ok {
			t.Fatalf("resolved acp owner %q omitted: %#v", owner, spec.Providers)
		}
		if strings.TrimSpace(provider.Command) == "" {
			t.Fatalf("resolved acp owner %q has an empty command", owner)
		}
	}
	for role, provider := range spec.Roles {
		if provider == "live-acp" || provider == "acp" {
			t.Fatalf("retained acp owner gained synthetic role %q", role)
		}
	}
	runtime := serviceProviderRuntime(t, service)
	assertOwnedProviderPresentButUnstarted(t, runtime, "live-acp")
	assertOwnedProviderPresentButUnstarted(t, runtime, "acp")
}

func TestBuildServiceRetainsValidLegacyCatalogKindOwner(t *testing.T) {
	cfg := durableOwnerCLIConfig(t)
	if _, ok := cfg.Harness.Providers["codex"]; ok {
		t.Fatal("test scenario requires no named codex provider profile")
	}
	writeDurableOwnerLease(t, cfg, "legacy-owner", "codex")
	service, err := buildService(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	owners, err := orchestrator.DurableProviderOwners(cfg)
	if err != nil {
		t.Fatal(err)
	}
	spec := normalizeHarnessSpecForComparison(harnessRuntimeSpec(cfg, "test", service.Runtime, owners...))
	retained, ok := spec.Providers[harness.ProviderID("codex")]
	if !ok {
		t.Fatalf("legacy catalog-kind owner omitted: %#v", spec.Providers)
	}
	expected := expectedLegacyHarnessConfig(cfg, "codex", "test", false)
	if !reflect.DeepEqual(retained, expected) {
		t.Fatalf("legacy owner composition = %#v, want %#v", retained, expected)
	}
	if got := spec.Roles[harness.RoleDeveloper]; got != "glm-dev" {
		t.Fatalf("developer role = %q, want glm-dev", got)
	}
	for role, provider := range spec.Roles {
		if provider == "codex" {
			t.Fatalf("retained legacy owner gained synthetic role %q", role)
		}
	}
	runtime := serviceProviderRuntime(t, service)
	if got := harness.ExecutionProvider(runtime.AcquireRole(harness.RoleDeveloper), "legacy-role-probe"); got != "glm-dev" {
		t.Fatalf("published developer role = %q, want glm-dev", got)
	}
	assertOwnedProviderPresentButUnstarted(t, runtime, "codex")
}

func TestHarnessRuntimeSpecLegacySingle(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "codex"
	cfg.Harness.Model = "gpt-legacy-test"
	cfg.Harness.ReasoningEffort = "high"
	cfg.Harness.ServiceMode = "priority"
	cfg.Harness.Sandbox = "workspace-write"

	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "legacy-single-spec")
	defer runtimeState.Close()

	spec := harnessRuntimeSpec(cfg, "test", runtimeState)

	if len(spec.Providers) != 1 {
		t.Fatalf("legacy single spec has %d providers, want 1", len(spec.Providers))
	}
	primary, ok := spec.Providers[harness.ProviderID("codex")]
	if !ok {
		t.Fatal("legacy single spec is missing the codex provider")
	}
	if primary.Name != "codex" {
		t.Fatalf("primary harness name = %q, want codex", primary.Name)
	}
	if primary.Model != "gpt-legacy-test" || primary.Effort != "high" || primary.ServiceMode != "priority" {
		t.Fatalf("primary inference settings = %+v", primary)
	}
	if primary.LegacyEffort != cfg.Harness.UsesLegacyReasoningEffort() {
		t.Fatalf("primary legacy effort = %t", primary.LegacyEffort)
	}
	if primary.Sandbox != "workspace-write" {
		t.Fatalf("primary sandbox = %q, want preserved value", primary.Sandbox)
	}
	if primary.SessionsFile != cfg.HarnessSessionsPath("codex") {
		t.Fatalf("primary sessions file = %q", primary.SessionsFile)
	}

	expected := harness.RuntimeSpec{
		Providers: map[harness.ProviderID]harness.HarnessConfig{
			"codex": expectedLegacyHarnessConfig(cfg, "codex", "test", true),
		},
		Roles: map[harness.Role]harness.ProviderID{
			harness.RoleChat:         "codex",
			harness.RoleDeveloper:    "codex",
			harness.RoleReviewer:     "codex",
			harness.RoleNotification: "codex",
			harness.RoleHeartbeat:    "codex",
		},
	}
	if got := normalizeHarnessSpecForComparison(spec); !reflect.DeepEqual(got, expected) {
		t.Fatalf("legacy single spec mismatch:\ngot  %#v\nwant %#v", got, expected)
	}
}

func TestHarnessRuntimeSpecLegacyRouting(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Model = "primary-model"
	cfg.Harness.ReasoningEffort = "high"
	cfg.Harness.ServiceMode = "priority"
	cfg.Harness.Sandbox = "read-only"
	cfg.Harness.Routing = &config.HarnessRouting{
		Developer: "codex",
		Reviewer:  "claude-code",
	}

	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "legacy-routing-spec")
	defer runtimeState.Close()

	spec := harnessRuntimeSpec(cfg, "test", runtimeState)

	expectedRoles := map[harness.Role]harness.ProviderID{
		harness.RoleChat:         "agent-zero",
		harness.RoleDeveloper:    "codex",
		harness.RoleReviewer:     "claude-code",
		harness.RoleNotification: "agent-zero",
		harness.RoleHeartbeat:    "agent-zero",
	}
	if !reflect.DeepEqual(spec.Roles, expectedRoles) {
		t.Fatalf("routing roles = %#v, want %#v", spec.Roles, expectedRoles)
	}
	if len(spec.Providers) != 3 {
		t.Fatalf("routed spec has %d providers, want 3", len(spec.Providers))
	}
	primary := spec.Providers["agent-zero"]
	if primary.Name != "agent-zero" || primary.Model != "primary-model" || primary.Effort != "high" || primary.ServiceMode != "priority" {
		t.Fatalf("primary composition = %+v", primary)
	}
	for name, sessions := range map[string]string{
		"codex":       cfg.HarnessSessionsPath("codex"),
		"claude-code": cfg.HarnessSessionsPath("claude-code"),
	} {
		routed, ok := spec.Providers[harness.ProviderID(name)]
		if !ok {
			t.Fatalf("routed spec is missing %s", name)
		}
		if routed.Model != "" || routed.Effort != "" || routed.ServiceMode != "" || routed.LegacyEffort {
			t.Fatalf("routed %s inherited primary inference settings: %+v", name, routed)
		}
		if routed.Sandbox != "read-only" {
			t.Fatalf("routed %s sandbox = %q, want the global value", name, routed.Sandbox)
		}
		if routed.SessionsFile != sessions {
			t.Fatalf("routed %s sessions file = %q", name, routed.SessionsFile)
		}
	}

	expected := harness.RuntimeSpec{
		Providers: map[harness.ProviderID]harness.HarnessConfig{
			"agent-zero":  expectedLegacyHarnessConfig(cfg, "agent-zero", "test", true),
			"codex":       expectedLegacyHarnessConfig(cfg, "codex", "test", false),
			"claude-code": expectedLegacyHarnessConfig(cfg, "claude-code", "test", false),
		},
		Roles: expectedRoles,
	}
	if got := normalizeHarnessSpecForComparison(spec); !reflect.DeepEqual(got, expected) {
		t.Fatalf("routing spec mismatch:\ngot  %#v\nwant %#v", got, expected)
	}
}

func TestHarnessRuntimeSpecLegacyRoutedACP(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "codex"
	cfg.Harness.Model = "primary-model"
	cfg.Harness.Sandbox = "workspace-write"
	cfg.Harness.ACPCommand = executable
	cfg.Harness.ACPArgs = []string{"--flag", "value with spaces"}
	cfg.Harness.Routing = &config.HarnessRouting{
		Developer: "acp",
	}

	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "legacy-routed-acp-spec")
	defer runtimeState.Close()

	spec := harnessRuntimeSpec(cfg, "test", runtimeState)

	if len(spec.Providers) != 2 {
		t.Fatalf("routed ACP spec has %d providers, want 2", len(spec.Providers))
	}
	routed, ok := spec.Providers[harness.ProviderID("acp")]
	if !ok {
		t.Fatal("routed ACP spec is missing the acp provider")
	}
	if routed.Command != expectedLegacyHarnessCommand("acp", executable) {
		t.Fatalf("routed ACP command = %q, want the global configured command", routed.Command)
	}
	if !reflect.DeepEqual(routed.Args, []string{"--flag", "value with spaces"}) {
		t.Fatalf("routed ACP args = %#v, want the global configured args", routed.Args)
	}
	if routed.Model != "" || routed.Effort != "" || routed.ServiceMode != "" {
		t.Fatalf("routed ACP inherited primary inference settings: %+v", routed)
	}
	if routed.Sandbox != "workspace-write" {
		t.Fatalf("routed ACP sandbox = %q", routed.Sandbox)
	}
	if routed.SessionsFile != cfg.HarnessSessionsPath("acp") {
		t.Fatalf("routed ACP sessions file = %q", routed.SessionsFile)
	}

	expected := harness.RuntimeSpec{
		Providers: map[harness.ProviderID]harness.HarnessConfig{
			"codex": expectedLegacyHarnessConfig(cfg, "codex", "test", true),
			"acp":   expectedLegacyHarnessConfig(cfg, "acp", "test", false),
		},
		Roles: map[harness.Role]harness.ProviderID{
			harness.RoleChat:         "codex",
			harness.RoleDeveloper:    "acp",
			harness.RoleReviewer:     "codex",
			harness.RoleNotification: "codex",
			harness.RoleHeartbeat:    "codex",
		},
	}
	if got := normalizeHarnessSpecForComparison(spec); !reflect.DeepEqual(got, expected) {
		t.Fatalf("routed ACP spec mismatch:\ngot  %#v\nwant %#v", got, expected)
	}
}

func TestHarnessRuntimeSpecNoHarnessSentinel(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = ""

	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "no-harness-sentinel-spec")
	defer runtimeState.Close()

	spec := harnessRuntimeSpec(cfg, "test", runtimeState)

	if len(spec.Providers) != 1 {
		t.Fatalf("no-harness sentinel spec has %d providers, want 1", len(spec.Providers))
	}
	if _, ok := spec.Providers[harness.ProviderID("")]; !ok {
		t.Fatal("no-harness sentinel spec lost the empty provider identity")
	}
	expectedRoles := map[harness.Role]harness.ProviderID{
		harness.RoleChat:         "",
		harness.RoleDeveloper:    "",
		harness.RoleReviewer:     "",
		harness.RoleNotification: "",
		harness.RoleHeartbeat:    "",
	}
	if !reflect.DeepEqual(spec.Roles, expectedRoles) {
		t.Fatalf("sentinel roles = %#v, want %#v", spec.Roles, expectedRoles)
	}
	expected := harness.RuntimeSpec{
		Providers: map[harness.ProviderID]harness.HarnessConfig{
			"": expectedLegacyHarnessConfig(cfg, "", "test", true),
		},
		Roles: expectedRoles,
	}
	if got := normalizeHarnessSpecForComparison(spec); !reflect.DeepEqual(got, expected) {
		t.Fatalf("no-harness sentinel spec mismatch:\ngot  %#v\nwant %#v", got, expected)
	}

	runtime, err := harness.NewRuntimeSpec(harness.NewBuiltinRegistry(), spec)
	if err != nil {
		t.Fatalf("no-harness sentinel rejected by NewRuntimeSpec: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHarnessRuntimeSpecNamedInstances(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Sandbox = "danger-full-access"
	cfg.Harness.Routing = &config.HarnessRouting{
		Developer:    "codex-dev",
		Reviewer:     "claude-rev",
		Notification: "claude-arch",
	}
	cfg.Harness.Providers = map[string]config.ProviderProfile{
		"codex-dev":   {Harness: "codex", Model: "DEV-MODEL", ReasoningEffort: "high", ServiceMode: "priority", Sandbox: "workspace-write"},
		"claude-rev":  {Harness: "claude-code", Model: "REV-MODEL", ReasoningEffort: "high", Sandbox: "read-only"},
		"claude-arch": {Harness: "claude-code", Model: "ARCH-MODEL", Sandbox: ""},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("named instance configuration rejected: %v", err)
	}

	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "named-instances-spec")
	defer runtimeState.Close()

	spec := harnessRuntimeSpec(cfg, "test", runtimeState)

	expectedRoles := map[harness.Role]harness.ProviderID{
		harness.RoleChat:         "agent-zero",
		harness.RoleDeveloper:    "codex-dev",
		harness.RoleReviewer:     "claude-rev",
		harness.RoleNotification: "claude-arch",
		harness.RoleHeartbeat:    "agent-zero",
	}
	if !reflect.DeepEqual(spec.Roles, expectedRoles) {
		t.Fatalf("roles = %#v, want %#v", spec.Roles, expectedRoles)
	}
	if len(spec.Providers) != 4 {
		t.Fatalf("providers = %#v, want exactly the referenced instances plus primary", spec.Providers)
	}

	// Instance IDs differ from their harness kinds, and the two same-kind
	// claude instances stay independent instead of collapsing into one
	// identity reconstructed from HarnessConfig.Name.
	revConfig, revOK := spec.Providers["claude-rev"]
	archConfig, archOK := spec.Providers["claude-arch"]
	if !revOK || !archOK {
		t.Fatalf("claude instances missing: %#v", spec.Providers)
	}
	if revConfig.Name != "claude-code" || archConfig.Name != "claude-code" {
		t.Fatalf("claude kinds = %q / %q", revConfig.Name, archConfig.Name)
	}
	if revConfig.Model != "REV-MODEL" || archConfig.Model != "ARCH-MODEL" {
		t.Fatalf("same-kind models collided: %q / %q", revConfig.Model, archConfig.Model)
	}
	if revConfig.Effort != "high" || revConfig.LegacyEffort {
		t.Fatalf("claude-rev effort = %q (legacy %t)", revConfig.Effort, revConfig.LegacyEffort)
	}
	if archConfig.Effort != "" || archConfig.ServiceMode != "" {
		t.Fatalf("claude-arch inherited settings it did not declare: %+v", archConfig)
	}

	devConfig, devOK := spec.Providers["codex-dev"]
	if !devOK {
		t.Fatalf("codex-dev missing: %#v", spec.Providers)
	}
	if devConfig.Name != "codex" || devConfig.Model != "DEV-MODEL" || devConfig.Effort != "high" || devConfig.ServiceMode != "priority" {
		t.Fatalf("codex-dev composition = %+v", devConfig)
	}
	// Explicit profile sandboxes override the global value; an empty profile
	// sandbox inherits it.
	if devConfig.Sandbox != "workspace-write" || revConfig.Sandbox != "read-only" {
		t.Fatalf("explicit profile sandboxes = %q / %q", devConfig.Sandbox, revConfig.Sandbox)
	}
	if archConfig.Sandbox != "danger-full-access" {
		t.Fatalf("empty profile sandbox did not inherit the global value: %q", archConfig.Sandbox)
	}

	// The primary keeps its legacy composition, and no named profile inherits
	// primary inference settings.
	primaryConfig := spec.Providers["agent-zero"]
	if primaryConfig.Name != "agent-zero" || primaryConfig.Model != "" || primaryConfig.SessionsFile != cfg.HarnessSessionsPath("agent-zero") {
		t.Fatalf("primary composition = %+v", primaryConfig)
	}

	// Named profiles use distinct instance-scoped session paths while the
	// primary retains the legacy shared file.
	codexDevProfile := cfg.Harness.Providers["codex-dev"]
	if devConfig.SessionsFile != cfg.ProviderSessionsPath(config.ProviderRef{ID: "codex-dev", Kind: "codex", Profile: &codexDevProfile}) {
		t.Fatalf("codex-dev sessions file = %q", devConfig.SessionsFile)
	}
	if revConfig.SessionsFile != filepath.Join(root, ".spynel", "runtime", "providers", "claude-rev", "harness-claude-code-sessions.json") {
		t.Fatalf("claude-rev sessions file = %q", revConfig.SessionsFile)
	}
	if archConfig.SessionsFile != filepath.Join(root, ".spynel", "runtime", "providers", "claude-arch", "harness-claude-code-sessions.json") {
		t.Fatalf("claude-arch sessions file = %q", archConfig.SessionsFile)
	}
	if revConfig.SessionsFile == archConfig.SessionsFile || filepath.Dir(revConfig.SessionsFile) == filepath.Dir(archConfig.SessionsFile) {
		t.Fatal("same-kind profiles share a session directory")
	}

	// Rebuilding after a global sandbox change updates only profiles that
	// inherit the global sandbox.
	cfg.Harness.Sandbox = "workspace-write"
	next := harnessRuntimeSpec(cfg, "test", runtimeState)
	if got := next.Providers["claude-arch"].Sandbox; got != "workspace-write" {
		t.Fatalf("inheriting profile sandbox after global change = %q", got)
	}
	if got := next.Providers["claude-rev"].Sandbox; got != "read-only" {
		t.Fatalf("explicit profile sandbox after global change = %q", got)
	}

	runtime, err := harness.NewRuntimeSpec(harness.NewBuiltinRegistry(), spec)
	if err != nil {
		t.Fatalf("named instance spec rejected by NewRuntimeSpec: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHarnessRuntimeSpecProfileUnusedNotComposed(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Providers = map[string]config.ProviderProfile{
		"glm-dev": {Harness: "opencode", Model: "UNUSED-MODEL"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unused profile configuration rejected: %v", err)
	}

	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "unused-profile-spec")
	defer runtimeState.Close()

	spec := harnessRuntimeSpec(cfg, "test", runtimeState)

	if len(spec.Providers) != 1 {
		t.Fatalf("unused profile composed into RuntimeSpec: %#v", spec.Providers)
	}
	if _, ok := spec.Providers["glm-dev"]; ok {
		t.Fatal("unused profile instance composed into RuntimeSpec")
	}
	for _, id := range spec.Roles {
		if id == "glm-dev" {
			t.Fatal("unused profile referenced by a role")
		}
	}
	if _, ok := spec.Providers["opencode"]; ok {
		t.Fatal("unused profile kind composed into RuntimeSpec")
	}
}

func TestHarnessRuntimeSpecProfileACP(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "codex"
	// The global legacy ACP settings exist but must not be consumed by named
	// ACP profiles; a missing global command proves the profiles resolve their
	// own command instead of falling back to the global value.
	cfg.Harness.ACPCommand = "missing-global-agent"
	cfg.Harness.ACPArgs = []string{"global-arg"}
	cfg.Harness.Providers = map[string]config.ProviderProfile{
		"acp-one": {Harness: "acp", ACPCommand: executable, ACPArgs: []string{"--one", "alpha"}},
		"acp-two": {Harness: "acp", ACPCommand: executable, ACPArgs: []string{"--two", "beta"}},
	}
	cfg.Harness.Routing = &config.HarnessRouting{
		Developer: "acp-one",
		Reviewer:  "acp-two",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("named ACP profile configuration rejected: %v", err)
	}

	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "profile-acp-spec")
	defer runtimeState.Close()

	spec := harnessRuntimeSpec(cfg, "test", runtimeState)

	one, oneOK := spec.Providers["acp-one"]
	two, twoOK := spec.Providers["acp-two"]
	if !oneOK || !twoOK {
		t.Fatalf("named ACP instances missing: %#v", spec.Providers)
	}
	for name, entry := range map[string]harness.HarnessConfig{"acp-one": one, "acp-two": two} {
		if entry.Name != "acp" {
			t.Fatalf("%s kind = %q", name, entry.Name)
		}
		if entry.Command != expectedLegacyHarnessCommand("acp", executable) {
			t.Fatalf("%s consumed the global command: %q", name, entry.Command)
		}
		if strings.Contains(strings.Join(entry.Args, " "), "global-arg") {
			t.Fatalf("%s consumed the global args: %#v", name, entry.Args)
		}
	}
	if !reflect.DeepEqual(one.Args, []string{"--one", "alpha"}) || !reflect.DeepEqual(two.Args, []string{"--two", "beta"}) {
		t.Fatalf("named ACP args = %#v / %#v", one.Args, two.Args)
	}
	if one.SessionsFile == two.SessionsFile || filepath.Dir(one.SessionsFile) == filepath.Dir(two.SessionsFile) {
		t.Fatalf("named ACP instances share a session directory: %q / %q", one.SessionsFile, two.SessionsFile)
	}

	// The profile argument lists are copied, not aliased into the spec.
	one.Args[0] = "mutated"
	if cfg.Harness.Providers["acp-one"].ACPArgs[0] != "--one" {
		t.Fatal("profile ACP arguments were mutated during composition")
	}
}

func TestHarnessRuntimeSpecProfileSharedByRoles(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Providers = map[string]config.ProviderProfile{
		"glm-dev": {Harness: "codex", Model: "SHARED-MODEL"},
	}
	cfg.Harness.Routing = &config.HarnessRouting{
		Developer: "glm-dev",
		Reviewer:  "glm-dev",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("shared profile routing rejected: %v", err)
	}

	runtimeState := app.NewRuntimeAt(cfg.StatePath("runtime", "logs"), "shared-profile-spec")
	defer runtimeState.Close()

	spec := harnessRuntimeSpec(cfg, "test", runtimeState)

	if len(spec.Providers) != 2 {
		t.Fatalf("shared profile duplicated: %#v", spec.Providers)
	}
	if got := spec.Roles[harness.RoleDeveloper]; got != "glm-dev" || spec.Roles[harness.RoleReviewer] != "glm-dev" {
		t.Fatalf("roles = %q / %q, want glm-dev for both", spec.Roles[harness.RoleDeveloper], spec.Roles[harness.RoleReviewer])
	}
	if entry := spec.Providers["glm-dev"]; entry.Name != "codex" || entry.Model != "SHARED-MODEL" {
		t.Fatalf("shared provider composition = %+v", entry)
	}
}

func targetHarnessConfig(target harness.ExecutionTarget) harness.HarnessConfig {
	return target.(interface{ HarnessConfig() harness.HarnessConfig }).HarnessConfig()
}
