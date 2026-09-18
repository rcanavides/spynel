package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestFindReportsUninitializedDirectory(t *testing.T) {
	_, err := Find(t.TempDir())
	if !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("Find error = %v, want ErrNotInitialized", err)
	}
}

func TestFindDiscoversCanonicalConfigFromChildDirectory(t *testing.T) {
	root := t.TempDir()
	path := writeTestConfig(t, root, []byte("version: 1\n"))
	child := filepath.Join(root, "nested", "project")
	if err := os.MkdirAll(child, 0o700); err != nil {
		t.Fatal(err)
	}
	found, err := Find(child)
	if err != nil {
		t.Fatal(err)
	}
	if found != path {
		t.Fatalf("Find() = %q, want %q", found, path)
	}
}

func writeTestConfig(t *testing.T, root string, data []byte) string {
	t.Helper()
	path := PathForRoot(root)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultIsValid(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Harness.Name != "" {
		t.Fatalf("default coding harness should await detection, got %q", cfg.Harness.Name)
	}
	if cfg.Harness.Sandbox != "danger-full-access" {
		t.Fatalf("default coding harness should be unrestricted, got %q", cfg.Harness.Sandbox)
	}
	if cfg.Harness.ChatAgentPrefix != "" || cfg.Harness.DeveloperAgentPrefix != "" || cfg.Harness.ReviewerAgentPrefix != "" || cfg.Harness.HeartbeatAgentPrefix != "" || cfg.Harness.Reviews != TaskReviewsSkipTrivial {
		t.Fatalf("unexpected harness agent defaults: %#v", cfg.Harness)
	}
	if !cfg.Speech.Enabled || cfg.Speech.Language != "en" || cfg.Speech.NumThreads != 2 {
		t.Fatalf("unexpected speech defaults: %#v", cfg.Speech)
	}
	if cfg.Channels.TUI.Theme != "spynel" {
		t.Fatalf("unexpected default TUI theme: %#v", cfg.Channels.TUI)
	}
	if cfg.Orchestrator.SemanticHeartbeatMinutes != 15 {
		t.Fatalf("semantic heartbeat default = %d, want 15", cfg.Orchestrator.SemanticHeartbeatMinutes)
	}
	if !cfg.Orchestrator.RetriggerUnrespondedMessages {
		t.Fatal("conversation recovery should default on")
	}
	if cfg.Workspace.CleanupRetentionDays != 30 {
		t.Fatalf("cleanup retention default = %d, want 30", cfg.Workspace.CleanupRetentionDays)
	}
}

func TestHarnessAgentPrefixesAndReviewModeValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		prefix string
		prompt string
		want   string
	}{
		{name: "command", prefix: "/goal", prompt: "Do the work", want: "/goal Do the work"},
		{name: "outer whitespace", prefix: "  /goal   ", prompt: "Do the work", want: "/goal Do the work"},
		{name: "multi-token", prefix: "  /goal keep   this  ", prompt: "Do the work", want: "/goal keep   this Do the work"},
		{name: "empty prefix", prefix: "", prompt: "prompt", want: "prompt"},
		{name: "whitespace-only prefix", prefix: " \t ", prompt: "prompt", want: "prompt"},
		{name: "empty prompt", prefix: " /goal ", prompt: "", want: "/goal "},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := PrependAgentPrefix(test.prefix, test.prompt); got != test.want {
				t.Fatalf("PrependAgentPrefix(%q, %q) = %q, want %q", test.prefix, test.prompt, got, test.want)
			}
		})
	}
	for _, mode := range []string{TaskReviewsSkipTrivial, TaskReviewsAlways, TaskReviewsNever} {
		cfg := Default()
		cfg.Harness.Reviews = mode
		if err := cfg.Validate(); err != nil {
			t.Fatalf("review mode %q: %v", mode, err)
		}
	}
	cfg := Default()
	cfg.Harness.Reviews = "sometimes"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "harness.reviews") {
		t.Fatalf("invalid review mode validation = %v", err)
	}
	cfg = Default()
	cfg.Harness.DeveloperAgentPrefix = "/goal\nunsafe"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "developer_agent_prefix") {
		t.Fatalf("multiline prefix validation = %v", err)
	}
}

func TestMinimalConfigUsesEmptyHarnessAgentPrefixDefaults(t *testing.T) {
	root := t.TempDir()
	path := writeTestConfig(t, root, []byte("version: 1\n"))
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Harness.ChatAgentPrefix != "" || cfg.Harness.DeveloperAgentPrefix != "" || cfg.Harness.ReviewerAgentPrefix != "" || cfg.Harness.HeartbeatAgentPrefix != "" || cfg.Harness.Reviews != TaskReviewsSkipTrivial {
		t.Fatalf("default harness settings were not retained: %#v", cfg.Harness)
	}
}

func TestSemanticHeartbeatValidationSupportsExplicitDisable(t *testing.T) {
	cfg := Default()
	cfg.Orchestrator.SemanticHeartbeatMinutes = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("disabled semantic heartbeat was rejected: %v", err)
	}
	for _, invalid := range []int{-1, 1, 4, 1441} {
		cfg.Orchestrator.SemanticHeartbeatMinutes = invalid
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "semantic_heartbeat_minutes") {
			t.Fatalf("invalid semantic heartbeat %d produced %v", invalid, err)
		}
	}
}

func TestCleanupRetentionValidationRequiresBoundedPositiveWholeDays(t *testing.T) {
	for _, invalid := range []int{-1, 0, 36501} {
		cfg := Default()
		cfg.Workspace.CleanupRetentionDays = invalid
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "cleanup_retention_days") {
			t.Fatalf("invalid cleanup retention %d produced %v", invalid, err)
		}
	}
}

func TestSpeechLanguagesContainEveryParakeetLanguage(t *testing.T) {
	want := []string{"auto", "bg", "hr", "cs", "da", "nl", "en", "et", "fi", "fr", "de", "el", "hu", "it", "lv", "lt", "mt", "pl", "pt", "ro", "sk", "sl", "es", "sv", "ru", "uk"}
	for _, language := range want {
		if !IsSpeechLanguage(language) {
			t.Fatalf("supported language %q is missing", language)
		}
	}
	if IsSpeechLanguage("ja") {
		t.Fatal("unsupported language was accepted")
	}
}

func TestTelegramWhitelistIsRequiredWhenEnabled(t *testing.T) {
	cfg := Default()
	cfg.Channels.Telegram.Enabled = true
	for _, invalid := range [][]string{nil, {}, {"  "}, {"@"}, {"..."}, {"-7"}, {"bad user"}, {strings.Repeat("a", 33)}} {
		cfg.Channels.Telegram.AllowedUsers = invalid
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "allowed_users requires at least one user") {
			t.Fatalf("enabled Telegram accepted invalid whitelist %#v: %v", invalid, err)
		}
	}
	cfg.Channels.Telegram.AllowedUsers = []string{"123456789"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("enabled Telegram rejected a configured whitelist: %v", err)
	}
}

func TestTelegramWebhookRequiresSecretWhenEnabled(t *testing.T) {
	cfg := Default()
	cfg.Channels.Telegram.Enabled = true
	cfg.Channels.Telegram.Mode = "webhook"
	cfg.Channels.Telegram.WebhookURL = "https://public.example"
	cfg.Channels.Telegram.AllowedUsers = []string{"123456789"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "webhook_secret is required") {
		t.Fatalf("enabled Telegram webhook accepted an empty secret: %v", err)
	}
	cfg.Channels.Telegram.WebhookSecret = "verification-secret"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("enabled Telegram webhook rejected a configured secret: %v", err)
	}
}

func TestWhatsAppWhitelistIsRequiredWhenEnabled(t *testing.T) {
	cfg := Default()
	cfg.Channels.WhatsApp.Enabled = true
	for _, invalid := range [][]string{nil, {}, {"  "}, {" + "}, {"phone"}, {"12x34"}, {"1234567890123456"}} {
		cfg.Channels.WhatsApp.AllowedNumbers = invalid
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "allowed_numbers requires at least one number") {
			t.Fatalf("enabled WhatsApp accepted invalid whitelist %#v: %v", invalid, err)
		}
	}
	cfg.Channels.WhatsApp.AllowedNumbers = []string{"+1 (555) 123-4567"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("enabled WhatsApp rejected a configured whitelist: %v", err)
	}
}

func TestNormalizeWhatsAppNumber(t *testing.T) {
	tests := map[string]string{
		"+420 123 456 789":  "420123456789",
		"00420-123-456-789": "420123456789",
		"(420) 123.456.789": "420123456789",
		"0123 456 789":      "0123456789",
		"phone":             "",
	}
	for input, want := range tests {
		if got := NormalizeWhatsAppNumber(input); got != want {
			t.Errorf("NormalizeWhatsAppNumber(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestLoadMergesUserValuesWithDefaults(t *testing.T) {
	root := t.TempDir()
	data := []byte("version: 1\nworkspace:\n  history_char_limit: 321\nharness:\n  name: codex\nchannels:\n  whatsapp:\n    mode: dedicated\n")
	path := writeTestConfig(t, root, data)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Workspace.HistoryCharLimit != 321 || cfg.Workspace.HistoryMaxMessages != 50 {
		t.Fatalf("defaults were not retained: %#v", cfg.Workspace)
	}
	if cfg.Harness.Sandbox != "danger-full-access" {
		t.Fatalf("missing sandbox did not inherit the unrestricted default: %#v", cfg.Harness)
	}
	if cfg.Channels.WhatsApp.Mode != "dedicated" || cfg.Channels.Telegram.PollTimeoutSec != 30 {
		t.Fatalf("nested defaults were not retained: %#v", cfg.Channels)
	}
	if cfg.Channels.TUI.Theme != "spynel" {
		t.Fatalf("missing TUI theme did not inherit the default: %#v", cfg.Channels.TUI)
	}
	if cfg.Resolve("relative") != filepath.Join(root, "relative") {
		t.Fatalf("relative path did not resolve against config root")
	}
}

func TestReasoningEffortOmissionPreservesLegacyMediumAndExplicitInherit(t *testing.T) {
	root := t.TempDir()
	path := writeTestConfig(t, root, []byte("version: 1\nharness:\n  name: codex\n"))
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Harness.ReasoningEffort != "medium" {
		t.Fatalf("omitted reasoning effort = %q, want legacy medium", cfg.Harness.ReasoningEffort)
	}
	if !cfg.Harness.UsesLegacyReasoningEffort() {
		t.Fatal("omitted reasoning effort lost its legacy provenance")
	}
	if err := Save(cfg); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(saved), "reasoning_effort:") {
		t.Fatalf("legacy omission was materialized during unrelated save:\n%s", saved)
	}
	cfg, err = Load(path)
	if err != nil || cfg.Harness.ReasoningEffort != "medium" {
		t.Fatalf("saved omitted reasoning effort = %q, %v", cfg.Harness.ReasoningEffort, err)
	}

	path = writeTestConfig(t, root, []byte("version: 1\nharness:\n  name: codex\n  reasoning_effort: inherit\n"))
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Harness.ReasoningEffort != "" {
		t.Fatalf("explicit inherited reasoning effort = %q, want empty provider default", cfg.Harness.ReasoningEffort)
	}
	if cfg.Harness.UsesLegacyReasoningEffort() {
		t.Fatal("explicit inherit was marked as a legacy omitted effort")
	}
	if err := Save(cfg); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil || cfg.Harness.ReasoningEffort != "" {
		t.Fatalf("saved inherited reasoning effort = %q, %v", cfg.Harness.ReasoningEffort, err)
	}
}

func TestDirectConfigRejectsUnsupportedACPInferenceProperties(t *testing.T) {
	root := t.TempDir()
	for _, test := range []struct {
		name string
		yaml string
		want string
	}{
		{name: "reasoning", yaml: "reasoning_effort: high", want: "reasoning_effort is not supported for ACP"},
		{name: "reasoning medium", yaml: "reasoning_effort: medium", want: "reasoning_effort is not supported for ACP"},
		{name: "speed", yaml: "service_mode: fast", want: "service_mode is not supported for agent-zero"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writeTestConfig(t, root, []byte("version: 1\nharness:\n  name: agent-zero\n  "+test.yaml+"\n"))
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("unsupported ACP property error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadIgnoresUnusedKeysAndSaveRemovesThem(t *testing.T) {
	root := t.TempDir()
	data := []byte("version: 1\nunused: {anything: true}\nspeech:\n  command: obsolete\nchannels:\n  tui:\n    enabled: false\n    title: Preserved\norchestrator:\n  routes: [{source: old-folder}]\n")
	path := writeTestConfig(t, root, data)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Channels.TUI.Title != "Preserved" {
		t.Fatal("known setting lost")
	}
	before, err := os.ReadFile(path)
	if err != nil || string(before) != string(data) {
		t.Fatal("load rewrote config")
	}
	if err := Save(cfg); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"unused:", "command:", "routes:", "old-folder", "obsolete"} {
		if strings.Contains(string(after), key) {
			t.Fatalf("unused key survived save: %s", key)
		}
	}
	reloaded, err := Load(path)
	if err != nil || reloaded.Channels.TUI.Title != "Preserved" {
		t.Fatalf("saved settings: %#v, %v", reloaded, err)
	}
	for _, invalid := range []string{"speech: {enabled: invalid}", "orchestrator: {max_parallel: 0}", "version: 1\nversion: 2"} {
		if _, err := decode([]byte(invalid), path); err == nil {
			t.Fatalf("invalid current setting accepted: %s", invalid)
		}
	}
}

func TestHarnessSandboxValidationAcceptsCanonicalModes(t *testing.T) {
	for _, mode := range []string{"read-only", "workspace-write", "danger-full-access"} {
		cfg := Default()
		cfg.Harness.Sandbox = mode
		if err := cfg.Validate(); err != nil {
			t.Fatalf("sandbox %q: %v", mode, err)
		}
	}
	cfg := Default()
	cfg.Harness.Sandbox = "unknown"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "harness.sandbox") {
		t.Fatalf("invalid sandbox validation = %v", err)
	}
}

func TestCustomACPRequiresCommandAndPreservesShellFreeArguments(t *testing.T) {
	cfg := Default()
	cfg.Harness.Name = "acp"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "harness.acp_command") {
		t.Fatalf("custom ACP without command = %v", err)
	}
	cfg.Harness.ACPCommand = "/tools/custom agent"
	cfg.Harness.ACPArgs = []string{"--stdio", "value with spaces"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid custom ACP rejected: %v", err)
	}
	arguments := cfg.HarnessArgs()
	if len(arguments) != 2 || arguments[1] != "value with spaces" {
		t.Fatalf("custom ACP args = %#v", arguments)
	}
	arguments[0] = "changed"
	if cfg.Harness.ACPArgs[0] != "--stdio" {
		t.Fatal("custom ACP arguments were returned by reference")
	}
	cfg.Harness.ACPArgs = []string{"bad\x00argument"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "NUL") {
		t.Fatalf("custom ACP NUL argument = %v", err)
	}
}

func TestLoadRejectsUnknownHarnessName(t *testing.T) {
	root := t.TempDir()
	data := []byte("version: 1\nharness:\n  name: custom-acp\n  sandbox: danger-full-access\n  acp_command: fixture\n")
	path := writeTestConfig(t, root, data)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "harness.name") {
		t.Fatalf("unknown harness error = %v", err)
	}
}

func TestStorePersistsValidatedUpdatesAndPublishesSnapshot(t *testing.T) {
	root := t.TempDir()
	path := PathForRoot(root)
	cfg := Default()
	cfg.Path = path
	cfg.Root = root
	if err := Save(cfg); err != nil {
		t.Fatal(err)
	}
	store := NewStore(cfg)
	updated, err := store.Update(func(next *Config) error {
		next.Workspace.HistoryMaxMessages = 25
		next.Workspace.HistoryCharLimit = 9000
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Workspace.HistoryMaxMessages != 25 || store.Snapshot().Workspace.HistoryCharLimit != 9000 {
		t.Fatalf("unexpected stored config: %#v", store.Snapshot().Workspace)
	}
	reloaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Workspace.HistoryMaxMessages != 25 || reloaded.Workspace.HistoryCharLimit != 9000 {
		t.Fatalf("update was not persisted: %#v", reloaded.Workspace)
	}
	select {
	case event := <-store.Updates():
		if event.Workspace.HistoryMaxMessages != 25 {
			t.Fatalf("unexpected update event: %#v", event.Workspace)
		}
	default:
		t.Fatal("configuration update was not published")
	}
	if _, err := store.Update(func(next *Config) error {
		next.Workspace.HistoryMaxMessages = -1
		return nil
	}); err == nil {
		t.Fatal("invalid configuration update succeeded")
	}
	if store.Snapshot().Workspace.HistoryMaxMessages != 25 {
		t.Fatal("invalid update changed the in-memory snapshot")
	}
}

func TestStoreUpdateSavesAndReloadsSharedSnapshot(t *testing.T) {
	root := t.TempDir()
	cfg := Default()
	cfg.Path = PathForRoot(root)
	cfg.Root = root
	if err := Save(cfg); err != nil {
		t.Fatal(err)
	}
	store := NewStore(cfg)
	updated, err := store.Update(func(next *Config) error {
		next.Channels.Telegram.Name = "reloaded"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := Load(cfg.Path)
	if err != nil || reloaded.Channels.Telegram.Name != "reloaded" {
		t.Fatalf("saved configuration = %q, %v", reloaded.Channels.Telegram.Name, err)
	}
	if updated.Channels.Telegram.Name != "reloaded" || store.Snapshot().Channels.Telegram.Name != "reloaded" {
		t.Fatalf("shared snapshot was not refreshed: update=%q snapshot=%q", updated.Channels.Telegram.Name, store.Snapshot().Channels.Telegram.Name)
	}
}

func TestValidateRequiresACPCommandForRoutedACP(t *testing.T) {
	cfg := Default()
	cfg.Harness.Name = "agent-zero"
	cfg.Harness.Routing = &HarnessRouting{
		Developer: "acp",
	}
	cfg.Harness.ACPCommand = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatal("routed ACP configuration without harness.acp_command unexpectedly validated")
	}
	if !strings.Contains(err.Error(), "harness.acp_command") {
		t.Fatalf("validation error = %q, want harness.acp_command", err)
	}
}

func TestProviderProfilesDecodeNormalizeAndRoundTrip(t *testing.T) {
	root := t.TempDir()
	data := []byte(`version: 1
harness:
  name: agent-zero
  providers:
    Claude-Arch:
      harness: claude-code
      model: Model A
      reasoning_effort: high
      sandbox: read-only
    CODEX-DEV:
      harness: Codex
      reasoning_effort: Inherit
      service_mode: priority
      sandbox: workspace-write
    ACP-Runner:
      harness: acp
      acp_command: fixture-agent
      acp_args: ["--stdio", "value with spaces"]
`)
	path := writeTestConfig(t, root, data)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	profiles := cfg.Harness.Providers
	if len(profiles) != 3 {
		t.Fatalf("provider profiles = %#v", profiles)
	}
	if _, ok := profiles["Claude-Arch"]; ok {
		t.Fatal("raw profile key survived normalization")
	}
	arch := profiles["claude-arch"]
	if arch.Harness != "claude-code" || arch.Model != "Model A" || arch.ReasoningEffort != "high" || arch.Sandbox != "read-only" {
		t.Fatalf("claude-arch profile = %#v", arch)
	}
	dev := profiles["codex-dev"]
	if dev.Harness != "codex" || dev.ReasoningEffort != "" || dev.ServiceMode != "priority" || dev.Sandbox != "workspace-write" {
		t.Fatalf("codex-dev profile = %#v", dev)
	}
	runner := profiles["acp-runner"]
	if runner.Harness != "acp" || runner.ACPCommand != "fixture-agent" || len(runner.ACPArgs) != 2 || runner.ACPArgs[1] != "value with spaces" {
		t.Fatalf("acp-runner profile = %#v", runner)
	}
	if err := Save(cfg); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reloaded.Harness.Providers, profiles) {
		t.Fatalf("round trip lost profile values:\ngot  %#v\nwant %#v", reloaded.Harness.Providers, profiles)
	}
}

func TestProviderProfilesValidation(t *testing.T) {
	accept := func(t *testing.T, name string, providers map[string]ProviderProfile) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			cfg := Default()
			cfg.Harness.Name = "agent-zero"
			cfg.Harness.Providers = providers
			if err := cfg.Validate(); err != nil {
				t.Fatalf("valid provider profiles rejected: %v", err)
			}
		})
	}
	// Unused profiles are accepted: they may be declared now and routed later.
	accept(t, "unused same-kind profiles", map[string]ProviderProfile{
		"claude-arch": {Harness: "claude-code", ReasoningEffort: "high", Sandbox: "read-only"},
		"claude-rev":  {Harness: "claude-code", ReasoningEffort: "high", Sandbox: "read-only"},
	})
	accept(t, "inherited profile defaults", map[string]ProviderProfile{
		"glm-dev": {Harness: "pi"},
	})
	accept(t, "acp profile with command", map[string]ProviderProfile{
		"acp-runner": {Harness: "acp", ACPCommand: "fixture-agent", ACPArgs: []string{"--stdio"}},
	})
	accept(t, "codex service mode", map[string]ProviderProfile{
		"codex-prod": {Harness: "codex", ServiceMode: "priority"},
	})

	rejections := []struct {
		name    string
		prepare func(h *Harness)
		want    string
	}{
		{name: "empty harness", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-dev": {}}
		}, want: "harness.providers.glm-dev.harness is required"},
		{name: "unknown harness", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-dev": {Harness: "not-a-harness"}}
		}, want: "harness.providers.glm-dev.harness is not a supported coding harness"},
		{name: "empty identity", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"": {Harness: "codex"}}
		}, want: "must be a non-empty lowercase instance identity"},
		{name: "whitespace identity", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"   ": {Harness: "codex"}}
		}, want: "must be a non-empty lowercase instance identity"},
		{name: "uppercase identity", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"Claude-Arch": {Harness: "codex"}}
		}, want: "must be a non-empty lowercase instance identity"},
		{name: "path separator identity", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"claude-arch/escape": {Harness: "codex"}}
		}, want: "must be a non-empty lowercase instance identity"},
		{name: "dot identity", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{".": {Harness: "codex"}}
		}, want: "must be a non-empty lowercase instance identity"},
		{name: "dotdot identity", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"..": {Harness: "codex"}}
		}, want: "must be a non-empty lowercase instance identity"},
		{name: "overlong identity", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{strings.Repeat("a", 64): {Harness: "codex"}}
		}, want: "must be a non-empty lowercase instance identity"},
		{name: "reserved codex kind", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"codex": {Harness: "codex"}}
		}, want: "is reserved by a built-in harness kind"},
		{name: "reserved claude-code kind", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"claude-code": {Harness: "claude-code"}}
		}, want: "is reserved by a built-in harness kind"},
		{name: "reserved agent-zero kind", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"agent-zero": {Harness: "agent-zero"}}
		}, want: "is reserved by a built-in harness kind"},
		{name: "reserved acp kind", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"acp": {Harness: "acp", ACPCommand: "fixture"}}
		}, want: "is reserved by a built-in harness kind"},
		{name: "invalid reasoning effort", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-dev": {Harness: "claude-code", ReasoningEffort: "two words"}}
		}, want: "harness.providers.glm-dev.reasoning_effort must be inherit"},
		{name: "acp reasoning effort", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-acp": {Harness: "acp", ReasoningEffort: "high", ACPCommand: "fixture"}}
		}, want: "reasoning_effort is not supported for ACP harnesses"},
		{name: "agent-zero reasoning effort", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-zero": {Harness: "agent-zero", ReasoningEffort: "high"}}
		}, want: "reasoning_effort is not supported for ACP harnesses"},
		{name: "service mode on unsupported kind", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-dev": {Harness: "claude-code", ServiceMode: "priority"}}
		}, want: "service_mode is not supported for claude-code"},
		{name: "invalid sandbox", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-dev": {Harness: "claude-code", Sandbox: "full-access"}}
		}, want: "sandbox must be empty (inherit), read-only, workspace-write, or danger-full-access"},
		{name: "literal inherit sandbox", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-dev": {Harness: "claude-code", Sandbox: "inherit"}}
		}, want: "sandbox must be empty (inherit), read-only, workspace-write, or danger-full-access"},
		{name: "acp profile without acp_command", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-acp": {Harness: "acp"}}
		}, want: "acp_command is required"},
		{name: "non-acp profile with acp_command", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-dev": {Harness: "claude-code", ACPCommand: "fixture"}}
		}, want: "acp_command is only supported"},
		{name: "non-acp profile with acp_args", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-dev": {Harness: "claude-code", ACPArgs: []string{"--stdio"}}}
		}, want: "acp_args is only supported"},
		{name: "invalid acp args NUL", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-acp": {Harness: "acp", ACPCommand: "fixture", ACPArgs: []string{"bad\x00argument"}}}
		}, want: "NUL"},
		{name: "invalid acp args UTF-8", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-acp": {Harness: "acp", ACPCommand: "fixture", ACPArgs: []string{"\xff"}}}
		}, want: "invalid UTF-8"},
		{name: "invalid acp args multiline", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-acp": {Harness: "acp", ACPCommand: "fixture", ACPArgs: []string{"line\nbreak"}}}
		}, want: "multiline"},
		{name: "invalid model line", prepare: func(h *Harness) {
			h.Providers = map[string]ProviderProfile{"glm-dev": {Harness: "claude-code", Model: strings.Repeat("m", 1025)}}
		}, want: "model must be one line of at most 1024 bytes"},
	}
	for _, test := range rejections {
		t.Run(test.name, func(t *testing.T) {
			cfg := Default()
			cfg.Harness.Name = "agent-zero"
			test.prepare(&cfg.Harness)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}

	// Distinct raw keys that normalize to one identity are rejected on load.
	root := t.TempDir()
	path := writeTestConfig(t, root, []byte("version: 1\nharness:\n  name: agent-zero\n  providers:\n    Claude-Arch:\n      harness: claude-code\n    claude-arch:\n      harness: claude-code\n"))
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "normalize to the same provider identity") {
		t.Fatalf("duplicate normalized identity = %v", err)
	}
}

// A literal `sandbox: inherit` value is rejected: only the empty value means
// inheritance, and the validation message must not present "inherit" as a
// valid literal value.
func TestProviderProfileLiteralInheritSandboxRejected(t *testing.T) {
	root := t.TempDir()
	path := writeTestConfig(t, root, []byte("version: 1\nharness:\n  name: agent-zero\n  providers:\n    glm-dev:\n      harness: claude-code\n      sandbox: inherit\n"))
	_, err := Load(path)
	if err == nil {
		t.Fatal("provider profile sandbox inherit was accepted")
	}
	if !strings.Contains(err.Error(), "harness.providers.glm-dev.sandbox must be empty (inherit), read-only, workspace-write, or danger-full-access") {
		t.Fatalf("validation error = %q", err)
	}
	if strings.Contains(err.Error(), "must be inherit,") {
		t.Fatalf("validation error presents inherit as a valid literal value: %q", err)
	}
}

func TestProviderSessionsPath(t *testing.T) {
	root := t.TempDir()
	cfg := Default()
	cfg.Root = root
	cfg.Path = PathForRoot(root)

	legacy := ProviderRef{ID: "claude-code", Kind: "claude-code"}
	if got, want := cfg.ProviderSessionsPath(legacy), cfg.HarnessSessionsPath("claude-code"); got != want {
		t.Fatalf("legacy provider session path = %q, want %q", got, want)
	}

	arch := ProviderRef{ID: "claude-arch", Kind: "claude-code", Profile: &ProviderProfile{Harness: "claude-code"}}
	rev := ProviderRef{ID: "claude-rev", Kind: "claude-code", Profile: &ProviderProfile{Harness: "claude-code"}}
	archPath := cfg.ProviderSessionsPath(arch)
	revPath := cfg.ProviderSessionsPath(rev)
	wantArch := filepath.Join(root, ".spynel", "runtime", "providers", "claude-arch", "harness-claude-code-sessions.json")
	wantRev := filepath.Join(root, ".spynel", "runtime", "providers", "claude-rev", "harness-claude-code-sessions.json")
	if archPath != wantArch || revPath != wantRev {
		t.Fatalf("profile session paths = %q and %q", archPath, revPath)
	}
	if archPath == revPath || filepath.Dir(archPath) == filepath.Dir(revPath) {
		t.Fatal("same-kind instances share a session directory")
	}
	if archPath == cfg.ProviderSessionsPath(legacy) {
		t.Fatal("profile session path overlaps the legacy path")
	}
	providersRoot := filepath.Join(root, ".spynel", "runtime", "providers")
	if !strings.HasPrefix(archPath, providersRoot+string(os.PathSeparator)) {
		t.Fatalf("profile session path escapes the providers directory: %q", archPath)
	}

	// A kind change moves the instance's session filename.
	piRef := ProviderRef{ID: "claude-arch", Kind: "pi", Profile: &ProviderProfile{Harness: "pi"}}
	if got := cfg.ProviderSessionsPath(piRef); got == archPath || !strings.HasSuffix(got, "harness-pi-sessions.json") {
		t.Fatalf("kind change did not move the session file: %q", got)
	}

	// Valid normalized identities have no traversal opportunity, and even
	// hostile directly-constructed references stay inside the providers
	// directory.
	for _, id := range []string{"claude-arch", "a", "z9", "glm_dev", strings.Repeat("x", 63), "../escape", "nested/id", "back\\slash", ".."} {
		ref := ProviderRef{ID: id, Kind: "codex", Profile: &ProviderProfile{Harness: "codex"}}
		path := cfg.ProviderSessionsPath(ref)
		clean := filepath.Clean(path)
		for _, part := range strings.Split(filepath.ToSlash(clean), "/") {
			if part == ".." || part == "." {
				t.Fatalf("provider identity %q traversed outside the providers directory: %q", id, path)
			}
		}
		if !strings.HasPrefix(path, providersRoot+string(os.PathSeparator)) {
			t.Fatalf("provider identity %q left the providers directory: %q", id, path)
		}
	}
}
