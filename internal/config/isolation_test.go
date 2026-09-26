package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWorkspaceIsolationDefaultsAndDecode(t *testing.T) {
	cfg := Default()
	if cfg.Orchestrator.WorkspaceIsolation != WorkspaceIsolationShared {
		t.Fatalf("default isolation = %q, want shared", cfg.Orchestrator.WorkspaceIsolation)
	}
	if cfg.Orchestrator.EffectiveWorkspaceIsolation() != WorkspaceIsolationShared {
		t.Fatal("effective default isolation must be shared")
	}
	if len(cfg.Orchestrator.Checks) != 0 {
		t.Fatal("an empty check list is the valid default")
	}
	path := writeTestConfig(t, t.TempDir(), []byte("version: 1\norchestrator:\n  workspace_isolation: Git-Worktree\n"))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Orchestrator.WorkspaceIsolation != WorkspaceIsolationGitWorktree {
		t.Fatalf("decoded isolation = %q, want normalized git-worktree", cfg.Orchestrator.WorkspaceIsolation)
	}
}

func TestWorkspaceIsolationRejectsUnknownModes(t *testing.T) {
	path := writeTestConfig(t, t.TempDir(), []byte("version: 1\norchestrator:\n  workspace_isolation: docker\n"))
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "workspace_isolation") {
		t.Fatalf("unknown isolation mode must fail validation: %v", err)
	}
}

func TestWorkspaceIsolationSettingRoundTrip(t *testing.T) {
	cfg := Default()
	setting, err := SetSetting(&cfg, "orchestrator.workspace_isolation", "git-worktree")
	if err != nil {
		t.Fatalf("SetSetting: %v", err)
	}
	if setting.Value != WorkspaceIsolationGitWorktree {
		t.Fatalf("setting value = %q", setting.Value)
	}
	if _, err := SetSetting(&cfg, "orchestrator.workspace_isolation", "vm"); err == nil {
		t.Fatal("unknown mode must be rejected by the setting path")
	}
	if _, err := SetSetting(&cfg, "orchestrator.workspace_isolation", "shared"); err != nil {
		t.Fatalf("restore shared: %v", err)
	}
}

func TestOrchestratorChecksValidation(t *testing.T) {
	valid := "version: 1\norchestrator:\n  checks:\n    - id: build\n      command: make\n      args: [check]\n      timeout: 5m\n"
	if _, err := Load(writeTestConfig(t, t.TempDir(), []byte(valid))); err != nil {
		t.Fatalf("valid checks must load: %v", err)
	}
	cases := []struct {
		name string
		body string
		want string
	}{
		{"bad id start", "    - id: _build\n      command: make\n", "checks[0].id"},
		{"bad id char", "    - id: bu!ld\n      command: make\n", "checks[0].id"},
		{"empty command", "    - id: build\n      command: \"\"\n", "checks[0].command"},
		{"timeout too small", "    - id: build\n      command: make\n      timeout: 100ms\n", "timeout"},
		{"timeout too large", "    - id: build\n      command: make\n      timeout: 3h\n", "timeout"},
		{"timeout invalid", "    - id: build\n      command: make\n      timeout: soon\n", "timeout"},
		{"multiline arg", "    - id: build\n      command: make\n      args: [\"a\\nb\"]\n", "multiline"},
	}
	for _, testCase := range cases {
		body := "version: 1\norchestrator:\n  checks:\n" + testCase.body
		_, err := Load(writeTestConfig(t, t.TempDir(), []byte(body)))
		if err == nil || !strings.Contains(err.Error(), testCase.want) {
			t.Fatalf("%s: err = %v, want %q", testCase.name, err, testCase.want)
		}
	}
	// Duplicate ids fail across entries.
	duplicate := "version: 1\norchestrator:\n  checks:\n    - id: build\n      command: make\n    - id: build\n      command: go\n"
	if _, err := Load(writeTestConfig(t, t.TempDir(), []byte(duplicate))); err == nil || !strings.Contains(err.Error(), "more than once") {
		t.Fatalf("duplicate ids must fail: %v", err)
	}
	// More than sixteen entries fail.
	var entries strings.Builder
	entries.WriteString("version: 1\norchestrator:\n  checks:\n")
	for i := 0; i < 17; i++ {
		entries.WriteString("    - id: check-")
		entries.WriteString(strings.TrimSpace(time.Duration(i).String()))
		entries.WriteString("\n      command: true\n")
	}
	if _, err := Load(writeTestConfig(t, t.TempDir(), []byte(entries.String()))); err == nil || !strings.Contains(err.Error(), "at most 16") {
		t.Fatalf("seventeen checks must fail: %v", err)
	}
}

func TestOrchestratorCheckTimeoutDefault(t *testing.T) {
	check := OrchestratorCheck{ID: "build", Command: "make"}
	if got := check.CheckTimeoutDuration(); got != 10*time.Minute {
		t.Fatalf("default check timeout = %v, want 10m", got)
	}
	check.Timeout = "90s"
	if got := check.CheckTimeoutDuration(); got != 90*time.Second {
		t.Fatalf("explicit check timeout = %v, want 90s", got)
	}
}

func TestChecksAreNotACommandSetting(t *testing.T) {
	cfg := Default()
	if _, err := SetSetting(&cfg, "orchestrator.checks", "build"); err == nil {
		t.Fatal("the YAML-only check list must not be a command setting")
	}
	if _, ok := SettingByKey(cfg, "orchestrator.checks"); ok {
		t.Fatal("the check list must not appear in the settings catalog")
	}
	if _, ok := SettingByKey(cfg, "orchestrator.workspace_isolation"); !ok {
		t.Fatal("workspace isolation must appear in the settings catalog")
	}
}

func TestLoadedChecksRoundTripThroughCanonicalSave(t *testing.T) {
	root := t.TempDir()
	body := "version: 1\norchestrator:\n  workspace_isolation: git-worktree\n  checks:\n    - id: build\n      command: make\n      args: [check, -j, \"2\"]\n"
	path := writeTestConfig(t, root, []byte(body))
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Orchestrator.Checks) != 1 {
		t.Fatalf("checks = %+v", cfg.Orchestrator.Checks)
	}
	check := cfg.Orchestrator.Checks[0]
	if check.ID != "build" || check.Command != "make" || len(check.Args) != 3 || check.Args[2] != "2" {
		t.Fatalf("decoded check = %+v", check)
	}
	if err := Save(cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.Orchestrator.WorkspaceIsolation != WorkspaceIsolationGitWorktree {
		t.Fatalf("isolation lost through save: %q", reloaded.Orchestrator.WorkspaceIsolation)
	}
	if len(reloaded.Orchestrator.Checks) != 1 || reloaded.Orchestrator.Checks[0].ID != "build" {
		t.Fatalf("checks lost through save: %+v", reloaded.Orchestrator.Checks)
	}
	_ = filepath.Join
}
