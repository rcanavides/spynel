package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/harness"
	"github.com/agent0ai/spynel/internal/workspace"
)

// failingCloseOwner is a provider lifecycle owner whose Close always fails,
// isolating the service shutdown boundary from any real harness.
type failingCloseOwner struct {
	err error
}

func (o *failingCloseOwner) Start(context.Context) error { return nil }
func (o *failingCloseOwner) Close() error                { return o.err }

// R17: Service.Close still returns the harness close error, and it logs that
// failure through the durable runtime log before the runtime log itself is
// drained and closed, so shutdown diagnostics never land after session end.
func TestServiceCloseLogsHarnessFailureBeforeRuntimeLogShutdown(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	closeBoom := errors.New("primary harness close failed")
	service := newWithHarnessOwner(cfg, newServiceHarness(), &failingCloseOwner{err: closeBoom}, NewRuntime())
	if err := service.Close(); !errors.Is(err, closeBoom) {
		t.Fatalf("Service.Close = %v, want the harness close failure", err)
	}
	logged, ended := -1, -1
	for index, entry := range service.Runtime.Logs() {
		if entry.Level == "error" && entry.Component == "harness" && entry.Event == "close_failed" {
			if !strings.Contains(entry.Text, closeBoom.Error()) {
				t.Fatalf("close failure log lost the cause: %q", entry.Text)
			}
			logged = index
		}
		if entry.Component == "runtime" && entry.Event == "session_end" {
			ended = index
		}
	}
	if logged < 0 {
		t.Fatal("harness close failure was not logged")
	}
	if ended < 0 {
		t.Fatal("runtime log session end missing")
	}
	if logged > ended {
		t.Fatal("harness close failure was logged after the runtime log shutdown")
	}
}

// R9: at the configuration layer, ErrRetirementIncomplete from the provider
// runtime means the requested topology is published and active, so settings
// persistence must proceed, the durable retirement_failed event must be
// logged, and no rollback may run.
func TestApplySettingsTreatsRetirementIncompleteAsPublished(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "codex"
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}
	target := &reconfigurableServiceHarness{serviceHarness: newServiceHarness(), config: harness.HarnessConfig{Name: "codex"}}
	service := New(cfg, target)
	retireErr := errors.Join(harness.ErrRetirementIncomplete, fmt.Errorf(`retire provider "old": boom`))
	var calls atomic.Int32
	service.ReconfigureProviders = func(config.Config) error {
		calls.Add(1)
		return retireErr
	}
	changed, err := service.ApplySettings(map[string]string{"harness.sandbox": "read-only"})
	if err != nil {
		t.Fatalf("ApplySettings = %v, want the published topology to persist", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("reconfiguration ran %d times, want one published reconciliation", calls.Load())
	}
	if len(changed) != 1 || changed[0].Key != "harness.sandbox" {
		t.Fatalf("reported changes = %#v", changed)
	}
	if snapshot := service.Settings.Snapshot().Harness; snapshot.Sandbox != "read-only" {
		t.Fatalf("live settings = %q, want the persisted replacement", snapshot.Sandbox)
	}
	reloaded, err := config.Load(config.PathForRoot(root))
	if err != nil || reloaded.Harness.Sandbox != "read-only" {
		t.Fatalf("persisted settings = %q, %v, want read-only", reloaded.Harness.Sandbox, err)
	}
	found := false
	for _, entry := range service.Runtime.Logs() {
		if entry.Level == "error" && entry.Component == "harness" && entry.Event == "retirement_failed" {
			if !strings.Contains(entry.Text, "retirement incomplete") {
				t.Fatalf("retirement event lost its cause: %q", entry.Text)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("incomplete retirement was not logged as a durable event")
	}
}

// R9: when configuration persistence itself fails, the restore path also
// treats ErrRetirementIncomplete as published: it logs the durable event and
// never turns the incomplete cleanup into a silent success or a second
// topology rollback.
func TestApplySettingsPersistenceFailureLogsIncompleteRetirementRestore(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	path := config.PathForRoot(root)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Harness.Name = "codex"
	if err := config.Save(cfg); err != nil {
		t.Fatal(err)
	}
	target := &reconfigurableServiceHarness{serviceHarness: newServiceHarness(), config: harness.HarnessConfig{Name: "codex"}}
	service := New(cfg, target)
	retireErr := errors.Join(harness.ErrRetirementIncomplete, fmt.Errorf(`retire provider "old": boom`))
	var calls atomic.Int32
	service.ReconfigureProviders = func(config.Config) error {
		calls.Add(1)
		return retireErr
	}
	// Force persistence failure: the canonical file becomes an unusable
	// directory so the atomic replacement cannot succeed.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ApplySettings(map[string]string{"harness.sandbox": "read-only"}); err == nil {
		t.Fatal("persistence failure was reported as success")
	}
	if calls.Load() != 2 {
		t.Fatalf("reconfiguration ran %d times, want the requested change plus one restore", calls.Load())
	}
	found := 0
	for _, entry := range service.Runtime.Logs() {
		if entry.Level == "error" && entry.Component == "harness" && entry.Event == "retirement_failed" {
			found++
		}
	}
	if found != 2 {
		t.Fatalf("retirement_failed events = %d, want the published change and the restore", found)
	}
}
