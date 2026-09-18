package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/extensions"
	"github.com/agent0ai/spynel/internal/workspace"
)

func TestWorkflowItemsUsesFolderStateAndDoesNotFollowDocumentSymlinks(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	validPath := cfg.StatePath("tasks", "working", "valid.md")
	if err := WriteDocument(validPath, Document{FrontMatter: map[string]any{
		"id": "task-valid", "title": "Visible task", "status": "todo", "review_required": false,
		"created_at": now.Add(-time.Hour).Format(time.RFC3339), "updated_at": now.Format(time.RFC3339),
		"attempt": 2, "review_attempt": 1, "provider_iterations": 3,
	}, Body: "## Progress\n\n- First.\n- Latest step\n  with continuation.\n\n## Notes\n\n- Not progress.\n"}); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "outside.md")
	if err := WriteDocument(target, Document{FrontMatter: map[string]any{
		"id": "secret", "title": "Must not be read", "status": "working", "updated_at": now.Format(time.RFC3339),
	}, Body: "## Progress\n- secret progress\n"}); err != nil {
		t.Fatal(err)
	}
	link := cfg.StatePath("tasks", "working", "linked.md")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	inventory := New(cfg, &heartbeatHarness{}, extensions.Runner{}).WorkflowItems("tasks")
	if len(inventory.Items) != 2 {
		t.Fatalf("workflow items = %#v", inventory.Items)
	}
	var valid, linked WorkflowItem
	for _, item := range inventory.Items {
		switch item.FileName {
		case "valid.md":
			valid = item
		case "linked.md":
			linked = item
		}
	}
	if valid.Title != "Visible task" || valid.Status != "working" || valid.Step != "Latest step with continuation." || valid.Attempt != 2 || valid.ReviewAttempt != 1 || valid.ProviderIterations != 3 || !valid.HasReviewPolicy || valid.ReviewRequired {
		t.Fatalf("valid workflow summary = %#v", valid)
	}
	if linked.DetailsAvailable || linked.Title != "linked" {
		t.Fatalf("symlink workflow summary = %#v", linked)
	}
	warnings := strings.Join(inventory.Diagnostics, "\n")
	if !strings.Contains(warnings, "front matter says") || !strings.Contains(warnings, "not a readable regular file") || strings.Contains(warnings, root) || strings.Contains(warnings, "Must not be read") {
		t.Fatalf("workflow diagnostics = %#v", inventory.Diagnostics)
	}
	if !containsWorkflowStatus(inventory.Statuses, "working") || !containsWorkflowStatus(inventory.Statuses, "done") {
		t.Fatalf("workflow statuses = %#v", inventory.Statuses)
	}
}

func TestWorkflowItemsExposesOnlyMatchingBlockedLeaseMetadata(t *testing.T) {
	root := t.TempDir()
	if err := workspace.Init(root, false); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(config.PathForRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	blockedSince := time.Date(2026, time.September, 18, 14, 30, 0, 0, time.UTC)
	updatedAt := blockedSince.Add(-time.Hour)
	paths := map[string]string{}
	for _, fixture := range []struct {
		name  string
		id    string
		front map[string]any
	}{
		{name: "blocked.md", id: "task-blocked"},
		{name: "unblocked.md", id: "task-unblocked"},
		{name: "no-lease.md", id: "task-no-lease", front: map[string]any{"blocked": LeaseBlockedProviderUnavailable, "blocked_reason": "spoofed"}},
	} {
		path := cfg.StatePath("tasks", "working", fixture.name)
		front := map[string]any{
			"id": fixture.id, "title": fixture.id, "status": "working", "review_required": true,
			"created_at": updatedAt.Add(-time.Hour).Format(time.RFC3339), "updated_at": updatedAt.Format(time.RFC3339),
		}
		for key, value := range fixture.front {
			front[key] = value
		}
		if err := WriteDocument(path, Document{FrontMatter: front, Body: "## Progress\n\n- Waiting for the provider.\n"}); err != nil {
			t.Fatal(err)
		}
		paths[fixture.id] = path
	}

	manager := New(cfg, &heartbeatHarness{}, extensions.Runner{})
	if err := manager.saveLease(Lease{
		ID: "blocked-lease", Route: "tasks", File: paths["task-blocked"], SessionKey: "blocked-session", State: "processing",
		StartedAt: updatedAt, HeartbeatAt: updatedAt,
		Blocked: &LeaseBlock{Reason: LeaseBlockedProviderUnavailable, Since: blockedSince},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.saveLease(Lease{
		ID: "unblocked-lease", Route: "tasks", File: paths["task-unblocked"], SessionKey: "unblocked-session", State: "processing",
		StartedAt: updatedAt.Add(time.Minute), HeartbeatAt: updatedAt, LastError: "provider unavailable",
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.saveLease(Lease{
		ID: "other-route-lease", Route: "goals", File: paths["task-no-lease"], SessionKey: "other-route-session", State: "processing",
		StartedAt: updatedAt.Add(2 * time.Minute), HeartbeatAt: updatedAt,
		Blocked: &LeaseBlock{Reason: LeaseBlockedProviderUnavailable, Since: blockedSince.Add(time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}

	inventory := manager.WorkflowItems("tasks")
	if len(inventory.Items) != 3 {
		t.Fatalf("workflow items = %#v", inventory.Items)
	}
	items := make(map[string]WorkflowItem, len(inventory.Items))
	for _, item := range inventory.Items {
		items[item.ID] = item
	}
	blocked := items["task-blocked"]
	if blocked.Blocked != LeaseBlockedProviderUnavailable || !blocked.BlockedSince.Equal(blockedSince) {
		t.Fatalf("blocked workflow metadata = %#v", blocked)
	}
	for _, id := range []string{"task-unblocked", "task-no-lease"} {
		item := items[id]
		if item.Blocked != "" || !item.BlockedSince.IsZero() {
			t.Fatalf("%s inherited blocked metadata: %#v", id, item)
		}
	}
}

func containsWorkflowStatus(statuses []string, wanted string) bool {
	for _, status := range statuses {
		if status == wanted {
			return true
		}
	}
	return false
}
