package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/facts"
)

// workspaceRecord classifies one registered isolated workspace from durable
// evidence for cleanup decisions.
type workspaceRecord struct {
	WorkspaceID   string
	Doc           facts.Doc
	Launch        string
	Phase         string
	NewestFact    time.Time
	Live          bool
	Preparing     bool
	Integrated    bool
	NotApplicable bool
	Superseded    bool
	Rejected      bool
	Failed        bool
	Reviewer      bool
}

// classifyWorkspaces indexes all durable launch evidence by workspace
// identity. A workspace with no launch_created evidence is unknown: cleanup
// reports it and never deletes it.
func (m *Manager) classifyWorkspaces() (map[string]workspaceRecord, []string, error) {
	all, factErrs := m.Facts.AllFacts()
	byLaunch := map[string]facts.Fact{}
	launchWorkspace := map[string]string{}
	records := map[string]workspaceRecord{}
	for _, fact := range all {
		if fact.Launch != "" {
			if _, exists := byLaunch[fact.Launch]; !exists {
				byLaunch[fact.Launch] = fact
			}
			if fact.WorkspaceID != "" {
				if _, exists := launchWorkspace[fact.Launch]; !exists {
					launchWorkspace[fact.Launch] = fact.WorkspaceID
				}
			}
		}
		if fact.WorkspaceID == "" {
			continue
		}
		record, exists := records[fact.WorkspaceID]
		if !exists {
			record = workspaceRecord{WorkspaceID: fact.WorkspaceID, Doc: fact.Doc, Launch: fact.Launch, Phase: fact.Phase}
		}
		if !fact.At.Before(record.NewestFact) {
			record.NewestFact = fact.At
		}
		switch fact.Kind {
		case facts.KindLaunchCreated:
			if fact.Supersedes != "" {
				// Mark the superseded launch's workspace: its retention
				// window starts at the replacement evidence.
				if prior, ok := byLaunch[fact.Supersedes]; ok && prior.WorkspaceID != "" && prior.WorkspaceID != fact.WorkspaceID {
					if priorRecord, ok := records[prior.WorkspaceID]; ok && priorRecord.Launch == fact.Supersedes {
						priorRecord.Superseded = true
						records[prior.WorkspaceID] = priorRecord
					}
				}
			}
		case facts.KindLaunchFailed:
			record.Failed = true
		case facts.KindIntegrationRejected:
			record.Rejected = true
		case facts.KindResultCaptured:
			if !fact.Changed {
				// Unchanged results never integrate: not-applicable.
				record.NotApplicable = true
			}
		case facts.KindReviewRequested:
			// A review request means integration is required before this
			// implementation workspace becomes removable.
			if record.NotApplicable {
				record.NotApplicable = false
			}
		}
		if fact.Phase == phaseTaskReview {
			record.Reviewer = true
		}
		records[fact.WorkspaceID] = record
	}
	// Integration evidence belongs to the reviewer launch while the
	// integrated workspace is the implementation launch's; link the two.
	for _, fact := range all {
		if fact.Kind != facts.KindIntegrationCompleted || fact.ImplLaunch == "" {
			continue
		}
		if workspaceID, ok := launchWorkspace[fact.ImplLaunch]; ok && workspaceID != "" {
			if record, ok := records[workspaceID]; ok {
				record.Integrated = true
				records[workspaceID] = record
			}
		}
	}
	// A launch whose launch_created never reached this index (corrupt
	// journal) leaves its workspace unknown; the caller reports it.
	var corrupt []string
	for _, err := range factErrs {
		corrupt = append(corrupt, err.Error())
	}
	return records, corrupt, nil
}

// CleanupIsolatedWorkspaces removes isolated workspaces whose durable
// evidence proves they are safe to delete:
//
//   - live-lease and preparing workspaces are protected;
//   - integrated or not-applicable implementation workspaces may be removed
//     at the next cleanup;
//   - failed, abandoned, superseded, rejected, and reviewer workspaces are
//     retained for the configured retention window;
//   - registered directories without matching launch_created evidence are
//     reported and never deleted.
//
// Fact evidence is always kept; result refs are never deleted by this
// cleanup (their approved archival boundary is a later milestone).
func (m *Manager) CleanupIsolatedWorkspaces(ctx context.Context, retention time.Duration) ([]string, []string, error) {
	if retention <= 0 {
		return nil, nil, errors.New("isolated workspace retention must be positive")
	}
	return m.cleanupIsolatedWorkspaces(ctx, retention, time.Now().UTC())
}

func (m *Manager) cleanupIsolatedWorkspaces(ctx context.Context, retention time.Duration, now time.Time) ([]string, []string, error) {
	backend := m.workspaceBackend()
	if backend == nil {
		return nil, nil, errors.New("isolated workspace backend unavailable")
	}
	// A root where isolation could never have functioned has no isolated
	// workspaces to clean: git missing, no repository, or a bare repository
	// all fail closed at isolated claims, so cleanup is a no-op rather than
	// an error. Other preflight findings (for example a detached root) do not
	// erase previously registered worktrees, so listing continues.
	if _, err := backend.Preflight(ctx); err != nil {
		var preflight *execws.PreflightError
		if errors.As(err, &preflight) {
			switch preflight.Reason {
			case "git_missing", "root_outside_worktree", "repo_bare":
				return nil, nil, nil
			}
		}
	}
	refs, err := backend.List(ctx)
	if err != nil {
		return nil, nil, err
	}
	records, corrupt, err := m.classifyWorkspaces()
	if err != nil {
		return nil, nil, err
	}
	leases, err := m.loadLeases()
	if err != nil {
		return nil, nil, err
	}
	live := map[string]bool{}
	for _, lease := range leases {
		// An error lease's launch already failed: its workspace follows the
		// retention rules instead of live protection. Every other state still
		// owns its workspace.
		if lease.WorkspaceID != "" && lease.State != "error" {
			live[lease.WorkspaceID] = true
		}
	}
	now = now.UTC()
	var removed, reported []string
	for _, ref := range refs {
		record, known := records[ref.ID]
		if !known {
			reported = append(reported, fmt.Sprintf("unknown workspace %s has no launch_created evidence; not deleted", ref.ID))
			continue
		}
		if live[ref.ID] {
			continue
		}
		removable := false
		switch {
		case record.Reviewer:
			// Reviewer workspaces always retain for the retention window.
			removable = now.Sub(record.NewestFact) >= retention
		case record.Integrated || record.NotApplicable:
			// Integrated or not-applicable implementation workspaces may be
			// removed at the next cleanup.
			removable = true
		case record.Failed || record.Superseded || record.Rejected:
			removable = now.Sub(record.NewestFact) >= retention
		default:
			// Live or unfinished launch evidence: protect.
			continue
		}
		if !removable {
			continue
		}
		if err := backend.Remove(ctx, ref); err != nil {
			reported = append(reported, fmt.Sprintf("remove workspace %s: %v", ref.ID, err))
			continue
		}
		if _, err := m.appendFact(facts.Fact{
			Kind: facts.KindWorkspaceRemoved, Key: workspaceRemovedKey(ref.ID),
			Doc: record.Doc, Launch: record.Launch, WorkspaceID: ref.ID,
			WorkspaceKind: execws.WorkspaceKindGitWorktree,
		}); err != nil {
			m.log("record workspace removal: " + err.Error())
		}
		removed = append(removed, ref.ID)
	}
	reported = append(reported, corrupt...)
	return removed, reported, nil
}
