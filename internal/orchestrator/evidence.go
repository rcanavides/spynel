package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/facts"
	"github.com/agent0ai/spynel/internal/fsx"
	"github.com/agent0ai/spynel/internal/harness"
)

// docKind maps a workflow route to its durable document kind.
func docKind(routeName string) string {
	if routeName == "goals" {
		return facts.DocKindGoal
	}
	return facts.DocKindTask
}

// documentDoc builds the full durable document identity for fact evidence.
func documentDoc(routeName, documentID string) facts.Doc {
	return facts.Doc{Kind: docKind(routeName), ID: documentID}
}

// orchestratorChecks resolves the live configured system checks for one new
// isolated launch. Shared launches never run checks.
func orchestratorChecks(cfg config.Config) []execws.CheckDef {
	defs := make([]execws.CheckDef, 0, len(cfg.Orchestrator.Checks))
	for _, check := range cfg.Orchestrator.Checks {
		defs = append(defs, execws.CheckDef{
			ID: check.ID, Command: check.Command, Args: check.Args,
			Timeout: check.CheckTimeoutDuration(),
		})
	}
	return defs
}

// isolatedClaimForPhase reports whether one new claim attempts the
// git-worktree isolation mode. Only task implementation and task review
// launch isolated workspaces; goals, chat, notification, and heartbeat work
// always run in the shared root. A task review without isolated
// implementation evidence keeps its shared lineage, which
// prepareIsolatedClaim decides from the document's durable facts.
func (m *Manager) isolatedClaimForPhase(routeName, phase string) bool {
	if m.runtimeSnapshot().Orchestrator.EffectiveWorkspaceIsolation() != config.WorkspaceIsolationGitWorktree {
		return false
	}
	if routeName != "tasks" {
		return false
	}
	return phase == phaseTaskImplementation || phase == phaseTaskReview
}

// appendFact publishes one durable fact through the workspace journal.
func (m *Manager) appendFact(fact facts.Fact) (facts.Fact, error) {
	if m.Facts == nil {
		return facts.Fact{}, errors.New("orchestrator: fact journal is unavailable")
	}
	if fact.At.IsZero() {
		fact.At = time.Now().UTC()
	}
	return m.Facts.Append(fact)
}

// documentFacts reads one document's evidence.
func (m *Manager) documentFacts(routeName, documentID string) ([]facts.Fact, error) {
	if m.Facts == nil {
		return nil, errors.New("orchestrator: fact journal is unavailable")
	}
	return m.Facts.Facts(documentDoc(routeName, documentID))
}

// findFact returns the newest fact with the given key.
func findFact(document []facts.Fact, key string) (facts.Fact, bool) {
	for index := len(document) - 1; index >= 0; index-- {
		if document[index].Key == key {
			return document[index], true
		}
	}
	return facts.Fact{}, false
}

// launchCreatedOf reads the launch_created fact of one launch.
func (m *Manager) launchCreatedOf(routeName, documentID, launch string) (facts.Fact, error) {
	document, err := m.documentFacts(routeName, documentID)
	if err != nil {
		return facts.Fact{}, err
	}
	fact, ok := findFact(document, launchCreatedKey(launch))
	if !ok {
		return facts.Fact{}, fmt.Errorf("launch %s has no launch_created evidence", launch)
	}
	return fact, nil
}

// writeLaunchChecks persists the exact captured check definitions of one
// isolated implementation launch so crash recovery reruns the same set.
func (m *Manager) writeLaunchChecks(launch string, defs []execws.CheckDef) error {
	if len(defs) == 0 {
		return nil
	}
	data, err := json.Marshal(defs)
	if err != nil {
		return err
	}
	return fsx.AtomicWriteFile(m.launchChecksPath(launch), data, 0o600)
}

// launchChecksOf restores the captured check definitions of one launch. The
// recorded set digest must match or the launch fails closed.
func (m *Manager) launchChecksOf(launch, wantSetDigest string) ([]execws.CheckDef, error) {
	path := m.launchChecksPath(launch)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		// The launch captured an empty set; nothing to restore.
		if wantSetDigest == execws.CheckSetDigest(nil) {
			return nil, nil
		}
		// Fall back to the live configuration when it still matches exactly.
		defs := orchestratorChecks(m.runtimeSnapshot())
		if execws.CheckSetDigest(defs) == wantSetDigest {
			return defs, nil
		}
		return nil, fmt.Errorf("launch %s check definitions are missing and the live set changed", launch)
	}
	if err != nil {
		return nil, err
	}
	var defs []execws.CheckDef
	if err := json.Unmarshal(data, &defs); err != nil {
		return nil, fmt.Errorf("launch %s check definitions are unreadable: %w", launch, err)
	}
	if execws.CheckSetDigest(defs) != wantSetDigest {
		return nil, fmt.Errorf("launch %s check definitions no longer match the captured digest", launch)
	}
	return defs, nil
}

func (m *Manager) launchChecksPath(launch string) string {
	return m.Config.StatePath("runtime", "launches", launch, "checks.json")
}

// isolatedStartSHA resolves the start commit for one new isolated launch: a
// prior captured result remains an eligible unfinished continuation while the
// target still matches its captured value; otherwise the launch starts from
// the current target.
func (m *Manager) isolatedStartSHA(routeName, documentID string, document []facts.Fact, currentTarget string) string {
	for index := len(document) - 1; index >= 0; index-- {
		fact := document[index]
		if fact.Kind != facts.KindResultCaptured || !fact.Changed {
			continue
		}
		if fact.TargetOld != "" && fact.TargetOld == currentTarget && fact.ResultSHA != "" {
			return fact.ResultSHA
		}
		return currentTarget
	}
	return currentTarget
}

// prepareIsolatedClaim freezes one isolated launch before the document is
// claimed: preflight, launch identity, session key, and durable evidence. A
// false return leaves the task eligible and unclaimed with the reason logged.
func (m *Manager) prepareIsolatedClaim(ctx context.Context, route workflowRoute, phase, documentID, source string, document Document, lease *Lease) bool {
	backend := m.workspaceBackend()
	if backend == nil {
		m.log("isolated claim " + source + ": workspace backend unavailable")
		return false
	}
	report, err := backend.Preflight(ctx)
	if err != nil {
		m.log("isolated claim " + source + ": " + err.Error())
		return false
	}
	evidence, err := m.documentFacts(route.Name, documentID)
	if err != nil {
		m.log("isolated claim " + source + ": read evidence: " + err.Error())
		return false
	}
	var baseSHA, reviewSHA, targetRef, targetOld, implLaunch string
	var defs []execws.CheckDef
	if phase == phaseTaskReview {
		request, ok := m.currentReviewRequest(route.Name, documentID, evidence)
		if !ok {
			// No isolated implementation evidence: this lineage stays shared.
			return false
		}
		reviewSHA = request.ReviewSHA
		targetRef = request.TargetRef
		targetOld = request.TargetOld
		implLaunch = request.ImplLaunch
		baseSHA = request.ReviewSHA
	} else {
		defs = orchestratorChecks(m.runtimeSnapshot())
		baseSHA = m.isolatedStartSHA(route.Name, documentID, evidence, report.TargetOld)
		targetRef = report.TargetRef
		targetOld = report.TargetOld
	}
	launch := NewLaunchID()
	lease.LaunchID = launch
	lease.WorkspaceID = execws.NewWorkspaceID()
	lease.WorkspaceKind = execws.WorkspaceKindGitWorktree
	lease.SessionKey = isolatedSessionKey(route.Name, documentID, phase, lease.ClaimAttempt, launch)
	lease.ThreadID = ""
	fact := facts.Fact{
		Kind: facts.KindLaunchCreated, Key: launchCreatedKey(launch),
		Doc: documentDoc(route.Name, documentID), Launch: launch, Phase: phase,
		WorkspaceID: lease.WorkspaceID, WorkspaceKind: execws.WorkspaceKindGitWorktree,
		BaseSHA: baseSHA, TargetRef: targetRef, TargetOld: targetOld,
		CheckSet: execws.CheckSetDigest(defs), Provider: string(lease.Provider),
		ImplLaunch: implLaunch, ReviewSHA: reviewSHA,
	}
	if _, err := m.appendFact(fact); err != nil {
		m.log("isolated claim " + source + ": record launch: " + err.Error())
		return false
	}
	if err := m.writeLaunchChecks(launch, defs); err != nil {
		m.log("isolated claim " + source + ": capture check set: " + err.Error())
		return false
	}
	return true
}

// reviewRequestEvidence is one isolated review request.
type reviewRequestEvidence struct {
	ReviewSHA  string
	TargetRef  string
	TargetOld  string
	ImplLaunch string
	Launch     string
}

// latestReviewRequest returns the newest review_requested evidence.
func latestReviewRequest(evidence []facts.Fact) (reviewRequestEvidence, bool) {
	for index := len(evidence) - 1; index >= 0; index-- {
		fact := evidence[index]
		if fact.Kind == facts.KindReviewRequested && fact.ReviewSHA != "" {
			return reviewRequestEvidence{
				ReviewSHA:  fact.ReviewSHA,
				TargetRef:  fact.TargetRef,
				TargetOld:  fact.TargetOld,
				ImplLaunch: fact.ImplLaunch,
				Launch:     fact.Launch,
			}, true
		}
	}
	return reviewRequestEvidence{}, false
}

// currentReviewRequest returns a request only when it is bound to the newest
// captured implementation result and that implementation has complete check
// evidence. Historical requests can never authorize a later review claim.
func (m *Manager) currentReviewRequest(routeName, documentID string, evidence []facts.Fact) (reviewRequestEvidence, bool) {
	var result facts.Fact
	for index := len(evidence) - 1; index >= 0; index-- {
		if evidence[index].Kind == facts.KindResultCaptured && normalizeLeasePhase(routeName, evidence[index].Phase) == phaseTaskImplementation {
			result = evidence[index]
			break
		}
	}
	if result.Launch == "" || result.ResultSHA == "" {
		return reviewRequestEvidence{}, false
	}
	implementation, err := m.implementationEvidence(routeName, documentID, result.Launch)
	if err != nil || !implementation.AllPassed || implementation.Result.ResultSHA != result.ResultSHA {
		return reviewRequestEvidence{}, false
	}
	for index := len(evidence) - 1; index >= 0; index-- {
		fact := evidence[index]
		if fact.Kind != facts.KindReviewRequested || fact.ImplLaunch != result.Launch || fact.ReviewSHA != result.ResultSHA {
			continue
		}
		if fact.TargetRef != implementation.Created.TargetRef || fact.TargetOld != implementation.Created.TargetOld {
			return reviewRequestEvidence{}, false
		}
		return reviewRequestEvidence{
			ReviewSHA: fact.ReviewSHA, TargetRef: fact.TargetRef, TargetOld: fact.TargetOld,
			ImplLaunch: fact.ImplLaunch, Launch: fact.Launch,
		}, true
	}
	return reviewRequestEvidence{}, false
}

// prepareIsolatedWorkspace proves the launch's workspace is ready and binds
// the provider session to it. It is idempotent for the same launch identity.
func (m *Manager) prepareIsolatedWorkspace(ctx context.Context, lease Lease) bool {
	backend := m.workspaceBackend()
	if backend == nil {
		return false
	}
	created, err := m.launchCreatedOf(lease.Route, documentIDForLease(lease), lease.LaunchID)
	if err != nil {
		m.log("isolated workspace " + lease.ID + ": " + err.Error())
		return false
	}
	evidence, err := m.documentFacts(lease.Route, documentIDForLease(lease))
	if err != nil {
		m.log("isolated workspace " + lease.ID + ": read evidence: " + err.Error())
		return false
	}
	if _, ready := findFact(evidence, workspaceReadyKey(lease.LaunchID)); !ready {
		if err := backend.Prepare(ctx, execws.Spec{ID: lease.WorkspaceID, StartSHA: created.BaseSHA}); err != nil {
			m.log("isolated workspace " + lease.ID + ": " + err.Error())
			return false
		}
		if _, err := m.appendFact(facts.Fact{
			Kind: facts.KindWorkspaceReady, Key: workspaceReadyKey(lease.LaunchID),
			Doc: created.Doc, Launch: lease.LaunchID, Phase: lease.Phase,
			WorkspaceID: lease.WorkspaceID, WorkspaceKind: lease.WorkspaceKind,
			BaseSHA: created.BaseSHA, TargetRef: created.TargetRef, TargetOld: created.TargetOld,
		}); err != nil {
			m.log("isolated workspace " + lease.ID + ": record readiness: " + err.Error())
			return false
		}
	}
	return m.bindIsolatedSession(ctx, backend, lease, created)
}

// bindIsolatedSession opens the workspace and binds the provider session's
// execution directory to the workspace provider view.
func (m *Manager) bindIsolatedSession(ctx context.Context, backend execws.Backend, lease Lease, created facts.Fact) bool {
	workspace, err := backend.Open(ctx, execws.Ref{ID: lease.WorkspaceID, Kind: execws.BackendGitWorktree})
	if err != nil {
		m.log("isolated session " + lease.ID + ": " + err.Error())
		return false
	}
	harness.BindSessionWorkspace(lease.SessionKey, harness.SessionWorkspace{Dir: workspace.ProviderView()})
	return true
}

// documentIDForLease returns the durable document identity recorded at claim
// time. Legacy leases without the field fall back to their document file
// name, which remains stable across status moves.
func documentIDForLease(lease Lease) string {
	if lease.DocumentID != "" {
		return lease.DocumentID
	}
	return filepath.Base(lease.File)
}

// abandonIsolatedClaim fails one isolated claim closed: the launch evidence
// records the failure, the workspace is removed, the claimed document returns
// to the todo queue, and the session binding is released.
func (m *Manager) abandonIsolatedClaim(ctx context.Context, lease Lease, errorClass, detail string) {
	if _, err := m.appendFact(facts.Fact{
		Kind: facts.KindLaunchFailed, Key: lease.LaunchID + ":launch_failed",
		Doc: documentDoc(lease.Route, documentIDForLease(lease)), Launch: lease.LaunchID,
		Phase: lease.Phase, WorkspaceID: lease.WorkspaceID, WorkspaceKind: lease.WorkspaceKind,
		Stage: "claim", ErrorClass: errorClass, ErrorDetail: facts.SanitizeErrorDetail(detail),
	}); err != nil {
		m.log("record failed isolated claim " + lease.ID + ": " + err.Error())
	}
	m.releaseIsolatedSession(lease)
	if backend := m.workspaceBackend(); backend != nil {
		if err := backend.Remove(ctx, execws.Ref{ID: lease.WorkspaceID, Kind: execws.BackendGitWorktree}); err != nil {
			m.log("remove abandoned workspace " + lease.WorkspaceID + ": " + err.Error())
		} else if _, err := m.appendFact(facts.Fact{
			Kind: facts.KindWorkspaceRemoved, Key: workspaceRemovedKey(lease.WorkspaceID),
			Doc: documentDoc(lease.Route, documentIDForLease(lease)), Launch: lease.LaunchID,
			WorkspaceID: lease.WorkspaceID, WorkspaceKind: lease.WorkspaceKind,
		}); err != nil {
			m.log("record workspace removal: " + err.Error())
		}
	}
	// A claimed document returns to the todo queue so the task stays eligible.
	if lease.SourceFile == "" {
		if info, err := os.Stat(lease.File); err == nil && info.Mode().IsRegular() {
			target := filepath.Join(filepath.Dir(filepath.Dir(lease.File)), "todo", filepath.Base(lease.File))
			note := "Spynel could not prepare the isolated workspace for this task (" + errorClass + "); it returned the task to todo without a provider turn."
			if _, _, err := m.redirectTransition(lease.File, target, "todo", note); err != nil {
				m.log("return unclaimed isolated task to todo: " + err.Error())
			}
		}
	}
	_ = os.Remove(m.leasePath(lease.ID))
	m.log("isolated claim " + lease.ID + " failed: " + detail)
}

// releaseIsolatedSession releases the provider session's workspace binding.
func (m *Manager) releaseIsolatedSession(lease Lease) {
	if lease.WorkspaceKind == execws.WorkspaceKindGitWorktree && lease.SessionKey != "" {
		harness.ReleaseSessionWorkspace(lease.SessionKey)
	}
}
