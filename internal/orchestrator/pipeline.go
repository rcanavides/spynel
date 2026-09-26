package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/facts"
)

// launchContext returns the manager's process lifetime for launch pipelines.
// Run installs its caller context; pipelines started outside Run (tests and
// one-shot scans) use a background lifetime.
func (m *Manager) launchContext() context.Context {
	m.runOnce.Do(func() {
		if m.runCtx == nil {
			m.runCtx = context.Background()
		}
	})
	m.runMu.RLock()
	defer m.runMu.RUnlock()
	if m.runCtx == nil {
		return context.Background()
	}
	return m.runCtx
}

// InstallLaunchContext records the manager's process lifetime once.
func (m *Manager) InstallLaunchContext(ctx context.Context) {
	m.runOnce.Do(func() { m.runCtx = ctx })
}

// failLaunch records one failed launch stage durably and parks the lease in
// its error state through the launch fence.
func (m *Manager) failLaunch(lease Lease, launch, stage, errorClass string, cause error) {
	m.recordLaunchFailure(lease, launch, stage, errorClass, cause)
	if _, err := m.updateLeaseValue(lease.ID, launch, func(current *Lease) {
		current.State = "error"
		current.LastError = stage + ": " + errorClass
		current.HeartbeatAt = time.Now().UTC()
	}); err != nil {
		m.log("fail launch " + lease.ID + ": " + err.Error())
	}
}

// runLaunchPipeline completes one isolated launch's post-terminal evidence.
// Implementation launches capture the canonical result from working content
// and run the captured check set; review launches verify the reviewer's
// source checkout is still exactly the reviewed SHA. Completion marks the
// lease finalized, the single state from which an isolated transition may
// reconcile. Every step is idempotent by its fact key, so an interrupted
// pipeline resumes without duplicates.
func (m *Manager) runLaunchPipeline(ctx context.Context, route workflowRoute, lease Lease, launch string, terminalAt time.Time) {
	if launch == "" {
		return
	}
	documentID := documentIDForLease(lease)
	created, err := m.launchCreatedOf(lease.Route, documentID, launch)
	if err != nil {
		m.failLaunch(lease, launch, "evidence", "launch_evidence_missing", err)
		return
	}
	backend := m.workspaceBackend()
	if backend == nil {
		m.failLaunch(lease, launch, "workspace", "workspace_backend_unavailable", errors.New("workspace backend unavailable"))
		return
	}
	if ctx.Err() != nil {
		// Shutdown interrupted the pipeline before it could prove anything;
		// the lease stays resumable and no failure evidence is fabricated.
		return
	}
	workspace, err := backend.Open(ctx, execws.Ref{ID: lease.WorkspaceID, Kind: execws.BackendGitWorktree})
	if err != nil {
		m.failLaunch(lease, launch, "workspace", "workspace_gone", err)
		return
	}
	if ctx.Err() != nil {
		// Shutdown interrupted the pipeline before it could prove anything;
		// the lease stays resumable and no evidence is fabricated.
		return
	}
	if normalizeLeasePhase(route.Name, lease.Phase) == phaseTaskReview {
		m.runReviewPipeline(ctx, route, lease, launch, created, workspace)
		return
	}
	m.runImplementationPipeline(ctx, route, lease, launch, created, workspace, terminalAt)
}

// runImplementationPipeline captures the canonical result and completes the
// captured checks for one isolated task-implementation launch.
func (m *Manager) runImplementationPipeline(ctx context.Context, route workflowRoute, lease Lease, launch string, created facts.Fact, workspace execws.Workspace, terminalAt time.Time) {
	documentID := documentIDForLease(lease)
	evidence, err := m.documentFacts(lease.Route, documentID)
	if err != nil {
		m.failLaunch(lease, launch, "evidence", "fact_journal_unavailable", err)
		return
	}
	resultFact, captured := findFact(evidence, resultCapturedKey(launch))
	if !captured {
		captured, captureErr := m.captureLaunchResult(ctx, lease, launch, created, workspace, terminalAt)
		if captureErr != nil {
			m.failLaunch(lease, launch, "capture", captureClass(captureErr), captureErr)
			return
		}
		resultFact = captured
	}
	if ctx.Err() != nil {
		return
	}
	defs, err := m.launchChecksOf(launch, created.CheckSet)
	if err != nil {
		m.failLaunch(lease, launch, "checks", "check_definitions_unavailable", err)
		return
	}
	for _, def := range defs {
		key := checkCompletedKey(launch, def.ID, resultFact.ResultSHA)
		evidence, err = m.documentFacts(lease.Route, documentID)
		if err != nil {
			m.failLaunch(lease, launch, "checks", "fact_journal_unavailable", err)
			return
		}
		if prior, done := findFact(evidence, key); done {
			if prior.Outcome != execws.CheckOutcomePassed {
				m.failLaunch(lease, launch, "checks", "checks_failed", fmt.Errorf("check %s recorded outcome %s", def.ID, prior.Outcome))
				return
			}
			continue
		}
		result, runErr := workspace.RunCheck(ctx, execws.CheckRequest{LaunchID: launch, Def: def, ResultSHA: resultFact.ResultSHA})
		if ctx.Err() != nil {
			return
		}
		if runErr != nil && errors.Is(runErr, execws.ErrVerifyFailed) {
			// The check mutated the nonignored source: record its evidence,
			// stop the remaining checks, and fail the launch.
			_, _ = m.appendFact(checkFact(lease, launch, def, result))
			m.failLaunch(lease, launch, "checks", "source_mutated", runErr)
			return
		}
		if _, err := m.appendFact(checkFact(lease, launch, def, result)); err != nil {
			m.failLaunch(lease, launch, "checks", "fact_journal_unavailable", err)
			return
		}
		if result.Outcome != execws.CheckOutcomePassed {
			m.failLaunch(lease, launch, "checks", "checks_failed", fmt.Errorf("check %s failed", def.ID))
			return
		}
	}
	if ctx.Err() != nil {
		return
	}
	if _, err := m.updateLeaseValue(lease.ID, launch, func(current *Lease) {
		current.State = "finalized"
		current.HeartbeatAt = time.Now().UTC()
	}); err != nil {
		m.log("finalize launch " + lease.ID + ": " + err.Error())
	}
}

// captureLaunchResult captures the canonical result of one launch from the
// workspace working content and records result_captured evidence.
func (m *Manager) captureLaunchResult(ctx context.Context, lease Lease, launch string, created facts.Fact, workspace execws.Workspace, terminalAt time.Time) (facts.Fact, error) {
	documentID := documentIDForLease(lease)
	capture, err := workspace.Capture(ctx, execws.CaptureRequest{
		LaunchID: launch, BaseSHA: created.BaseSHA, TaskDoc: documentID,
		Title: m.documentTitle(lease), At: terminalAt,
	})
	if err != nil {
		return facts.Fact{}, err
	}
	fact := facts.Fact{
		Kind: facts.KindResultCaptured, Key: resultCapturedKey(launch),
		Doc: documentDoc(lease.Route, documentID), Launch: launch, Phase: lease.Phase,
		WorkspaceID: lease.WorkspaceID, WorkspaceKind: lease.WorkspaceKind,
		BaseSHA: created.BaseSHA, ResultSHA: capture.ResultSHA, TreeSHA: capture.TreeSHA,
		Changed: capture.Changed, AgentHeadMoved: capture.AgentHeadMoved,
		ChangedPaths: capture.ChangedPaths, TargetRef: created.TargetRef,
		TargetOld: created.TargetOld, At: terminalAt,
	}
	published, err := m.appendFact(fact)
	if err != nil {
		return facts.Fact{}, err
	}
	return published, nil
}

// checkFact builds the durable evidence for one completed check.
func checkFact(lease Lease, launch string, def execws.CheckDef, result execws.CheckResult) facts.Fact {
	return facts.Fact{
		Kind: facts.KindCheckCompleted, Key: checkCompletedKey(launch, def.ID, result.ResultSHA),
		Doc: documentDoc(lease.Route, documentIDForLease(lease)), Launch: launch,
		Phase: lease.Phase, Check: result.ID, DefDigest: result.DefDigest,
		ResultSHA: result.ResultSHA, Outcome: result.Outcome, ExitCode: result.ExitCode,
		DurationMS: result.DurationMS, OutputPath: result.OutputPath,
		OutputBytes: result.OutputBytes, OutputKept: result.OutputKept,
		OutputSHA: result.OutputSHA256, Truncated: result.Truncated,
	}
}

// captureClass maps capture failures onto bounded evidence classes.
func captureClass(err error) string {
	switch {
	case errors.Is(err, execws.ErrCaptureRejected):
		return "capture_rejected"
	case errors.Is(err, execws.ErrVerifyFailed):
		return "workspace_verification_failed"
	default:
		return "capture_failed"
	}
}

// documentTitle reads the durable document's title for the canonical commit
// message. A moved or unreadable document falls back to a stable title.
func (m *Manager) documentTitle(lease Lease) string {
	if document, err := ReadDocument(lease.File); err == nil {
		if title := strings.TrimSpace(stringField(document, "title")); title != "" {
			return title
		}
	}
	if status, path := m.locateDocument(lease.Route, filepath.Base(lease.File)); status != "" {
		if document, err := ReadDocument(path); err == nil {
			if title := strings.TrimSpace(stringField(document, "title")); title != "" {
				return title
			}
		}
	}
	return "task result"
}

// locateDocument finds a document by basename across its route's status
// folders after an agent-authored move.
func (m *Manager) locateDocument(routeName, name string) (string, string) {
	route, ok := routeByName(routeName)
	if !ok {
		return "", ""
	}
	base := filepath.Dir(m.Config.Resolve(route.Source))
	for _, status := range route.AllowedNext {
		path := filepath.Join(base, status, name)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return status, path
		}
	}
	return "", ""
}

// runReviewPipeline verifies the reviewer's source checkout still matches the
// exact reviewed SHA. A mutated checkout invalidates the review: the verdict
// is recorded, no integration happens, and the document returns to the review
// queue for a fresh reviewer launch at the same SHA.
func (m *Manager) runReviewPipeline(ctx context.Context, route workflowRoute, lease Lease, launch string, created facts.Fact, workspace execws.Workspace) {
	documentID := documentIDForLease(lease)
	if err := workspace.VerifyAt(ctx, created.ReviewSHA); err != nil {
		if ctx.Err() != nil {
			return
		}
		if _, factErr := m.appendFact(facts.Fact{
			Kind: facts.KindReviewCompleted, Key: reviewCompletedKey(launch),
			Doc: documentDoc(lease.Route, documentID), Launch: launch,
			Phase: lease.Phase, WorkspaceKind: lease.WorkspaceKind,
			ImplLaunch: created.ImplLaunch, ReviewSHA: created.ReviewSHA,
			Verdict: "invalid", WorkspaceMutated: true,
		}); factErr != nil {
			m.log("record invalid review: " + factErr.Error())
		}
		if status, path := m.locateDocument(lease.Route, filepath.Base(lease.File)); status != "" && status != "review" {
			target := filepath.Join(filepath.Dir(filepath.Dir(path)), "review", filepath.Base(path))
			note := "Spynel invalidated this review because the reviewer's source checkout no longer matched the exact reviewed result; it queued a fresh independent review."
			if _, _, moveErr := m.redirectTransition(path, target, "review", note); moveErr != nil {
				m.log("requeue invalidated review: " + moveErr.Error())
			}
		}
		m.finishRuntimeJob(lease.ID)
		_ = os.Remove(m.leasePath(lease.ID))
		m.releaseIsolatedSession(lease)
		return
	}
	if _, err := m.updateLeaseValue(lease.ID, launch, func(current *Lease) {
		current.State = "finalized"
		current.HeartbeatAt = time.Now().UTC()
	}); err != nil {
		m.log("finalize review launch " + lease.ID + ": " + err.Error())
	}
}

// resumeLaunchPipelines resumes crash-interrupted isolated launches after
// interrupted-claim recovery and before transition reconciliation:
//
//   - preparing: the document is claimed and the workspace not yet proven
//     ready; the same launch re-prepares and dispatches (R1/R2);
//   - awaiting_transition with durable terminal evidence but no finalized
//     state: the post-terminal pipeline resumes — capture if missing, checks
//     if needed, then finalized (R4-R8).
//
// Live dispatches and in-flight pipelines are never resumed twice.
func (m *Manager) resumeLaunchPipelines(ctx context.Context) error {
	leases, err := m.loadLeases()
	if err != nil {
		return err
	}
	var errs []error
	for _, lease := range leases {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if lease.WorkspaceKind != execws.WorkspaceKindGitWorktree || lease.LaunchID == "" {
			continue
		}
		route, ok := routeByName(lease.Route)
		if !ok {
			continue
		}
		if m.isInflight(lease.ID) || m.pipelineInflight(lease.ID) {
			continue
		}
		switch lease.State {
		case "preparing":
			if lease.SourceFile != "" {
				// The claim itself is interrupted; interrupted-claim resume
				// owns this lease.
				continue
			}
			if m.harnessForPhase(lease.Phase).IsActive(lease.SessionKey) {
				continue
			}
			m.dispatch(ctx, route, lease, true)
		case "awaiting_transition":
			evidence, evidenceErr := m.documentFacts(lease.Route, documentIDForLease(lease))
			if evidenceErr != nil {
				errs = append(errs, fmt.Errorf("lease %s: resume pipeline evidence: %w", lease.ID, evidenceErr))
				continue
			}
			terminal, hasTerminal := findFact(evidence, providerTerminalKey(lease.LaunchID))
			if !hasTerminal {
				// The durable lease reached awaiting_transition only through a
				// settled terminal emit, so the terminal happened even though
				// its evidence write was lost to the crash (R3'). Recording it
				// now un-strands the already requested transition.
				published, appendErr := m.appendFact(facts.Fact{
					Kind: facts.KindProviderTerminal, Key: providerTerminalKey(lease.LaunchID),
					Doc:    documentDoc(lease.Route, documentIDForLease(lease)),
					Launch: lease.LaunchID, Phase: lease.Phase,
					WorkspaceKind: lease.WorkspaceKind, Provider: string(lease.Provider),
				})
				if appendErr != nil {
					errs = append(errs, fmt.Errorf("lease %s: record recovered terminal: %w", lease.ID, appendErr))
					continue
				}
				terminal = published
			}
			m.resumeLaunchPipelineAsync(ctx, route, lease, terminal.At)
		}
	}
	return errors.Join(errs...)
}

// resumeLaunchPipelineAsync acquires the workflow writer slot for a resumed
// pipeline and hands it to the launch pipeline. The acquisition runs in its
// own goroutine so a busy writer gate never blocks the scan.
func (m *Manager) resumeLaunchPipelineAsync(ctx context.Context, route workflowRoute, lease Lease, terminalAt time.Time) {
	if !m.setPipeline(lease.ID, true) {
		return
	}
	m.jobs.add()
	go func() {
		defer m.jobs.done()
		if !m.acquireWorkflowWriter(ctx) {
			m.setPipeline(lease.ID, false)
			return
		}
		slot := newWriterSlot(m.releaseWorkflowWriter)
		m.registerWriterWaiter(lease.ID, slot)
		defer func() {
			m.setPipeline(lease.ID, false)
			m.clearWriterWaiter(lease.ID)
			slot.Release()
		}()
		m.runLaunchPipeline(m.launchContext(), route, lease, lease.LaunchID, terminalAt)
	}()
}
