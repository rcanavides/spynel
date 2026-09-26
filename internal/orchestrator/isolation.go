package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/agent0ai/spynel/internal/config"
	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/facts"
	"github.com/agent0ai/spynel/internal/harness"
)

// reconcileIsolatedTaskTransition accepts or rejects an agent-requested
// isolated transition using mechanical evidence only. Implementation
// transitions require a finalized launch with a captured result and passed
// checks; review transitions require a finalized reviewer launch verified at
// the exact reviewed SHA, and an accepted review integrates ff-only before
// any terminal effect.
func (m *Manager) reconcileIsolatedTaskTransition(ctx context.Context, route workflowRoute, lease Lease, phase, status, path string) (string, string, error) {
	switch phase {
	case phaseTaskImplementation:
		return m.reconcileIsolatedImplementation(ctx, route, lease, status, path)
	case phaseTaskReview:
		return m.reconcileIsolatedReview(ctx, route, lease, status, path)
	}
	return status, path, fmt.Errorf("isolated lease %s has unsupported phase %q", lease.ID, phase)
}

// isolatedLiveState reports whether a lease state still belongs to a live or
// resumable launch whose pipeline has not settled.
func isolatedLiveState(state string) bool {
	switch state {
	case "claiming", "preparing", "processing", "recovering", "awaiting_transition", "hook_cancelled":
		return true
	}
	return false
}

// documentSHA256 hashes one durable document's current bytes for evidence.
func documentSHA256(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// implementationEvidence validates the mechanical evidence of one isolated
// implementation launch: a captured result and every configured check passed
// against it. A corrupt journal fails closed.
type implementationEvidence struct {
	Result    facts.Fact
	Created   facts.Fact
	AllPassed bool
}

func (m *Manager) implementationEvidence(routeName, documentID, launch string) (implementationEvidence, error) {
	created, err := m.launchCreatedOf(routeName, documentID, launch)
	if err != nil {
		return implementationEvidence{}, err
	}
	evidence, err := m.documentFacts(routeName, documentID)
	if err != nil {
		return implementationEvidence{}, fmt.Errorf("isolated evidence journal: %w", err)
	}
	result, hasResult := findFact(evidence, resultCapturedKey(launch))
	if !hasResult {
		return implementationEvidence{Created: created}, nil
	}
	// The exact captured set must be reconstructable; a changed live set is
	// irrelevant to this launch.
	defs, err := m.launchChecksOf(launch, created.CheckSet)
	if err != nil {
		return implementationEvidence{Created: created, Result: result}, fmt.Errorf("isolated evidence journal: %w", err)
	}
	for _, def := range defs {
		check, ok := findFact(evidence, checkCompletedKey(launch, def.ID, result.ResultSHA))
		if !ok || check.Outcome != execws.CheckOutcomePassed {
			return implementationEvidence{Created: created, Result: result}, nil
		}
	}
	return implementationEvidence{Created: created, Result: result, AllPassed: hasResult && result.ResultSHA != ""}, nil
}

// ensureReviewRequest binds the one request for an implementation launch to
// that same launch's finalized canonical result and frozen integration base.
func (m *Manager) ensureReviewRequest(lease Lease, evidence implementationEvidence) (reviewRequestEvidence, error) {
	want := facts.Fact{
		Kind: facts.KindReviewRequested, Key: reviewRequestedKey(lease.LaunchID),
		Doc:    documentDoc(lease.Route, documentIDForLease(lease)),
		Launch: lease.LaunchID, Phase: phaseTaskReview,
		WorkspaceKind: execws.WorkspaceKindGitWorktree,
		ImplLaunch:    lease.LaunchID, ReviewSHA: evidence.Result.ResultSHA,
		TargetRef: evidence.Created.TargetRef, TargetOld: evidence.Created.TargetOld,
		ResultSHA: evidence.Result.ResultSHA,
	}
	published, err := m.appendFact(want)
	if err != nil {
		return reviewRequestEvidence{}, err
	}
	if published.ImplLaunch != want.ImplLaunch || published.ReviewSHA != want.ReviewSHA || published.TargetRef != want.TargetRef || published.TargetOld != want.TargetOld {
		return reviewRequestEvidence{}, fmt.Errorf("review request %s does not match finalized implementation %s", published.Key, lease.LaunchID)
	}
	return reviewRequestEvidence{
		ReviewSHA: published.ReviewSHA, TargetRef: published.TargetRef, TargetOld: published.TargetOld,
		ImplLaunch: published.ImplLaunch, Launch: published.Launch,
	}, nil
}

// reconcileIsolatedImplementation accepts the implementation transition of
// one finalized isolated launch. review_required outcomes get exact-SHA
// review requests; direct completions still require full mechanical
// evidence. Unfinalized or failed launches defer or fail closed.
func (m *Manager) reconcileIsolatedImplementation(ctx context.Context, route workflowRoute, lease Lease, status, path string) (string, string, error) {
	base := filepath.Dir(m.Config.Resolve(route.Source))
	name := filepath.Base(path)
	if isolatedLiveState(lease.State) {
		// The pipeline or a replacement launch still owns this document.
		return status, path, errTransitionDeferred
	}
	if lease.State != "finalized" {
		// The launch died without complete evidence: return the document to
		// todo so a fresh launch can retry, and let the caller remove the
		// lease.
		note := "Spynel rejected this isolated implementation transition because its launch did not complete verified result evidence; it returned the task to todo."
		return m.redirectTransition(path, statusPath(base, "todo", name), "todo", note)
	}
	evidence, err := m.implementationEvidence(route.Name, documentIDForLease(lease), lease.LaunchID)
	if err != nil {
		// A corrupt journal fails this document's evidence gates closed
		// without touching the durable transition.
		m.log("isolated implementation " + lease.ID + ": " + err.Error())
		return status, path, errTransitionDeferred
	}
	if evidence.Result.Seq == 0 || !evidence.AllPassed {
		m.log("isolated implementation " + lease.ID + ": result or check evidence is incomplete")
		return status, path, errTransitionDeferred
	}

	harnessSettings := m.harnessSettings()
	document, readErr := ReadDocument(path)
	if readErr != nil {
		return status, path, readErr
	}
	policy, policyErr := TaskPolicyFromDocument(document)
	if policyErr != nil {
		// Persist the conservative interpretation so retries and inspection
		// see the same unambiguous policy.
		document.FrontMatter["review_required"] = true
		if writeErr := WriteDocument(path, document); writeErr != nil {
			return status, path, writeErr
		}
		m.log("normalized unsafe task review policy in " + path + ": " + policyErr.Error())
		policy, _ = TaskPolicyFromDocument(document)
	}
	// Apply the configured review overlay exactly like the shared flow.
	if harnessSettings.Reviews == config.TaskReviewsNever && (status == "review" || status == "reviewing") {
		note := "Independent task review is disabled; Spynel returned this task to todo to record direct-completion evidence."
		return m.redirectTransition(path, statusPath(base, "todo", name), "todo", note)
	}
	if status == "reviewing" || (status == "done" && harnessSettings.EffectiveTaskReviewRequired(policy.ReviewRequired)) {
		note := "Implementation cannot bypass independent review; Spynel redirected this task to review."
		status, path, err = m.redirectTransition(path, statusPath(base, "review", name), "review", note)
		if err != nil {
			return status, path, err
		}
	}
	if !map[string]bool{"todo": true, "review": true, "waiting": true, "done": true, "failed": true, "cancelled": true}[status] {
		note := "Invalid task implementation transition; Spynel returned this task to todo."
		return m.redirectTransition(path, statusPath(base, "todo", name), "todo", note)
	}
	if status == "done" && !harnessSettings.EffectiveTaskReviewRequired(policy.ReviewRequired) {
		if evidenceErr := validateDirectCompletionEvidence(document); evidenceErr != nil {
			note := "Direct completion rejected: " + evidenceErr.Error()
			status, path, err = m.redirectTransition(path, statusPath(base, "review", name), "review", note)
			if err != nil {
				return status, path, err
			}
		}
	}
	if status == "review" {
		if _, err := m.ensureReviewRequest(lease, evidence); err != nil {
			return status, path, err
		}
		if document, readErr := ReadDocument(path); readErr != nil {
			return status, path, readErr
		} else {
			document.FrontMatter["implementation_thread"] = lease.ThreadID
			document.FrontMatter["implementation_session"] = lease.SessionKey
			if err := WriteDocument(path, document); err != nil {
				return status, path, err
			}
		}
		return status, path, nil
	}
	if status == "done" {
		// Integration need is target-relative, never launch-relative: Changed
		// compares the result to this launch's own BaseSHA, so an unchanged
		// continuation of an already-captured result still must integrate
		// whenever the canonical ResultSHA differs from the frozen TargetOld.
		if evidence.Result.ResultSHA != evidence.Created.TargetOld {
			var integrationErr error
			status, path, integrationErr = m.integrateDirectImplementation(ctx, lease, evidence, path)
			if integrationErr != nil || status != "done" {
				return status, path, integrationErr
			}
		}
		m.finalizeTaskCompletionSummary(path, status)
	}
	if status == "done" || status == "waiting" || status == "failed" || status == "cancelled" {
		if err := m.completeTransition(ctx, route, lease, status, path); err != nil {
			return status, path, err
		}
	}
	return status, path, nil
}

// integrateDirectImplementation makes exact-result integration a precondition
// of an isolated implementation's no-review done effect whenever the canonical
// ResultSHA differs from the frozen TargetOld SHA.
func (m *Manager) integrateDirectImplementation(ctx context.Context, lease Lease, evidence implementationEvidence, path string) (string, string, error) {
	backend := m.workspaceBackend()
	if backend == nil {
		return "done", path, errTransitionDeferred
	}
	target := execws.Target{Ref: evidence.Created.TargetRef, OldSHA: evidence.Created.TargetOld}
	integration, integrateErr := backend.Integrator().Integrate(ctx, target, evidence.Result.ResultSHA)
	if integrateErr != nil {
		var advanced *execws.IntegrationAdvancedError
		var blocked *execws.IntegrationBlockedError
		switch {
		case errors.As(integrateErr, &advanced):
			if _, err := m.appendFact(facts.Fact{
				Kind: facts.KindIntegrationRejected, Key: integrationKey(lease.LaunchID),
				Doc: documentDoc(lease.Route, documentIDForLease(lease)), Launch: lease.LaunchID,
				Phase: lease.Phase, WorkspaceKind: lease.WorkspaceKind,
				ImplLaunch: lease.LaunchID, ResultSHA: evidence.Result.ResultSHA,
				TargetRef: target.Ref, TargetOld: target.OldSHA,
				Outcome: "target_advanced", ErrorClass: "target_advanced",
			}); err != nil {
				return "done", path, err
			}
			note := "Spynel could not integrate the direct result because the target branch advanced; it returned this task to todo. The result stays available for porting."
			return m.redirectTransition(path, statusPath(filepath.Dir(filepath.Dir(path)), "todo", filepath.Base(path)), "todo", note)
		case errors.As(integrateErr, &blocked):
			m.log("isolated direct integration blocked: " + integrateErr.Error())
			return "done", path, errTransitionDeferred
		default:
			return "done", path, integrateErr
		}
	}
	if _, err := m.appendFact(facts.Fact{
		Kind: facts.KindIntegrationCompleted, Key: integrationKey(lease.LaunchID),
		Doc: documentDoc(lease.Route, documentIDForLease(lease)), Launch: lease.LaunchID,
		Phase: lease.Phase, WorkspaceKind: lease.WorkspaceKind,
		ImplLaunch: lease.LaunchID, ResultSHA: evidence.Result.ResultSHA,
		TargetRef: target.Ref, TargetOld: target.OldSHA,
		Integration: integration.Mechanism, Outcome: integration.Mechanism,
	}); err != nil {
		return "done", path, err
	}
	return "done", path, nil
}

// reconcileIsolatedReview decides one isolated reviewer launch's transition.
// The verdict comes from the durable document move; Spynel binds the review
// to the exact reviewed SHA, re-verifies the reviewer's workspace, and only
// an accepted review integrates ff-only before any terminal effect.
func (m *Manager) reconcileIsolatedReview(ctx context.Context, route workflowRoute, lease Lease, status, path string) (string, string, error) {
	base := filepath.Dir(m.Config.Resolve(route.Source))
	name := filepath.Base(path)
	if isolatedLiveState(lease.State) {
		return status, path, errTransitionDeferred
	}
	if lease.State != "finalized" {
		// The reviewer launch failed: requeue the document for a fresh
		// reviewer at the same reviewed SHA.
		note := "Spynel requeued this review because the reviewer launch did not complete; a fresh independent reviewer will inspect the same exact result."
		return m.redirectTransition(path, statusPath(base, "review", name), "review", note)
	}
	created, err := m.launchCreatedOf(lease.Route, documentIDForLease(lease), lease.LaunchID)
	if err != nil {
		m.log("isolated review " + lease.ID + ": " + err.Error())
		return status, path, errTransitionDeferred
	}
	if created.ReviewSHA == "" {
		m.log("isolated review " + lease.ID + ": launch has no pinned review SHA")
		return status, path, errTransitionDeferred
	}
	// F10: only the latest review request owns a verdict. A reviewer pinned to
	// a superseded result cannot accept or reject the current one.
	if evidence, evidenceErr := m.documentFacts(lease.Route, documentIDForLease(lease)); evidenceErr == nil {
		if request, ok := m.currentReviewRequest(lease.Route, documentIDForLease(lease), evidence); !ok || request.ReviewSHA != created.ReviewSHA || request.ImplLaunch != created.ImplLaunch {
			if _, factErr := m.appendFact(facts.Fact{
				Kind: facts.KindReviewCompleted, Key: reviewCompletedKey(lease.LaunchID),
				Doc:    documentDoc(lease.Route, documentIDForLease(lease)),
				Launch: lease.LaunchID, Phase: lease.Phase,
				WorkspaceKind: execws.WorkspaceKindGitWorktree,
				ImplLaunch:    created.ImplLaunch, ReviewSHA: created.ReviewSHA,
				Verdict: "invalid", DocSHA: documentSHA256(path),
			}); factErr != nil {
				return status, path, factErr
			}
			note := "Spynel invalidated this review because a newer result replaced the one it inspected; it queued a fresh independent review."
			return m.redirectTransition(path, statusPath(base, "review", name), "review", note)
		}
	}
	// Reviewer thread-reuse defense remains enforced even though isolated
	// launches start with fresh threads.
	if lease.ImplementerThread != "" && lease.ThreadID == lease.ImplementerThread {
		if _, err := m.appendFact(facts.Fact{
			Kind: facts.KindReviewCompleted, Key: reviewCompletedKey(lease.LaunchID),
			Doc:    documentDoc(lease.Route, documentIDForLease(lease)),
			Launch: lease.LaunchID, Phase: lease.Phase,
			WorkspaceKind: execws.WorkspaceKindGitWorktree,
			ImplLaunch:    created.ImplLaunch, ReviewSHA: created.ReviewSHA,
			Verdict: "invalid", DocSHA: documentSHA256(path),
		}); err != nil {
			return status, path, err
		}
		note := "Independent review rejected automatically because the reviewer reused the implementation harness thread."
		return m.redirectTransition(path, statusPath(base, "review", name), "review", note)
	}
	// Re-verify the reviewer's source checkout at the exact reviewed SHA.
	backend := m.workspaceBackend()
	if backend == nil {
		return status, path, errTransitionDeferred
	}
	workspace, err := backend.Open(ctx, execws.Ref{ID: lease.WorkspaceID, Kind: execws.BackendGitWorktree})
	if err != nil {
		return status, path, errTransitionDeferred
	}
	if err := workspace.VerifyAt(ctx, created.ReviewSHA); err != nil {
		if _, factErr := m.appendFact(facts.Fact{
			Kind: facts.KindReviewCompleted, Key: reviewCompletedKey(lease.LaunchID),
			Doc:    documentDoc(lease.Route, documentIDForLease(lease)),
			Launch: lease.LaunchID, Phase: lease.Phase,
			WorkspaceKind: execws.WorkspaceKindGitWorktree,
			ImplLaunch:    created.ImplLaunch, ReviewSHA: created.ReviewSHA,
			Verdict: "invalid", WorkspaceMutated: true, DocSHA: documentSHA256(path),
		}); factErr != nil {
			return status, path, factErr
		}
		note := "Spynel invalidated this review because the reviewer's source checkout no longer matched the exact reviewed result; it queued a fresh independent review."
		return m.redirectTransition(path, statusPath(base, "review", name), "review", note)
	}
	if status != "done" && status != "todo" {
		note := "Invalid task-review transition; review may only accept into done or return findings to todo."
		return m.redirectTransition(path, statusPath(base, "review", name), "review", note)
	}
	verdict := "rejected"
	if status == "done" {
		verdict = "accepted"
	}
	if _, err := m.appendFact(facts.Fact{
		Kind: facts.KindReviewCompleted, Key: reviewCompletedKey(lease.LaunchID),
		Doc:    documentDoc(lease.Route, documentIDForLease(lease)),
		Launch: lease.LaunchID, Phase: lease.Phase,
		WorkspaceKind: execws.WorkspaceKindGitWorktree,
		ImplLaunch:    created.ImplLaunch, ReviewSHA: created.ReviewSHA,
		Provider: string(lease.Provider), Verdict: verdict, DocSHA: documentSHA256(path),
	}); err != nil {
		return status, path, err
	}
	if verdict == "rejected" {
		m.finalizeTaskCompletionSummary(path, status)
		return status, path, nil
	}
	// Accepted review: integration is a precondition of the done effect.
	if err := m.integrateAcceptedReview(ctx, lease, created, path); err != nil {
		if errors.Is(err, errTransitionDeferred) {
			return status, path, err
		}
		return status, path, err
	}
	m.finalizeTaskCompletionSummary(path, status)
	if err := m.completeTransition(ctx, route, lease, status, path); err != nil {
		return status, path, err
	}
	return status, path, nil
}

// integrateAcceptedReview moves the repository target to the approved result
// under the repository-global integration lock, ff-only and CAS-guarded, and
// records the integration evidence. Git truth precedes every terminal effect:
// a blocked integration defers the transition, and an advanced target
// redirects the task to todo while the result ref and evidence stay.
func (m *Manager) integrateAcceptedReview(ctx context.Context, lease Lease, created facts.Fact, path string) error {
	documentID := documentIDForLease(lease)
	implEvidence, err := m.implementationEvidence(lease.Route, documentID, created.ImplLaunch)
	if err != nil {
		m.log("isolated review " + lease.ID + ": " + err.Error())
		return errTransitionDeferred
	}
	if !implEvidence.AllPassed || implEvidence.Result.ResultSHA != created.ReviewSHA {
		m.log("isolated review " + lease.ID + ": implementation evidence does not match the reviewed result")
		return errTransitionDeferred
	}
	backend := m.workspaceBackend()
	if backend == nil {
		return errTransitionDeferred
	}
	target := execws.Target{Ref: created.TargetRef, OldSHA: created.TargetOld}
	integration, integrateErr := backend.Integrator().Integrate(ctx, target, created.ReviewSHA)
	if integrateErr != nil {
		var advanced *execws.IntegrationAdvancedError
		var blocked *execws.IntegrationBlockedError
		switch {
		case errors.As(integrateErr, &advanced):
			if _, err := m.appendFact(facts.Fact{
				Kind: facts.KindIntegrationRejected, Key: integrationKey(lease.LaunchID),
				Doc: documentDoc(lease.Route, documentID), Launch: lease.LaunchID,
				Phase: lease.Phase, WorkspaceKind: lease.WorkspaceKind,
				ImplLaunch: created.ImplLaunch, ResultSHA: created.ReviewSHA,
				TargetRef: created.TargetRef, TargetOld: created.TargetOld,
				Outcome: "target_advanced", ErrorClass: "target_advanced",
			}); err != nil {
				return err
			}
			note := "Spynel could not integrate the approved result because the target branch advanced; it returned this task to todo. The reviewed result stays available for porting."
			_, _, err := m.redirectTransition(path, statusPath(filepath.Dir(filepath.Dir(path)), "todo", filepath.Base(path)), "todo", note)
			return err
		case errors.As(integrateErr, &blocked):
			// Keep the lease and the done document: a later scan retries
			// after the operator resolves the block. Never notify success.
			m.log("isolated review integration blocked: " + integrateErr.Error())
			return errTransitionDeferred
		default:
			return integrateErr
		}
	}
	if _, err := m.appendFact(facts.Fact{
		Kind: facts.KindIntegrationCompleted, Key: integrationKey(lease.LaunchID),
		Doc: documentDoc(lease.Route, documentID), Launch: lease.LaunchID,
		Phase: lease.Phase, WorkspaceKind: lease.WorkspaceKind,
		ImplLaunch: created.ImplLaunch, ResultSHA: created.ReviewSHA,
		TargetRef: created.TargetRef, TargetOld: created.TargetOld,
		Integration: integration.Mechanism, Outcome: integration.Mechanism,
	}); err != nil {
		return err
	}
	return nil
}

// replaceIsolatedLaunch begins a replacement launch identity for one stale
// isolated lease whose prior launch was admitted. The old session binding is
// released; the new launch starts from the eligible continuation base or the
// current target.
func (m *Manager) replaceIsolatedLaunch(ctx context.Context, route workflowRoute, lease *Lease) bool {
	backend := m.workspaceBackend()
	if backend == nil {
		m.log("isolated recovery " + lease.ID + ": workspace backend unavailable")
		return false
	}
	report, err := backend.Preflight(ctx)
	if err != nil {
		m.log("isolated recovery " + lease.ID + ": " + err.Error())
		return false
	}
	documentID := documentIDForLease(*lease)
	evidence, err := m.documentFacts(lease.Route, documentID)
	if err != nil {
		m.log("isolated recovery " + lease.ID + ": read evidence: " + err.Error())
		return false
	}
	oldLaunch := lease.LaunchID
	oldKey := lease.SessionKey
	launch := NewLaunchID()
	defs := []execws.CheckDef(nil)
	baseSHA := report.TargetOld
	targetRef, targetOld := report.TargetRef, report.TargetOld
	var reviewSHA, implLaunch string
	if normalizeLeasePhase(lease.Route, lease.Phase) == phaseTaskReview {
		// A reviewer replacement stays pinned to the exact reviewed result and
		// integration target of the latest review request (R9).
		if request, ok := m.currentReviewRequest(lease.Route, documentID, evidence); ok {
			reviewSHA = request.ReviewSHA
			implLaunch = request.ImplLaunch
			baseSHA = request.ReviewSHA
			if request.TargetRef != "" {
				targetRef, targetOld = request.TargetRef, request.TargetOld
			}
		} else {
			m.log("isolated recovery " + lease.ID + ": no review request for the reviewer replacement")
			return false
		}
	} else {
		defs = orchestratorChecks(m.runtimeSnapshot())
		baseSHA = m.isolatedStartSHA(lease.Route, documentID, evidence, report.TargetOld)
	}
	lease.LaunchID = launch
	lease.WorkspaceID = execws.NewWorkspaceID()
	lease.SessionKey = isolatedSessionKey(lease.Route, documentID, lease.Phase, lease.ClaimAttempt, launch)
	lease.ThreadID = ""
	if _, err := m.appendFact(facts.Fact{
		Kind: facts.KindLaunchCreated, Key: launchCreatedKey(launch),
		Doc: documentDoc(lease.Route, documentID), Launch: launch, Phase: lease.Phase,
		WorkspaceID: lease.WorkspaceID, WorkspaceKind: execws.WorkspaceKindGitWorktree,
		BaseSHA: baseSHA, TargetRef: targetRef, TargetOld: targetOld,
		CheckSet: execws.CheckSetDigest(defs), Supersedes: oldLaunch,
		Provider: string(lease.Provider), ReviewSHA: reviewSHA, ImplLaunch: implLaunch,
	}); err != nil {
		m.log("isolated recovery " + lease.ID + ": record replacement launch: " + err.Error())
		return false
	}
	if err := m.writeLaunchChecks(launch, defs); err != nil {
		m.log("isolated recovery " + lease.ID + ": capture check set: " + err.Error())
		return false
	}
	harness.ReleaseSessionWorkspace(oldKey)
	return true
}
