package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/core"
	"github.com/agent0ai/spynel/internal/execws"
	"github.com/agent0ai/spynel/internal/facts"
)

// ErrStaleLaunch reports a launch-fenced lease mutation that targeted a
// launch the durable lease no longer owns. A stale launch can never mutate
// current durable workflow state.
var ErrStaleLaunch = errors.New("orchestrator: stale launch")

// errTransitionDeferred reports an isolated transition that must wait for its
// own launch evidence. The lease and the moved document stay untouched.
var errTransitionDeferred = errors.New("orchestrator: isolated transition deferred pending launch evidence")

// launchIDPrefix and the 16-byte random body give every route-scanned
// workflow execution a globally unique durable fencing identity.
const (
	launchIDPrefix = "ln-"
	launchIDLength = 26
)

// NewLaunchID generates one launch identity: ln- plus 26 lowercase base32
// characters. One LaunchID identifies exactly one route-scanned workflow
// provider execution of one phase for one document plus its Spynel
// post-terminal pipeline. Reviewers, recoveries, and correction iterations
// are always separate launches.
func NewLaunchID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("orchestrator: crypto/rand unavailable: " + err.Error())
	}
	encoded := strings.ToLower(base32.StdEncoding.EncodeToString(raw[:]))
	return launchIDPrefix + strings.TrimRight(encoded, "=")[:launchIDLength]
}

// isolatedSessionKey extends the shared phase session key with the launch
// identity. Every isolated launch gets a fresh provider conversation; shared
// launches never append the launch to their session key.
func isolatedSessionKey(routeName, id, phase string, attempt int, launchID string) string {
	return phaseSessionKey(routeName, id, phase, attempt) + ":" + launchID
}

// Deterministic fact idempotency keys. A key that already exists returns the
// existing fact without allocating a sequence.
func launchCreatedKey(launch string) string    { return launch + ":launch_created" }
func workspaceReadyKey(launch string) string   { return launch + ":workspace_ready" }
func providerAdmittedKey(launch string) string { return launch + ":provider_admitted" }
func providerTerminalKey(launch string) string { return launch + ":provider_terminal" }
func resultCapturedKey(launch string) string   { return launch + ":result_captured" }
func checkCompletedKey(launch, check, result string) string {
	return launch + ":check:" + check + ":" + result
}
func reviewRequestedKey(launch string) string     { return launch + ":review_requested" }
func reviewCompletedKey(launch string) string     { return launch + ":review_completed" }
func integrationKey(launch string) string         { return launch + ":integration" }
func workspaceRemovedKey(workspace string) string { return workspace + ":workspace_removed" }

// writerSlot is one acquisition of the shared-checkout workflow writer gate.
// Its release is exactly-once across the launch's whole lifetime: dispatch
// error paths, terminal settlement, and post-terminal pipelines share it.
type writerSlot struct {
	once    sync.Once
	release func()
}

func newWriterSlot(release func()) *writerSlot {
	if release == nil {
		release = func() {}
	}
	return &writerSlot{release: release}
}

func (s *writerSlot) Release() {
	if s == nil {
		return
	}
	s.once.Do(s.release)
}

// writerWaiter couples one live launch's writer slot with its lease so
// same-process supersession can cancel a launch that will never settle on
// its own. Cancellation releases the slot; the replacement launch acquires
// its own.
type writerWaiter struct {
	slot      *writerSlot
	cancel    sync.Once
	cancelled bool
}

func (w *writerWaiter) Cancel() {
	if w == nil {
		return
	}
	w.cancel.Do(func() {
		w.cancelled = true
		w.slot.Release()
	})
}

// registerWriterWaiter records the launch that owns a writer slot for one
// lease. A prior waiter for the same lease is cancelled first: the
// replacement launch must never deadlock behind a superseded launch.
func (m *Manager) registerWriterWaiter(leaseID string, slot *writerSlot) *writerWaiter {
	waiter := &writerWaiter{slot: slot}
	m.mu.Lock()
	defer m.mu.Unlock()
	if previous := m.writerWaiters[leaseID]; previous != nil {
		// Cancelled outside the map lock: Cancel is exactly-once and
		// non-blocking, and the previous waiter's own settle path cannot
		// re-acquire anything.
		go previous.Cancel()
	}
	m.writerWaiters[leaseID] = waiter
	return waiter
}

// cancelWriterWaiter releases a lease's live writer slot. The superseding
// dispatch calls it before acquiring its own slot.
func (m *Manager) cancelWriterWaiter(leaseID string) {
	m.mu.Lock()
	waiter := m.writerWaiters[leaseID]
	delete(m.writerWaiters, leaseID)
	m.mu.Unlock()
	waiter.Cancel()
}

// clearWriterWaiter drops the waiter registration without releasing the slot.
func (m *Manager) clearWriterWaiter(leaseID string) {
	m.mu.Lock()
	delete(m.writerWaiters, leaseID)
	m.mu.Unlock()
}

// leaseUpdateLock serializes launch-fenced lease mutations for one lease id.
// The map grows with the live lease set only; entries are cleaned when the
// lease disappears from the waiters and pipeline registries.
func (m *Manager) leaseUpdateLock(id string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock := m.leaseLocks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		m.leaseLocks[id] = lock
	}
	return lock
}

func (m *Manager) forgetLeaseUpdateLock(id string) {
	m.mu.Lock()
	if len(m.leaseLocks) > 512 {
		delete(m.leaseLocks, id)
	}
	m.mu.Unlock()
}

// updateLease applies one launch-fenced mutation to the durable lease. The
// loaded lease must still be owned by expectedLaunch; an empty expected
// value fences only leases that have never launched. On mismatch the
// mutation fails with ErrStaleLaunch and no durable state changes.
func (m *Manager) updateLease(id string, expectedLaunch string, apply func(*Lease)) error {
	lock := m.leaseUpdateLock(id)
	lock.Lock()
	defer lock.Unlock()
	current, err := m.loadLease(id)
	if err != nil {
		return err
	}
	if current.LaunchID != expectedLaunch {
		return fmt.Errorf("%w: lease %s is owned by launch %q, not %q", ErrStaleLaunch, id, current.LaunchID, expectedLaunch)
	}
	if apply != nil {
		apply(&current)
	}
	return m.saveLease(current)
}

// updateLeaseValue loads, fences, mutates, saves, and returns the lease.
func (m *Manager) updateLeaseValue(id string, expectedLaunch string, apply func(*Lease)) (Lease, error) {
	lock := m.leaseUpdateLock(id)
	lock.Lock()
	defer lock.Unlock()
	current, err := m.loadLease(id)
	if err != nil {
		return Lease{}, err
	}
	if current.LaunchID != expectedLaunch {
		return Lease{}, fmt.Errorf("%w: lease %s is owned by launch %q, not %q", ErrStaleLaunch, id, current.LaunchID, expectedLaunch)
	}
	if apply != nil {
		apply(&current)
	}
	return current, m.saveLease(current)
}

// beginLaunch establishes one durable launch identity before Send. Shared
// dispatches always begin a new launch — a recovery supersession included —
// recording the prior launch as superseded. Isolated dispatches keep the
// identity fixed at claim time: their session key already embeds it.
func (m *Manager) beginLaunch(ctx context.Context, route workflowRoute, lease *Lease) (string, error) {
	if lease.WorkspaceKind == execws.WorkspaceKindGitWorktree {
		if lease.LaunchID == "" {
			return "", errors.New("isolated lease has no launch identity")
		}
		return lease.LaunchID, nil
	}
	prior := lease.LaunchID
	launch := NewLaunchID()
	if _, err := m.updateLeaseValue(lease.ID, prior, func(current *Lease) {
		current.LaunchID = launch
	}); err != nil {
		return "", err
	}
	lease.LaunchID = launch
	if _, err := m.appendFact(facts.Fact{
		Kind: facts.KindLaunchCreated, Key: launchCreatedKey(launch),
		Doc:    documentDoc(route.Name, documentIDForLease(*lease)),
		Launch: launch, Phase: lease.Phase, Supersedes: prior,
		WorkspaceKind: execws.WorkspaceKindShared, Provider: string(lease.Provider),
	}); err != nil {
		return "", err
	}
	return launch, nil
}

// recordProviderAdmitted durably records provider admission for one launch.
func (m *Manager) recordProviderAdmitted(lease Lease, launch string) {
	if launch == "" {
		return
	}
	if _, err := m.appendFact(facts.Fact{
		Kind: facts.KindProviderAdmitted, Key: providerAdmittedKey(launch),
		Doc:    documentDoc(lease.Route, documentIDForLease(lease)),
		Launch: launch, Phase: lease.Phase, WorkspaceKind: lease.WorkspaceKind,
		Provider: string(lease.Provider),
	}); err != nil {
		m.log("record provider admission: " + err.Error())
	}
}

// recordLaunchFailure durably records one failed launch stage with bounded
// diagnostic evidence.
func (m *Manager) recordLaunchFailure(lease Lease, launch, stage, errorClass string, cause error) {
	if launch == "" {
		return
	}
	detail := ""
	if cause != nil {
		detail = facts.SanitizeErrorDetail(cause.Error())
	}
	if _, err := m.appendFact(facts.Fact{
		Kind: facts.KindLaunchFailed, Key: launch + ":launch_failed",
		Doc:    documentDoc(lease.Route, documentIDForLease(lease)),
		Launch: launch, Phase: lease.Phase, WorkspaceID: lease.WorkspaceID,
		WorkspaceKind: lease.WorkspaceKind, Stage: stage,
		ErrorClass: errorClass, ErrorDetail: detail,
	}); err != nil {
		m.log("record launch failure: " + err.Error())
	}
}

// settleLaunch completes the first non-continuing terminal of one launch:
// provider_terminal evidence is recorded, shared launches release the writer
// slot, and isolated launches hand the slot to their post-terminal pipeline.
func (m *Manager) settleLaunch(route workflowRoute, lease Lease, launch string, slot *writerSlot, event core.Event) {
	terminalAt := time.Now().UTC()
	fact, err := m.appendFact(facts.Fact{
		Kind: facts.KindProviderTerminal, Key: providerTerminalKey(launch),
		Doc:    documentDoc(lease.Route, documentIDForLease(lease)),
		Launch: launch, Phase: lease.Phase, WorkspaceKind: lease.WorkspaceKind,
		Provider: string(lease.Provider),
	})
	if err != nil {
		m.log("record provider terminal: " + err.Error())
	} else {
		terminalAt = fact.At
	}
	if lease.WorkspaceKind == execws.WorkspaceKindGitWorktree {
		m.startLaunchPipeline(route, lease, launch, slot, terminalAt)
		return
	}
	slot.Release()
	m.clearWriterWaiter(lease.ID)
}

// startLaunchPipeline runs the launch's post-terminal pipeline while holding
// its writer slot: the writer gate spans the pipeline to completion. The
// pipeline joins the manager's job group so one-shot callers and shutdown
// wait for it.
func (m *Manager) startLaunchPipeline(route workflowRoute, lease Lease, launch string, slot *writerSlot, terminalAt time.Time) {
	if !m.setPipeline(lease.ID, true) {
		// A pipeline for this lease is already in flight: it owns the state.
		slot.Release()
		return
	}
	m.jobs.add()
	go func() {
		defer m.jobs.done()
		defer func() {
			m.setPipeline(lease.ID, false)
			m.clearWriterWaiter(lease.ID)
			slot.Release()
		}()
		m.runLaunchPipeline(m.launchContext(), route, lease, launch, terminalAt)
	}()
}

func (m *Manager) setPipeline(leaseID string, active bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if active {
		if m.pipelines[leaseID] {
			return false
		}
		m.pipelines[leaseID] = true
		return true
	}
	delete(m.pipelines, leaseID)
	return true
}

func (m *Manager) pipelineInflight(leaseID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pipelines[leaseID]
}

// launchWasAdmitted reports whether one launch's provider admission evidence
// exists. Stale recovery of an admitted isolated launch begins a replacement
// launch; an unadmitted crash resumes the same launch.
func (m *Manager) launchWasAdmitted(routeName, documentID, launch string) bool {
	if launch == "" {
		return false
	}
	evidence, err := m.documentFacts(routeName, documentID)
	if err != nil {
		return false
	}
	_, ok := findFact(evidence, providerAdmittedKey(launch))
	return ok
}
