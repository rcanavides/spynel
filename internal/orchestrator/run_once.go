package orchestrator

import (
	"context"
	"errors"
	"fmt"
)

// maxSettleRounds bounds one-shot settlement so an always-changing workspace
// can extend a run but never trap it.
const maxSettleRounds = 8

// settlementGate reuses pendingGroup as the per-run-once terminal-completion
// gate. It tracks only terminal durable completion of provider turns that
// belong to one RunOnce invocation; Manager.jobs separately tracks all
// manager-owned goroutine lifetime.
type settlementGate struct {
	pendingGroup
}

func newSettlementGate() *settlementGate {
	gate := &settlementGate{}
	gate.zero = make(chan struct{})
	close(gate.zero)
	return gate
}

type settlementKey struct{}

func withSettlement(ctx context.Context, gate *settlementGate) context.Context {
	return context.WithValue(ctx, settlementKey{}, gate)
}

func settlementFrom(ctx context.Context) *settlementGate {
	gate, _ := ctx.Value(settlementKey{}).(*settlementGate)
	return gate
}

// reconcileOnly settles durable transition consequences without claiming any
// queued work: exactly one bounded reconciliation pass followed by the
// ordinary outbox. It never runs stale/orphan/resume/wake/goal advancement or
// queue admission.
func (m *Manager) reconcileOnly(ctx context.Context) (int, error) {
	m.scanMu.Lock()
	defer m.scanMu.Unlock()
	reconciled, err := m.reconcileTransitionsCount(ctx)
	if outboxErr := m.Outbox.Process(ctx); outboxErr != nil {
		m.log("notification delivery deferred: " + outboxErr.Error())
	}
	return reconciled, err
}

// RunOnce performs one ordinary scan and then settles the durable
// consequences of the work that scan dispatched. Each settle round waits for
// manager jobs, waits for the terminal durable writes of this invocation's
// provider turns, reconciles the resulting transitions, and processes the
// ordinary outbox. Settlement is bounded, never claims queued work, and may
// be delayed by unrelated live-primary activity because the idle wait and
// reconciliation remain workspace-global.
func (m *Manager) RunOnce(ctx context.Context) error {
	gate := newSettlementGate()
	ctx = withSettlement(ctx, gate)
	var errs []error
	if err := m.ScanOnce(ctx); err != nil {
		// The scan may have dispatched work despite reporting unrelated
		// errors; that work must still settle.
		errs = append(errs, err)
	}
	for round := 0; round < maxSettleRounds; round++ {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if err := m.WaitForIdle(ctx); err != nil {
			return errors.Join(append(errs, err)...)
		}
		if err := gate.wait(ctx); err != nil {
			return errors.Join(append(errs, err)...)
		}
		reconciled, err := m.reconcileOnly(ctx)
		if err != nil {
			errs = append(errs, err)
		}
		if reconciled == 0 {
			return errors.Join(errs...)
		}
	}
	errs = append(errs, fmt.Errorf("run once did not settle within %d rounds", maxSettleRounds))
	return errors.Join(errs...)
}
