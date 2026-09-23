package harness

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// These tests race Supervisor.Close against the ownership boundaries of a
// prepared structural change. Every interleaving must keep the same cleanup
// invariants: each adapter closes exactly once, a settled change can never
// republish or reclose, and a closed supervisor stays closed. They complement
// the deterministic single-interleaving tests for the same boundary.

// R11: Close races the settlement of an already-prepared change from both
// sides (Commit and Abort). Whatever wins, the previous adapter and the
// candidate each close exactly once, and later settlement plus a repeated
// Close can never add a second close.
func TestSupervisorCloseVersusPreparedSettlementRace(t *testing.T) {
	for iteration := 0; iteration < 60; iteration++ {
		for settle := 0; settle < 2; settle++ {
			s, old, candidate := newChangeSupervisor(t)
			change, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				if settle == 1 {
					if previous := change.Commit(); previous != nil {
						_ = previous.Close()
					}
					return
				}
				_ = change.Abort()
			}()
			go func() {
				defer wg.Done()
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			wg.Wait()
			// Late settlement and a repeated Close must both be no-ops.
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := change.Abort(); err != nil {
				t.Fatal(err)
			}
			if previous := change.Commit(); previous != nil {
				t.Fatal("settled change republished an adapter")
			}
			if got := old.closeCalls.Load(); got != 1 {
				t.Fatalf("iteration %d settle %d: previous adapter closed %d times, want exactly once", iteration, settle, got)
			}
			if got := candidate.closeCalls.Load(); got != 1 {
				t.Fatalf("iteration %d settle %d: candidate closed %d times, want exactly once", iteration, settle, got)
			}
			if _, _, err := s.Send(context.Background(), "closed", "prompt", nil); !errors.Is(err, ErrProviderUnavailable) {
				t.Fatalf("iteration %d settle %d: closed supervisor send = %v", iteration, settle, err)
			}
		}
	}
}

// R11: Close races an in-flight PrepareChange. Close either adopts the
// published change and owns the candidate cleanup, or it closes first and the
// rejected preparation never constructs the candidate; both sides end with the
// previous adapter closed exactly once.
func TestSupervisorCloseVersusPrepareChangeRace(t *testing.T) {
	for iteration := 0; iteration < 60; iteration++ {
		s, old, candidate := newChangeSupervisor(t)
		prepared := make(chan *PreparedChange, 1)
		failed := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			change, err := s.PrepareChange(context.Background(), HarnessConfig{Name: "new"})
			if err != nil {
				failed <- err
				return
			}
			prepared <- change
		}()
		go func() {
			defer wg.Done()
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
		select {
		case change := <-prepared:
			// Close adopted the published change; late settlement must not
			// add a second candidate close.
			if err := change.Abort(); err != nil {
				t.Fatal(err)
			}
			if got := candidate.closeCalls.Load(); got != 1 {
				t.Fatalf("iteration %d: adopted candidate closed %d times, want exactly once", iteration, got)
			}
		case err := <-failed:
			if !errors.Is(err, ErrProviderUnavailable) {
				t.Fatalf("iteration %d: close-raced prepare = %v, want unavailable", iteration, err)
			}
			if got := candidate.closeCalls.Load(); got != 0 {
				t.Fatalf("iteration %d: rejected preparation closed the candidate %d times", iteration, got)
			}
		}
		if got := old.closeCalls.Load(); got != 1 {
			t.Fatalf("iteration %d: previous adapter closed %d times, want exactly once", iteration, got)
		}
	}
}
