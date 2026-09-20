package orchestrator

import (
	"context"
	"sync"
)

// pendingGroup counts live pieces of work. It is reusable across generations:
// the zero value is safe (waiting returns immediately while nothing is
// pending), and every 0 -> 1 transition opens a fresh zero channel that is
// closed again on the matching 1 -> 0 transition.
type pendingGroup struct {
	mu      sync.Mutex
	pending int
	zero    chan struct{}
}

func newPendingGroup() *pendingGroup {
	zero := make(chan struct{})
	close(zero)
	return &pendingGroup{zero: zero}
}

func (g *pendingGroup) add() {
	g.mu.Lock()
	if g.pending == 0 {
		g.zero = make(chan struct{})
	}
	g.pending++
	g.mu.Unlock()
}

func (g *pendingGroup) done() {
	g.mu.Lock()
	if g.pending > 0 {
		g.pending--
		if g.pending == 0 {
			close(g.zero)
		}
	}
	g.mu.Unlock()
}

// wait blocks until the group is empty or the context is cancelled. It never
// spawns a helper goroutine and re-checks the count after every wakeup so the
// group stays reusable across wait generations.
func (g *pendingGroup) wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		if g.pending == 0 {
			g.mu.Unlock()
			return nil
		}
		zero := g.zero
		g.mu.Unlock()
		select {
		case <-zero:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *pendingGroup) waitBackground() {
	_ = g.wait(context.Background())
}
