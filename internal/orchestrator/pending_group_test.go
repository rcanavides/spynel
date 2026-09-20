package orchestrator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// P1: concurrent add/done stays safe while a waiter observes the group.
func TestPendingGroupConcurrentAddDoneWithWaiter(t *testing.T) {
	group := newPendingGroup()
	const workers = 16
	var added, doneCount atomic.Int64
	var ready sync.WaitGroup
	ready.Add(workers)
	var workersDone sync.WaitGroup
	workersDone.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer workersDone.Done()
			ready.Done()
			for round := 0; round < 50; round++ {
				group.add()
				added.Add(1)
				group.done()
				doneCount.Add(1)
			}
		}()
	}
	ready.Wait()
	waited := make(chan error, 1)
	go func() { waited <- group.wait(context.Background()) }()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("wait failed while work churned: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not return while work churned")
	}
	workersDone.Wait()
	if err := group.wait(context.Background()); err != nil {
		t.Fatalf("wait after churn failed: %v", err)
	}
	if added.Load() != doneCount.Load() {
		t.Fatalf("added %d, done %d", added.Load(), doneCount.Load())
	}
}

// P2: the group is reusable across multiple wait generations.
func TestPendingGroupReusableAcrossGenerations(t *testing.T) {
	group := newPendingGroup()
	for generation := 0; generation < 3; generation++ {
		if err := group.wait(context.Background()); err != nil {
			t.Fatalf("generation %d wait before add: %v", generation, err)
		}
		group.add()
		group.add()
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := group.wait(cancelled); !errors.Is(err, context.Canceled) {
			t.Fatalf("generation %d wait while pending = %v, want context.Canceled", generation, err)
		}
		group.done()
		group.done()
		if err := group.wait(context.Background()); err != nil {
			t.Fatalf("generation %d wait after done: %v", generation, err)
		}
	}
}

// P3: wait returns on cancellation without stranding a waiter goroutine, and
// an empty group returns immediately even with a cancelled context.
func TestPendingGroupWaitCancellation(t *testing.T) {
	group := newPendingGroup()
	group.add()
	ctx, cancel := context.WithCancel(context.Background())
	waited := make(chan error, 1)
	go func() { waited <- group.wait(ctx) }()
	select {
	case got := <-waited:
		t.Fatalf("wait returned before cancellation: %v", got)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-waited:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled wait = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not observe cancellation")
	}
	// Waiting again with a live context still blocks until the work is done,
	// then the group settles normally without any stranded waiter.
	group.done()
	if err := group.wait(context.Background()); err != nil {
		t.Fatalf("post-cancellation wait = %v", err)
	}
	empty, emptyCancel := context.WithCancel(context.Background())
	emptyCancel()
	if err := newPendingGroup().wait(empty); err != nil {
		t.Fatalf("empty group wait with cancelled context = %v, want nil", err)
	}
}

// P4: done below zero is a no-op, never driving the count negative or
// closing an unrelated generation's channel.
func TestPendingGroupDoneBelowZeroIsNoOp(t *testing.T) {
	group := newPendingGroup()
	group.done()
	group.done()
	if err := group.wait(context.Background()); err != nil {
		t.Fatalf("wait after below-zero done = %v", err)
	}
	group.add()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := group.wait(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait while pending = %v, want context.Canceled", err)
	}
	group.done()
	if err := group.wait(context.Background()); err != nil {
		t.Fatalf("wait after single done = %v", err)
	}
}

// The zero value must stay safe because Manager is also built as a literal in
// tests: waiting on an empty literal group returns immediately and add/done
// cycles work without any constructor.
func TestPendingGroupZeroValueIsUsable(t *testing.T) {
	var group pendingGroup
	if err := group.wait(context.Background()); err != nil {
		t.Fatalf("zero-value wait = %v", err)
	}
	group.add()
	group.done()
	if err := group.wait(context.Background()); err != nil {
		t.Fatalf("zero-value cycle wait = %v", err)
	}
	group.done()
	if err := group.wait(context.Background()); err != nil {
		t.Fatalf("zero-value below-zero wait = %v", err)
	}
}
