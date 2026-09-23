package harness

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// The closeConcurrently helper is the shared engine behind Runtime provider
// shutdown and post-publication retirement. These tests pin its direct
// contract: nil/empty input, one goroutine per item, waiting for every item,
// labeled error wrapping, deterministic aggregation order independent of
// completion order, and panic containment that keeps sibling closes running.

func TestCloseConcurrentlyNilAndEmpty(t *testing.T) {
	if err := closeConcurrently(nil); err != nil {
		t.Fatalf("nil items = %v", err)
	}
	if err := closeConcurrently([]closeItem{}); err != nil {
		t.Fatalf("empty items = %v", err)
	}
}

func TestCloseConcurrentlyEntersEveryItemBeforeRelease(t *testing.T) {
	const count = 3
	entered := make(chan string, count)
	release := make(chan struct{})
	items := make([]closeItem, 0, count)
	for index := 0; index < count; index++ {
		label := fmt.Sprintf("close provider %q", string(rune('a'+index)))
		items = append(items, closeItem{label: label, close: func() error {
			entered <- label
			<-release
			return nil
		}})
	}
	done := make(chan error, 1)
	go func() { done <- closeConcurrently(items) }()
	// Every item must enter while the shared release gate stays closed; a
	// sequential close would block on the first item forever.
	seen := make(map[string]bool, count)
	for index := 0; index < count; index++ {
		seen[awaitChange(t, entered)] = true
	}
	for index := 0; index < count; index++ {
		if !seen[fmt.Sprintf("close provider %q", string(rune('a'+index)))] {
			t.Fatalf("missing concurrent close entry for %q", string(rune('a'+index)))
		}
	}
	close(release)
	if err := awaitChange(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestCloseConcurrentlyWaitsForEveryItem(t *testing.T) {
	first := make(chan struct{})
	second := make(chan struct{})
	last := make(chan struct{})
	entered := make(chan struct{}, 2)
	items := []closeItem{
		{label: "one", close: func() error { entered <- struct{}{}; <-first; return nil }},
		{label: "two", close: func() error { entered <- struct{}{}; <-second; return nil }},
		{label: "three", close: func() error { <-last; return nil }},
	}
	done := make(chan error, 1)
	go func() { done <- closeConcurrently(items) }()
	awaitChange(t, entered)
	awaitChange(t, entered)
	close(first)
	// Two items still hold their gates, so the aggregate cannot return yet
	// regardless of how the released item schedules.
	select {
	case err := <-done:
		t.Fatalf("aggregate returned while items were gated: %v", err)
	default:
	}
	close(second)
	select {
	case err := <-done:
		t.Fatalf("aggregate returned while the last item was gated: %v", err)
	default:
	}
	close(last)
	if err := awaitChange(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestCloseConcurrentlyWrapsErrorsWithLabel(t *testing.T) {
	cause := errors.New("boom")
	err := closeConcurrently([]closeItem{
		{label: `close provider "x"`, close: func() error { return cause }},
	})
	if !errors.Is(err, cause) {
		t.Fatalf("wrapped error lost its cause: %v", err)
	}
	if err.Error() != `close provider "x": boom` {
		t.Fatalf("wrapped error = %q", err.Error())
	}
}

func TestCloseConcurrentlyAggregatesInInputOrder(t *testing.T) {
	// Every item blocks on one shared start gate and then returns its own
	// error, so goroutine completion order is scheduler-dependent; the
	// aggregate must still follow the caller's deterministic input order.
	start := make(chan struct{})
	items := []closeItem{
		{label: "close a", close: func() error { <-start; return errors.New("first") }},
		{label: "close b", close: func() error { <-start; return errors.New("second") }},
		{label: "close c", close: func() error { <-start; return errors.New("third") }},
	}
	done := make(chan error, 1)
	go func() { done <- closeConcurrently(items) }()
	close(start)
	err := awaitChange(t, done)
	if err == nil {
		t.Fatal("expected aggregated failures")
	}
	got := err.Error()
	for _, pair := range [][2]string{{"close a", "close b"}, {"close b", "close c"}, {"close a", "close c"}} {
		if strings.Index(got, pair[0]) > strings.Index(got, pair[1]) {
			t.Fatalf("aggregation order followed completion order: %q", got)
		}
	}
}

func TestCloseConcurrentlyContainsPanicsAndClosesSiblings(t *testing.T) {
	closed := make(chan struct{}, 1)
	err := closeConcurrently([]closeItem{
		{label: `close provider "broken"`, close: func() error { panic("exploded") }},
		{label: `close provider "healthy"`, close: func() error { closed <- struct{}{}; return nil }},
	})
	// A failing test run would never reach the assertions if the panic were
	// not contained: it would take the whole test binary down.
	awaitChange(t, closed)
	if err == nil {
		t.Fatal("panic was converted into success")
	}
	if !strings.Contains(err.Error(), `close provider "broken": panic during close: exploded`) {
		t.Fatalf("panic error = %q", err.Error())
	}
	if strings.Contains(err.Error(), `close provider "healthy"`) {
		t.Fatalf("healthy close was reported as a failure: %q", err.Error())
	}
}
