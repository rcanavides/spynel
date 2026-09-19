package harness

import (
	"context"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/agent0ai/spynel/internal/core"
)

type acpDrainFixture struct {
	adapter *ACP
	writer  *io.PipeWriter
	turn    *acpTurn
	mu      sync.Mutex
	events  []core.Event
}

func newACPDrainFixture(t *testing.T) *acpDrainFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	f := &acpDrainFixture{writer: writer}
	f.turn = &acpTurn{activity: make(chan struct{}, 1), emit: func(event core.Event) {
		f.mu.Lock()
		f.events = append(f.events, event)
		f.mu.Unlock()
	}}
	waiter := make(chan acpResponse, 1)
	f.adapter = &ACP{
		ctx: ctx, cancel: cancel,
		pending:     map[string]chan acpResponse{"1": waiter},
		promptTurns: map[string]*acpTurn{"1": f.turn},
		sessions:    map[string]acpSession{"key": {ID: "session"}},
		active:      map[string]*acpTurn{"key": f.turn},
	}
	go f.adapter.readLoop(reader)
	go f.adapter.awaitPrompt("key", "session", f.turn, waiter)
	return f
}

func (f *acpDrainFixture) close() {
	_ = f.adapter.Close()
	_ = f.writer.Close()
	synctest.Wait()
}

func (f *acpDrainFixture) send(t *testing.T, value any) {
	t.Helper()
	if err := writeACP(f.writer, value); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
}

func (f *acpDrainFixture) result(t *testing.T, reason string) {
	f.send(t, map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]string{"stopReason": reason}})
}

func (f *acpDrainFixture) update(t *testing.T, kind, text string) {
	f.send(t, map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{
		"sessionId": "session", "update": map[string]any{
			"sessionUpdate": kind,
			"content":       map[string]string{"type": "text", "text": text},
		},
	}})
}

func (f *acpDrainFixture) snapshot() []core.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]core.Event(nil), f.events...)
}

func (f *acpDrainFixture) assertPending(t *testing.T) {
	t.Helper()
	for _, event := range f.snapshot() {
		if event.Done {
			t.Fatalf("turn completed before drain: %#v", event)
		}
	}
	if !f.adapter.IsActive("key") {
		t.Fatal("turn inactive before drain")
	}
	f.turn.mu.Lock()
	terminal := f.turn.terminal
	f.turn.mu.Unlock()
	if !terminal {
		t.Fatal("prompt result did not mark the turn terminal")
	}
}

func (f *acpDrainFixture) assertFinal(t *testing.T, want string) {
	t.Helper()
	events := f.snapshot()
	if len(events) == 0 {
		t.Fatal("missing final event")
	}
	done := 0
	for _, event := range events {
		if event.Kind == core.EventError {
			t.Fatalf("terminal success downgraded: %#v", event)
		}
		if event.Done {
			done++
			if event.Kind != core.EventFinal || event.Text != want || event.FinalText == nil || *event.FinalText != want {
				t.Fatalf("final event = %#v, want text %q", event, want)
			}
		}
	}
	if done != 1 || f.adapter.IsActive("key") {
		t.Fatalf("done events=%d active=%t events=%#v", done, f.adapter.IsActive("key"), events)
	}
}

func acpAdvance(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	<-timer.C
	synctest.Wait()
}

func TestACPTerminalDrainLateChunkAndActive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		f.result(t, "end_turn")
		f.assertPending(t)
		acpAdvance(acpLateOutputQuiet / 2)
		f.update(t, "agent_message_chunk", "late")
		f.assertPending(t)
		acpAdvance(acpLateOutputQuiet - time.Nanosecond)
		f.assertPending(t)
		acpAdvance(time.Nanosecond)
		f.assertFinal(t, "late")
	})
}

func TestACPTerminalDrainOtherSuccessfulStopReason(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		f.result(t, "max_tokens")
		f.assertPending(t)
		acpAdvance(acpLateOutputQuiet)
		f.assertFinal(t, "")
	})
}

func TestACPTerminalDrainQuietResetsOnAnyUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		f.result(t, "end_turn")
		acpAdvance(90 * time.Millisecond)
		f.update(t, "tool_call_update", "")
		acpAdvance(10 * time.Millisecond)
		f.assertPending(t)
		acpAdvance(80 * time.Millisecond)
		f.update(t, "tool_call_update", "")
		acpAdvance(99 * time.Millisecond)
		f.assertPending(t)
		acpAdvance(time.Millisecond)
		f.assertFinal(t, "")
	})
}

func TestACPTerminalDrainHardCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		f.result(t, "end_turn")
		for range 26 {
			acpAdvance(75 * time.Millisecond)
			f.update(t, "tool_call_update", "")
			f.assertPending(t)
		}
		acpAdvance(50 * time.Millisecond)
		f.assertFinal(t, "")
	})
}

func TestACPTerminalDrainPreResultChunkAndLateAfterDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		f.update(t, "agent_message_chunk", "first")
		f.result(t, "end_turn")
		f.assertPending(t)
		acpAdvance(acpLateOutputQuiet)
		f.assertFinal(t, "first")
		before := f.snapshot()
		f.update(t, "agent_message_chunk", "discarded")
		if got := f.snapshot(); len(got) != len(before) {
			t.Fatalf("late update after Done emitted events: before=%#v after=%#v", before, got)
		}
		f.assertFinal(t, "first")
	})
}

func TestACPTerminalDrainTransportEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		f.result(t, "end_turn")
		f.update(t, "agent_message_chunk", "late")
		f.assertPending(t)
		_ = f.writer.Close()
		synctest.Wait()
		f.assertFinal(t, "late")
	})
}

func TestACPTerminalDrainImmediateEOFAfterResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		if err := writeACP(f.writer, map[string]any{
			"jsonrpc": "2.0", "id": 1, "result": map[string]string{"stopReason": "end_turn"},
		}); err != nil {
			t.Fatal(err)
		}
		_ = f.writer.Close()
		synctest.Wait()
		f.assertFinal(t, "")
	})
}

func TestACPTerminalDrainClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		f.result(t, "end_turn")
		f.update(t, "agent_message_chunk", "late")
		f.assertPending(t)
		if err := f.adapter.Close(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		f.assertFinal(t, "late")
	})
}

func TestACPTerminalDrainCancelledImmediate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		at := time.Now()
		f.result(t, "cancelled")
		events := f.snapshot()
		if len(events) != 1 || events[0].Kind != core.EventError || !events[0].Done || f.adapter.IsActive("key") {
			t.Fatalf("cancelled turn = %#v, active=%t", events, f.adapter.IsActive("key"))
		}
		if !time.Now().Equal(at) {
			t.Fatal("cancelled turn entered a virtual-time drain")
		}
	})
}

func TestACPDrainPromptFailuresImmediate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response any
	}{
		{"rpc error", map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32000, "message": "provider error"}}},
		{"decode error", map[string]any{"jsonrpc": "2.0", "id": 1, "result": "wrong shape"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newACPDrainFixture(t)
				defer f.close()
				at := time.Now()
				f.send(t, tc.response)
				events := f.snapshot()
				if len(events) != 1 || events[0].Kind != core.EventError || !events[0].Done || f.adapter.IsActive("key") {
					t.Fatalf("prompt failure = %#v, active=%t", events, f.adapter.IsActive("key"))
				}
				if !time.Now().Equal(at) {
					t.Fatal("prompt failure entered a virtual-time drain")
				}
			})
		})
	}
}

func TestACPDrainFailBeforeTerminalStillErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newACPDrainFixture(t)
		defer f.close()
		_ = f.writer.Close()
		synctest.Wait()
		events := f.snapshot()
		if len(events) != 1 || events[0].Kind != core.EventError || !events[0].Done || f.adapter.IsActive("key") {
			t.Fatalf("pre-terminal failure = %#v, active=%t", events, f.adapter.IsActive("key"))
		}
	})
}
