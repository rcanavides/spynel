//go:build windows

package harness

import (
	"fmt"
	"io"
	"os"
	"time"
)

// runProcessLifecycleFixture provides the Windows behavior for the provider
// process lifecycle fixtures. Windows has no deliverable Unix signals, so the
// cooperative provider exits on stdin EOF while the uncooperative ones block
// until the direct-kill escalation ends them; none of these fixtures record
// signal evidence. The uncooperative fixtures drain stdin in the background
// and record readiness as soon as that drain is established — before stdin
// EOF can ever arrive — so tests can synchronize on hold-ready before
// stopping the process.
func runProcessLifecycleFixture(mode string) int {
	switch mode {
	case "proc-cooperative":
		_, _ = io.Copy(io.Discard, os.Stdin)
		appendFixtureLog(map[string]any{"kind": "exit", "text": "eof"})
		return 0
	case "proc-ignore-eof", "proc-hold":
		go io.Copy(io.Discard, os.Stdin)
		appendFixtureLog(map[string]any{"kind": "hold-ready"})
		for {
			time.Sleep(time.Hour)
		}
	}
	_, _ = fmt.Fprintf(os.Stderr, "unknown windows process fixture mode %q\n", mode)
	return 2
}

// holdThroughBoundedStop models a provider whose turn never completes
// naturally: it drains stdin in the background, records when the cooperative
// stop stage arrives (stdin EOF), and blocks until the direct-kill escalation
// ends it. Windows has no catchable termination signals, so only the stop
// entry is recorded.
func holdThroughBoundedStop() {
	stdinDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		close(stdinDone)
	}()
	<-stdinDone
	appendFixtureLog(map[string]any{"kind": "stop-entered"})
	for {
		time.Sleep(time.Hour)
	}
}
