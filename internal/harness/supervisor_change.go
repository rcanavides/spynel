package harness

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrProviderFenced means structural preparation temporarily bars admission.
	ErrProviderFenced = errors.New("provider is fenced for a structural change")
	// ErrProviderUnavailable identifies failure to select a running provider,
	// not an error returned by an already-admitted provider execution.
	ErrProviderUnavailable = errors.New("harness unavailable")
)

// PreparedChange owns a structural admission fence until Commit or Abort.
// Abort is safe to defer, including after Commit. Close takes ownership of
// an unsettled returned change; subsequent Commit/Abort calls are no-ops.
// No mutex spans this lifetime.
type PreparedChange struct {
	supervisor *Supervisor
	config     HarnessConfig
	candidate  Harness
	settled    bool // protected by supervisor.mu
}

// beginLifecycle reserves lifecycle ownership without retaining a mutex over
// adapter I/O or application persistence. Other lifecycle calls wait, except
// externally requested mutations during a prepared change, which fail closed.
func (s *Supervisor) beginLifecycle(ctx context.Context, waitFenced bool) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if s.fenced && !waitFenced {
			s.mu.Unlock()
			return ErrProviderFenced
		}
		if s.lifecycleDone == nil {
			s.lifecycleDone = make(chan struct{})
			s.mu.Unlock()
			return nil
		}
		done := s.lifecycleDone
		s.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Supervisor) endLifecycle() {
	s.mu.Lock()
	s.endLifecycleLocked()
	s.mu.Unlock()
}

func (s *Supervisor) endLifecycleLocked() {
	close(s.lifecycleDone)
	s.lifecycleDone = nil
	s.wakeLifecycleLocked()
}

func (s *Supervisor) wakeLifecycleLocked() {
	close(s.lifecycleChanged)
	s.lifecycleChanged = make(chan struct{})
}

func (s *Supervisor) admissionTargetLocked() (Harness, error) {
	if s.fenced {
		return nil, ErrProviderFenced
	}
	return s.targetLocked()
}

func (s *Supervisor) finishOperation() {
	s.mu.Lock()
	s.inflight--
	if s.inflight == 0 && s.inflightDone != nil {
		close(s.inflightDone)
		s.inflightDone = nil
	}
	s.mu.Unlock()
}

// lockDispatch returns with mu held, ordered after structural preparation
// and inference persistence. Existing sends wait with their caller context.
// Terminal/interrupt cleanup and read-only inspection need not wait on it.
func (s *Supervisor) lockDispatch(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		done := s.inferenceDone
		if s.fenced {
			done = s.lifecycleDone
		}
		if done == nil {
			return nil
		}
		s.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// PrepareChange proves structural idleness and fences provider/session
// operations before constructing the candidate. Active turns (including
// queued continuations) are never stopped or altered by a rejected prepare.
// Before Start, this preserves Reconfigure's configuration-only behavior;
// otherwise the candidate is fully started before preparation succeeds.
func (s *Supervisor) PrepareChange(ctx context.Context, cfg HarnessConfig) (*PreparedChange, error) {
	if err := s.beginLifecycle(ctx, false); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.endLifecycleLocked()
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: harness supervisor is closed", ErrProviderUnavailable)
	}
	if len(s.active) > 0 {
		s.endLifecycleLocked()
		s.mu.Unlock()
		return nil, errors.New("cannot change the harness while a harness turn is active; use /stop or wait for completion")
	}
	s.fenced = true
	s.broadcastReadyLocked()
	var drained <-chan struct{}
	if s.inflight > 0 {
		if s.inflightDone == nil {
			s.inflightDone = make(chan struct{})
		}
		drained = s.inflightDone
	}
	lifetime := s.ctx
	s.mu.Unlock()
	fail := func(err error) (*PreparedChange, error) {
		s.mu.Lock()
		s.releaseChangeLocked()
		s.mu.Unlock()
		return nil, err
	}
	if drained != nil {
		select {
		case <-drained:
		case <-ctx.Done():
			return fail(ctx.Err())
		}
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	// Candidate construction may load the session map: fence and drain first.
	cfg.Args = append([]string(nil), cfg.Args...)
	cfg.Env = append([]string(nil), cfg.Env...)
	change := &PreparedChange{supervisor: s, config: cfg}
	if lifetime != nil {
		// Preparation cancellation bounds negotiation without making a
		// successfully prepared adapter depend on the transaction context.
		candidateCtx, cancel := context.WithCancel(lifetime)
		stop := context.AfterFunc(ctx, cancel)
		target, err := s.startTarget(candidateCtx, cfg)
		stop()
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				err = errors.Join(ctx.Err(), err)
			}
			return fail(err)
		}
		if err := candidateCtx.Err(); err != nil || ctx.Err() != nil {
			cancel()
			return fail(errors.Join(err, ctx.Err(), target.Close()))
		}
		change.candidate = target
	}
	s.mu.Lock()
	s.prepared = change
	s.wakeLifecycleLocked()
	s.mu.Unlock()
	return change, nil
}

func (s *Supervisor) releaseChangeLocked() {
	s.prepared = nil
	s.fenced = false
	s.broadcastReadyLocked()
	s.endLifecycleLocked()
}

// Commit publishes only in-memory state and reopens admission. The returned
// previous adapter belongs to the caller and must be retired outside locks.
// A settled or Close-owned change publishes nothing and returns nil.
func (p *PreparedChange) Commit() Harness {
	s := p.supervisor
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.settled {
		return nil
	}
	p.settled = true
	previous := s.current
	s.current = p.candidate
	s.config = p.config
	if p.candidate != nil {
		s.startErr = nil
		s.signalReadyLocked()
	} else {
		s.broadcastReadyLocked()
	}
	s.prepared = nil
	s.fenced = false
	s.endLifecycleLocked()
	return previous
}

// Abort closes only the candidate, retaining the old adapter/configuration.
// Admissions remain fenced through candidate cleanup; cleanup errors are
// returned, never substituted for a successful configuration publication.
func (p *PreparedChange) Abort() error {
	s := p.supervisor
	s.mu.Lock()
	if p.settled {
		s.mu.Unlock()
		return nil
	}
	p.settled = true
	s.prepared = nil
	s.mu.Unlock()
	var err error
	if p.candidate != nil {
		err = p.candidate.Close()
	}
	s.mu.Lock()
	s.releaseChangeLocked()
	s.mu.Unlock()
	return err
}

// AdmissionError reports structural availability without admitting provider work.
func (s *Supervisor) AdmissionError() error {
	s.mu.RLock()
	_, err := s.admissionTargetLocked()
	s.mu.RUnlock()
	return err
}
