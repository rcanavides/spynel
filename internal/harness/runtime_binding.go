package harness

import (
	"context"
	"errors"

	"github.com/agent0ai/spynel/internal/core"
)

func (r *Runtime) roleLocked(role Role) supervisorOperations {
	if provider := r.roles[role]; provider != nil {
		return provider
	}
	return r.roles[RoleChat]
}

func (t *runtimeTarget) current() supervisorOperations {
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.roleLocked(t.role)
}

// releaseBinding probes without the runtime mutex. A concurrent admission from
// zero replaces the binding pointer, invalidating even an idle result obtained
// before that admission both began and returned.
func (r *Runtime) releaseBinding(key string) {
	r.mu.Lock()
	b := r.bindings[key]
	if b == nil || b.admitting != 0 {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	active := b.provider.IsActive(key)
	r.mu.Lock()
	if r.bindings[key] == b && b.admitting == 0 && !active {
		delete(r.bindings, key)
	}
	r.mu.Unlock()
}

func (r *Runtime) sweepBindings() {
	r.mu.Lock()
	keys := make([]string, 0, len(r.bindings))
	for key := range r.bindings {
		keys = append(keys, key)
	}
	r.mu.Unlock()
	for _, key := range keys {
		r.releaseBinding(key)
	}
}

func (t *runtimeTarget) admit(key string) (*binding, error) {
	r := t.runtime
	r.sweepBindings()
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, ErrProviderUnavailable
		}
		b := r.bindings[key]
		if b != nil && b.admitting == 0 {
			r.mu.Unlock()
			active := b.provider.IsActive(key)
			r.mu.Lock()
			if r.bindings[key] != b || b.admitting != 0 {
				r.mu.Unlock()
				continue
			}
			if !active {
				delete(r.bindings, key)
				b = nil
			}
		}
		provider := r.roleLocked(t.role)
		if b != nil {
			provider = b.provider
		}
		if r.providerFencedLocked(provider) {
			r.mu.Unlock()
			return nil, ErrProviderFenced
		}
		if b == nil || b.admitting == 0 {
			b = &binding{provider: provider}
			r.bindings[key] = b
		}
		b.admitting++
		r.mu.Unlock()
		return b, nil
	}
}

// ReserveExecution pins the exact owner through application job admission and
// dispatch. The caller must release on every path; no runtime lock is retained.
// Orchestrator scans use this fail-fast admission: a structural fence leaves
// queued work unclaimed.
func (t *runtimeTarget) ReserveExecution(key string) (ProviderID, func(), error) {
	b, err := t.admit(key)
	if err != nil {
		return "", nil, err
	}
	return t.reserved(key, b)
}

// ReserveExecutionWait admits like an ordinary turn, waiting through a
// structural provider fence with the caller context instead of failing fast.
func (t *runtimeTarget) ReserveExecutionWait(ctx context.Context, key string) (ProviderID, func(), error) {
	b, err := t.waitAdmission(ctx, key)
	if err != nil {
		return "", nil, err
	}
	return t.reserved(key, b)
}

func (t *runtimeTarget) reserved(key string, b *binding) (ProviderID, func(), error) {
	release := func() {
		t.admitted(key, b)
	}
	if s, ok := b.provider.(*Supervisor); ok {
		if err := s.AdmissionError(); err != nil {
			release()
			return "", nil, err
		}
	}
	if available, _ := b.provider.Available(); !available {
		release()
		return "", nil, ErrProviderUnavailable
	}
	return ProviderID(b.provider.HarnessConfig().Name), release, nil
}

// ReserveExecution supports injected operational targets without lifecycle ownership.
func ReserveExecution(target ExecutionTarget, key string) (ProviderID, func(), error) {
	if reserver, ok := target.(interface {
		ReserveExecution(string) (ProviderID, func(), error)
	}); ok {
		return reserver.ReserveExecution(key)
	}
	if a, ok := target.(Availability); ok {
		if ready, _ := a.Available(); !ready {
			return "", nil, ErrProviderUnavailable
		}
	}
	id := ProviderID("")
	if c, ok := target.(interface{ HarnessConfig() HarnessConfig }); ok {
		id = ProviderID(c.HarnessConfig().Name)
	}
	return id, func() {}, nil
}

// ReserveExecutionWait waits through structural fences where interactive chat
// previously waited in Supervisor dispatch, falling back to the fail-fast
// reservation for injected targets without lifecycle ownership.
func ReserveExecutionWait(ctx context.Context, target ExecutionTarget, key string) (ProviderID, func(), error) {
	if reserver, ok := target.(interface {
		ReserveExecutionWait(context.Context, string) (ProviderID, func(), error)
	}); ok {
		return reserver.ReserveExecutionWait(ctx, key)
	}
	return ReserveExecution(target, key)
}

func (t *runtimeTarget) admitted(key string, b *binding) {
	r := t.runtime
	r.mu.Lock()
	b.admitting--
	r.mu.Unlock()
	r.releaseBinding(key)
}

// owner resolves existing logical work independently of the current role map.
// Unbound/inactive keys retain the ordinary current-provider behavior.
func (t *runtimeTarget) owner(key string) (supervisorOperations, error) {
	r := t.runtime
	r.releaseBinding(key)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrProviderUnavailable
	}
	if b := r.bindings[key]; b != nil {
		return b.provider, nil
	}
	return r.roleLocked(t.role), nil
}

func (t *runtimeTarget) Send(ctx context.Context, key, prompt string, emit core.Emit) (string, bool, error) {
	b, err := t.waitAdmission(ctx, key)
	if err != nil {
		return "", false, err
	}
	defer t.admitted(key, b)
	return b.provider.Send(ctx, key, prompt, emit)
}
func (t *runtimeTarget) SendConversation(ctx context.Context, key, prompt, message string, emit core.Emit) (string, bool, error) {
	b, err := t.waitAdmission(ctx, key)
	if err != nil {
		return "", false, err
	}
	defer t.admitted(key, b)
	return b.provider.SendConversation(ctx, key, prompt, message, emit)
}
func (t *runtimeTarget) SendControl(ctx context.Context, key string, request ControlRequest) (ControlResult, error) {
	s, err := t.owner(key)
	if err != nil {
		return ControlResult{}, err
	}
	defer t.runtime.releaseBinding(key)
	return s.SendControl(ctx, key, request)
}
func (t *runtimeTarget) Interrupt(ctx context.Context, key string) (bool, error) {
	s, err := t.owner(key)
	if err != nil {
		return false, err
	}
	defer t.runtime.releaseBinding(key)
	return s.Interrupt(ctx, key)
}
func (t *runtimeTarget) ResetSession(key string) error {
	r := t.runtime
	r.releaseBinding(key)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrProviderUnavailable
	}
	s := r.roleLocked(t.role)
	if b := r.bindings[key]; b != nil {
		s = b.provider
	}
	var entry *providerEntry
	for _, p := range r.providers {
		if p.supervisor == s {
			entry = p
			break
		}
	}
	if entry != nil {
		if entry.fenced {
			r.mu.Unlock()
			return ErrProviderFenced
		}
		entry.operations++
	}
	r.mu.Unlock()
	defer func() {
		if entry != nil {
			r.mu.Lock()
			entry.operations--
			r.mu.Unlock()
		}
		r.releaseBinding(key)
	}()
	return s.ResetSession(key)
}
func (t *runtimeTarget) IsActive(key string) bool {
	s, err := t.owner(key)
	return err == nil && s.IsActive(key)
}
func (t *runtimeTarget) ThreadID(key string) string {
	s, err := t.owner(key)
	if err != nil {
		return ""
	}
	return s.ThreadID(key)
}
func (t *runtimeTarget) ConversationAdmission(key string) string {
	s, err := t.owner(key)
	if err != nil {
		return "new"
	}
	return s.ConversationAdmission(key)
}
func (t *runtimeTarget) usingCurrent() (supervisorOperations, func(), error) {
	r := t.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, ErrProviderUnavailable
	}
	s := r.roleLocked(t.role)
	for _, p := range r.providers {
		if p.supervisor == s {
			if p.fenced {
				return nil, nil, ErrProviderFenced
			}
			p.operations++
			return s, func() { r.mu.Lock(); p.operations--; r.mu.Unlock() }, nil
		}
	}
	return s, func() {}, nil // instrumented ownership fixtures
}
func (t *runtimeTarget) Models(ctx context.Context) ([]Model, error) {
	s, release, err := t.usingCurrent()
	if err != nil {
		return nil, err
	}
	defer release()
	return s.Models(ctx)
}
func (t *runtimeTarget) Available() (bool, string) {
	r := t.runtime
	r.mu.Lock()
	p := r.roleLocked(t.role)
	blocked := r.closed || r.providerFencedLocked(p)
	r.mu.Unlock()
	if blocked {
		return false, ErrProviderFenced.Error()
	}
	return p.Available()
}
func (t *runtimeTarget) ReadyEvents() <-chan struct{}         { return t.runtime.ReadyEvents() }
func (t *runtimeTarget) Readiness() (uint64, <-chan struct{}) { return t.runtime.Readiness() }
func (t *runtimeTarget) CommitInference(selection InferenceSelection, commit func() error) error {
	s, release, err := t.usingCurrent()
	if err != nil {
		return err
	}
	defer release()
	return s.CommitInference(selection, commit)
}
func (t *runtimeTarget) CommitModel(model string, commit func() error) error {
	s, release, err := t.usingCurrent()
	if err != nil {
		return err
	}
	defer release()
	return s.CommitModel(model, commit)
}
func (t *runtimeTarget) HarnessConfig() HarnessConfig { return t.current().HarnessConfig() }
func (t *runtimeTarget) HasActiveTurns() bool         { return t.current().HasActiveTurns() }

func (t *runtimeTarget) waitAdmission(ctx context.Context, key string) (*binding, error) {
	for {
		_, changed := t.runtime.Readiness()
		b, err := t.admit(key)
		if !errors.Is(err, ErrProviderFenced) {
			return b, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// ExecutionProvider reports the pinned owner while a reservation or turn is live.
func (t *runtimeTarget) ExecutionProvider(key string) ProviderID {
	s, err := t.owner(key)
	if err != nil {
		return ""
	}
	return ProviderID(s.HarnessConfig().Name)
}
func ExecutionProvider(target ExecutionTarget, key string) ProviderID {
	if p, ok := target.(interface{ ExecutionProvider(string) ProviderID }); ok {
		return p.ExecutionProvider(key)
	}
	if p, ok := target.(interface{ HarnessConfig() HarnessConfig }); ok {
		return ProviderID(p.HarnessConfig().Name)
	}
	return ""
}
