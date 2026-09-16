package harness

import (
	"context"

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
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrProviderUnavailable
	}
	b := r.bindings[key]
	if b == nil {
		b = &binding{provider: r.roleLocked(t.role)}
		r.bindings[key] = b
	} else if b.admitting == 0 {
		b = &binding{provider: b.provider}
		r.bindings[key] = b
	}
	b.admitting++
	return b, nil
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
	b, err := t.admit(key)
	if err != nil {
		return "", false, err
	}
	defer t.admitted(key, b)
	return b.provider.Send(ctx, key, prompt, emit)
}
func (t *runtimeTarget) SendConversation(ctx context.Context, key, prompt, message string, emit core.Emit) (string, bool, error) {
	b, err := t.admit(key)
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
	s, err := t.owner(key)
	if err != nil {
		return err
	}
	defer t.runtime.releaseBinding(key)
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
func (t *runtimeTarget) Models(ctx context.Context) ([]Model, error) { return t.current().Models(ctx) }
func (t *runtimeTarget) Available() (bool, string)                   { return t.current().Available() }
func (t *runtimeTarget) ReadyEvents() <-chan struct{}                { return t.current().ReadyEvents() }
func (t *runtimeTarget) Readiness() (uint64, <-chan struct{})        { return t.current().Readiness() }
func (t *runtimeTarget) CommitInference(selection InferenceSelection, commit func() error) error {
	return t.current().CommitInference(selection, commit)
}
func (t *runtimeTarget) CommitModel(model string, commit func() error) error {
	return t.current().CommitModel(model, commit)
}
func (t *runtimeTarget) HarnessConfig() HarnessConfig { return t.current().HarnessConfig() }
func (t *runtimeTarget) HasActiveTurns() bool         { return t.current().HasActiveTurns() }
