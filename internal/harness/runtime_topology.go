package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

// ProviderID is the normalized provider instance identity from the topology
// map key. It is never a profile or generation. HarnessConfig.Name remains the
// harness/catalog kind, so multiple provider instances may share one kind.
type ProviderID string

type RuntimeSpec struct {
	Providers map[ProviderID]HarnessConfig
	Roles     map[Role]ProviderID
}

type providerEntry struct {
	id         ProviderID
	supervisor *Supervisor
	operations int
	fenced     bool // structural work or retirement; protected by Runtime.mu
	stop       chan struct{}
}

func normalizeSpec(spec RuntimeSpec) (RuntimeSpec, error) {
	out := RuntimeSpec{Providers: make(map[ProviderID]HarnessConfig), Roles: make(map[Role]ProviderID)}
	paths := make(map[string]ProviderID)
	chatID := ProviderID(strings.ToLower(strings.TrimSpace(string(spec.Roles[RoleChat]))))
	for id, cfg := range spec.Providers {
		id = ProviderID(strings.ToLower(strings.TrimSpace(string(id))))
		cfg.Name = strings.ToLower(strings.TrimSpace(cfg.Name))
		// An identity may only stay empty as the legacy no-harness-selected
		// marker (empty instance, empty kind) so a harness-less service stays
		// constructible; every other empty side is an explicit error.
		if id == "" && cfg.Name != "" {
			return out, errors.New("provider identity is empty")
		}
		if id != "" && cfg.Name == "" {
			return out, fmt.Errorf("provider %q has an empty harness name", id)
		}
		if _, ok := out.Providers[id]; ok {
			return out, fmt.Errorf("duplicate provider: %q", id)
		}
		if cfg.SessionsFile != "" {
			path, err := sessionOwnerPath(cfg.SessionsFile)
			if err != nil {
				return out, err
			}
			path = filepath.Clean(path)
			if other, ok := paths[path]; ok {
				return out, fmt.Errorf("providers %q and %q share a session file", other, id)
			}
			paths[path] = id
		}
		if cfg.Name == "acp" && chatID != id && strings.TrimSpace(cfg.Command) == "" {
			return out, errors.New("custom ACP requires a command")
		}
		cfg.Args = append([]string(nil), cfg.Args...)
		cfg.Env = append([]string(nil), cfg.Env...)
		out.Providers[id] = cfg
	}
	for role, id := range spec.Roles {
		id = ProviderID(strings.ToLower(strings.TrimSpace(string(id))))
		if _, ok := out.Providers[id]; !ok {
			return out, fmt.Errorf("role %q selects absent provider %q", role, id)
		}
		out.Roles[role] = id
	}
	if _, ok := out.Roles[RoleChat]; !ok {
		return out, errors.New("chat provider is required")
	}
	return out, nil
}

func NewRuntimeSpec(registry *Registry, spec RuntimeSpec) (*Runtime, error) {
	spec, err := normalizeSpec(spec)
	if err != nil {
		return nil, err
	}
	r := &Runtime{registry: registry, providers: make(map[ProviderID]*providerEntry), roles: make(map[Role]providerRoute), targets: make(map[Role]*runtimeTarget), bindings: make(map[string]*binding), changed: make(chan struct{}), ready: make(chan struct{}, 1)}
	for id, cfg := range spec.Providers {
		r.providers[id] = &providerEntry{id: id, supervisor: NewSupervisor(registry, cfg), stop: make(chan struct{})}
	}
	r.publishRolesLocked(spec.Roles)
	for _, p := range r.providers {
		r.watch(p)
	}
	return r, nil
}
func (r *Runtime) publishRolesLocked(roles map[Role]ProviderID) {
	r.roles = make(map[Role]providerRoute, len(roles))
	for role, id := range roles {
		r.roles[role] = providerRoute{id: id, provider: r.providers[id].supervisor}
	}
	r.supervisor = r.providers[roles[RoleChat]].supervisor
}
func (r *Runtime) notifyLocked() {
	r.version++
	close(r.changed)
	r.changed = make(chan struct{})
	select {
	case r.ready <- struct{}{}:
	default:
	}
}
func (r *Runtime) watch(p *providerEntry) {
	_, changed := p.supervisor.Readiness()
	go func() {
		for {
			select {
			case <-p.stop:
				return
			case <-changed:
			}
			_, changed = p.supervisor.Readiness()
			r.mu.Lock()
			if !r.closed {
				r.notifyLocked()
			}
			r.mu.Unlock()
		}
	}()
}
func (r *Runtime) begin(ctx context.Context) error {
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return ErrProviderUnavailable
		}
		if r.lifecycle == nil {
			r.lifecycle = make(chan struct{})
			r.mu.Unlock()
			return nil
		}
		done := r.lifecycle
		r.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (r *Runtime) end() {
	r.mu.Lock()
	close(r.lifecycle)
	r.lifecycle = nil
	r.notifyLocked()
	r.mu.Unlock()
}
func (r *Runtime) Start(ctx context.Context) error {
	if err := r.begin(ctx); err != nil {
		return err
	}
	defer r.end()
	r.mu.Lock()
	r.ctx = ctx
	primary := r.supervisor
	entries := make([]*providerEntry, 0, len(r.providers))
	for _, p := range r.providers {
		entries = append(entries, p)
	}
	r.mu.Unlock()
	var primaryErr error
	for _, p := range entries {
		if err := p.supervisor.Start(ctx); err != nil && p.supervisor == primary {
			primaryErr = err
		}
	}
	return primaryErr
}
func (r *Runtime) Close() error {
	// Wait on a reservation, never while holding the runtime mutex.
	for {
		r.mu.Lock()
		if r.lifecycle != nil {
			done := r.lifecycle
			r.mu.Unlock()
			<-done
			continue
		}
		if r.closed {
			r.mu.Unlock()
			return nil
		}
		r.closed = true
		r.lifecycle = make(chan struct{})
		clear(r.bindings)
		entries := make([]*providerEntry, 0, len(r.providers))
		for _, p := range r.providers {
			close(p.stop)
			entries = append(entries, p)
		}
		r.notifyLocked()
		r.mu.Unlock()
		var errs []error
		for _, p := range entries {
			errs = append(errs, p.supervisor.Close())
		}
		r.end()
		return errors.Join(errs...)
	}
}

// Reconcile prepares affected providers before atomically publishing the role
// table. Unchanged supervisors retain their process and session state.
func (r *Runtime) Reconcile(ctx context.Context, spec RuntimeSpec) error {
	spec, err := normalizeSpec(spec)
	if err != nil {
		return err
	}
	if err = r.begin(ctx); err != nil {
		return err
	}
	defer r.end()
	r.mu.Lock()
	old := make(map[ProviderID]*providerEntry, len(r.providers))
	for id, p := range r.providers {
		old[id] = p
	}
	lifetime := r.ctx
	r.mu.Unlock()
	primaryAvailable, _ := r.Available()
	// A new identity cannot open a session file still owned by an old entry,
	// even when that entry is scheduled for removal in this reconciliation.
	for id, p := range old {
		file := p.supervisor.HarnessConfig().SessionsFile
		if file == "" {
			continue
		}
		oldPath, e := sessionOwnerPath(file)
		if e != nil {
			return e
		}
		for nextID, cfg := range spec.Providers {
			if nextID == id || cfg.SessionsFile == "" {
				continue
			}
			newPath, e := sessionOwnerPath(cfg.SessionsFile)
			if e != nil {
				return e
			}
			if oldPath == newPath {
				return fmt.Errorf("provider %q still owns session file selected by %q", id, nextID)
			}
		}
	}
	next := make(map[ProviderID]*providerEntry, len(spec.Providers))
	changes := make(map[ProviderID]*PreparedChange)
	affected := make(map[ProviderID]*providerEntry)
	added := make(map[ProviderID]*providerEntry)
	committed := false
	defer func() {
		if !committed {
			for _, c := range changes {
				_ = c.Abort()
			}
			for _, p := range added {
				_ = p.supervisor.Close()
			}
		}
		r.mu.Lock()
		for _, p := range affected {
			p.fenced = false
		}
		r.notifyLocked()
		r.mu.Unlock()
	}()
	ids := make([]string, 0, len(old))
	for id := range old {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, name := range ids {
		id := ProviderID(name)
		p := old[id]
		cfg, exists := spec.Providers[id]
		current := p.supervisor.HarnessConfig()
		// Logging sinks are composition dependencies, not structural settings.
		cfg.Stderr = current.Stderr
		if exists && reflect.DeepEqual(cfg, current) {
			next[id] = p
			continue
		}
		affected[id] = p
	}
	// Fence under the same synchronization as admissions, then probe logical
	// work without mu. Any admission already in progress rejects the change.
	r.mu.Lock()
	for _, p := range affected {
		p.fenced = true
	}
	r.notifyLocked()
	r.mu.Unlock()
	r.sweepBindings()
	r.mu.Lock()
	busy := false
	for _, p := range affected {
		if p.operations > 0 {
			busy = true
		}
	}
	for _, b := range r.bindings {
		for _, p := range affected {
			if b.provider == p.supervisor {
				busy = true
			}
		}
	}
	r.mu.Unlock()
	if busy {
		return fmt.Errorf("%w: provider has an owned execution", ErrProviderFenced)
	}
	for _, name := range ids {
		id := ProviderID(name)
		p := affected[id]
		if p == nil {
			continue
		}
		if p.supervisor.HasActiveTurns() {
			return fmt.Errorf("%w: provider has active turns", ErrProviderFenced)
		}
		cfg, exists := spec.Providers[id]
		if !exists {
			continue
		}
		c, e := p.supervisor.PrepareChange(ctx, cfg)
		if e != nil {
			return e
		}
		changes[id] = c
		next[id] = p
	}
	for id, cfg := range spec.Providers {
		if next[id] != nil {
			continue
		}
		p := &providerEntry{id: id, supervisor: NewSupervisor(r.registry, cfg), stop: make(chan struct{})}
		added[id] = p
		next[id] = p
		if lifetime != nil {
			e := p.supervisor.Start(lifetime)
			if e != nil && id == spec.Roles[RoleChat] && primaryAvailable {
				return e
			}
		}
	}
	// Retry unavailable entries without reconstructing healthy providers.
	if lifetime != nil {
		for id, p := range next {
			if old[id] != p || changes[id] != nil {
				continue
			}
			if ok, _ := p.supervisor.Available(); ok {
				continue
			}
			r.mu.Lock()
			p.fenced = true
			affected[id] = p
			r.notifyLocked()
			r.mu.Unlock()
			r.sweepBindings()
			r.mu.Lock()
			busy := p.operations > 0
			for _, b := range r.bindings {
				if b.provider == p.supervisor {
					busy = true
				}
			}
			r.mu.Unlock()
			if busy || p.supervisor.HasActiveTurns() {
				continue
			}
			c, e := p.supervisor.PrepareChange(ctx, spec.Providers[id])
			if e == nil {
				changes[id] = c
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	var retired []Harness
	for _, c := range changes {
		if previous := c.Commit(); previous != nil {
			retired = append(retired, previous)
		}
	}
	r.mu.Lock()
	r.providers = next
	r.publishRolesLocked(spec.Roles)
	r.notifyLocked()
	r.mu.Unlock()
	committed = true
	for _, p := range added {
		r.watch(p)
	}
	for id, p := range old {
		if next[id] == nil {
			close(p.stop)
			_ = p.supervisor.Close()
		}
	}
	for _, h := range retired {
		_ = h.Close()
	}
	return nil
}

func (r *Runtime) providerFencedLocked(s supervisorOperations) bool {
	for _, p := range r.providers {
		if p.supervisor == s {
			return p.fenced
		}
	}
	return false
}

// Resolve existing ancestors as well as the leaf so aliases of not-yet-created
// session files cannot produce two lifecycle owners.
func sessionOwnerPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	cursor := filepath.Clean(absolute)
	var tail []string
	for {
		resolved, err := filepath.EvalSymlinks(cursor)
		if err == nil {
			for i := len(tail) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, tail[i])
			}
			return resolved, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(cursor)
		if parent == cursor {
			return "", err
		}
		tail = append(tail, filepath.Base(cursor))
		cursor = parent
	}
}
