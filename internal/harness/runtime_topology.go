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
	"sync"
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
	// watchDone closes when this entry's readiness watcher has exited. Every
	// owner that closes stop must join watchDone outside Runtime.mu before
	// considering the entry retired.
	watchDone chan struct{}
}

// closeItem pairs one independent close target with the complete error
// attribution prefix used when wrapping its failures, e.g.
// `close provider "x"`.
type closeItem struct {
	label string
	close func() error
}

// closeConcurrently closes every item in its own goroutine, waits for all of
// them, and aggregates results in deterministic input order rather than
// goroutine completion order. Callers supply sorted input for stable error
// ordering. A panicking close callback is converted into a labeled error so
// one broken target can never abort the remaining shutdown.
func closeConcurrently(items []closeItem) error {
	if len(items) == 0 {
		return nil
	}
	errs := make([]error, len(items))
	var wg sync.WaitGroup
	for index := range items {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					errs[index] = fmt.Errorf("%s: panic during close: %v", items[index].label, recovered)
				}
			}()
			if err := items[index].close(); err != nil {
				errs[index] = fmt.Errorf("%s: %w", items[index].label, err)
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
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
	r.shutdown, r.cancelLife = context.WithCancel(context.Background())
	for id, cfg := range spec.Providers {
		r.providers[id] = &providerEntry{id: id, supervisor: NewSupervisor(registry, cfg), stop: make(chan struct{}), watchDone: make(chan struct{})}
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
		defer close(p.watchDone)
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
	// Provider lifetime work runs under the caller context joined with the
	// runtime-owned shutdown lifetime, so closing the runtime unblocks an
	// in-flight Start as reliably as the caller cancelling its context. The
	// joined lifetime is stored as r.ctx: later Reconcile additions and
	// candidates derive from it, so it must outlive this call. A superseded
	// lifetime is never cancelled here because running supervisors may still
	// derive from it; each stored AfterFunc registration self-releases when
	// shutdown cancels, so repeated Starts cannot leak goroutines.
	lifetime, cancelLifetime := r.joinShutdown(ctx)
	if err := r.begin(lifetime); err != nil {
		cancelLifetime()
		return err
	}
	defer r.end()
	r.mu.Lock()
	r.ctx, r.lifetimeCancel = lifetime, cancelLifetime
	primary := r.supervisor
	entries := make([]*providerEntry, 0, len(r.providers))
	for _, p := range r.providers {
		entries = append(entries, p)
	}
	r.mu.Unlock()
	var primaryErr error
	for _, p := range entries {
		if err := p.supervisor.Start(lifetime); err != nil && p.supervisor == primary {
			primaryErr = err
		}
	}
	return primaryErr
}

// Close is the one whole-runtime shutdown path: it cancels the runtime-owned
// lifetime first so an in-flight Start or Reconcile whose provider work waits
// on its joined context unblocks instead of holding the shutdown forever,
// then obtains the lifecycle reservation and fences admissions. Exactly one
// caller orchestrates; every concurrent or repeated caller returns the stored
// orchestration result. No provider I/O runs under Runtime.mu, watchers are
// joined before return, and independent provider supervisors close
// concurrently so total latency approximates the slowest close, not the sum.
func (r *Runtime) Close() error {
	r.cancelLife()
	r.releaseLifetime()
	for {
		r.mu.Lock()
		if r.lifecycle != nil {
			// A lifecycle reservation is still held (Start, Reconcile, or the
			// one Close orchestration); wait outside the mutex.
			done := r.lifecycle
			r.mu.Unlock()
			<-done
			continue
		}
		if r.closed {
			// The shutdown orchestration already finished; every later or
			// concurrent caller shares its stored result.
			err := r.closeErr
			r.mu.Unlock()
			return err
		}
		r.closed = true
		r.lifecycle = make(chan struct{})
		clear(r.bindings)
		entries := make([]*providerEntry, 0, len(r.providers))
		for _, p := range r.providers {
			entries = append(entries, p)
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })
		for _, p := range entries {
			close(p.stop)
		}
		r.notifyLocked()
		r.mu.Unlock()
		// No Runtime-owned watcher may survive Close; joins happen outside
		// r.mu because the watcher takes it to forward notifications.
		for _, p := range entries {
			<-p.watchDone
		}
		items := make([]closeItem, 0, len(entries))
		for _, p := range entries {
			supervisor := p.supervisor
			items = append(items, closeItem{label: fmt.Sprintf("close provider %q", p.id), close: supervisor.Close})
		}
		err := closeConcurrently(items)
		r.mu.Lock()
		r.closeErr = err
		r.mu.Unlock()
		r.end()
		return err
	}
}

// Reconcile prepares affected providers before atomically publishing the role
// table. Unchanged supervisors retain their process and session state. The
// caller context is joined with the runtime shutdown lifetime before lifecycle
// admission so closing the runtime unblocks candidate startup, inflight
// draining, and any pre-publication wait. Publication stays transactional:
// cleanup failures before publication join the primary failure, while a
// retirement failure after publication reports ErrRetirementIncomplete because
// the published topology remains active and must never be rolled back.
func (r *Runtime) Reconcile(ctx context.Context, spec RuntimeSpec) (err error) {
	spec, err = normalizeSpec(spec)
	if err != nil {
		return err
	}
	ctx, cancelJoin := r.joinShutdown(ctx)
	defer cancelJoin()
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
			// Pre-publication cleanup keeps the primary failure first and
			// joins any cleanup errors after it; publication never happened,
			// so nothing here is ErrRetirementIncomplete.
			var cleanup []error
			for _, name := range sortedProviderIDs(changes) {
				id := ProviderID(name)
				if c := changes[id]; c != nil {
					if abortErr := c.Abort(); abortErr != nil {
						cleanup = append(cleanup, fmt.Errorf("abort candidate of provider %q: %w", id, abortErr))
					}
				}
			}
			for _, name := range sortedProviderIDs(added) {
				id := ProviderID(name)
				if p := added[id]; p != nil {
					if closeErr := p.supervisor.Close(); closeErr != nil {
						cleanup = append(cleanup, fmt.Errorf("close added provider %q: %w", id, closeErr))
					}
				}
			}
			if len(cleanup) > 0 {
				if err != nil {
					err = errors.Join(append([]error{err}, cleanup...)...)
				} else {
					err = errors.Join(cleanup...)
				}
			}
		}
		r.mu.Lock()
		for _, p := range affected {
			p.fenced = false
		}
		r.notifyLocked()
		r.mu.Unlock()
	}()
	ids := sortedProviderIDs(old)
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
		p := &providerEntry{id: id, supervisor: NewSupervisor(r.registry, cfg), stop: make(chan struct{}), watchDone: make(chan struct{})}
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

	if err = ctx.Err(); err != nil {
		return err
	}
	// Commit point: replacements publish in sorted provider order and hand
	// each previous harness back for retirement outside locks.
	retired := make(map[ProviderID]Harness)
	for _, name := range sortedProviderIDs(changes) {
		id := ProviderID(name)
		if previous := changes[id].Commit(); previous != nil {
			retired[id] = previous
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
	if retireErr := r.retire(old, next, retired); retireErr != nil {
		return errors.Join(ErrRetirementIncomplete, retireErr)
	}
	return nil
}

func sortedProviderIDs[V any](values map[ProviderID]V) []string {
	ids := make([]string, 0, len(values))
	for id := range values {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	return ids
}

// retire shuts down ownership that stopped being part of the published
// topology: removed providers' supervisors and the previous harnesses handed
// back by structural commits. The targets are independent, so they close
// concurrently; each has exactly one retirement owner here. A removed
// provider's watcher is joined (outside Runtime.mu) after its stop is closed
// and before its supervisor closes, so no watcher survives retirement.
func (r *Runtime) retire(old, next map[ProviderID]*providerEntry, retired map[ProviderID]Harness) error {
	ids := sortedProviderIDs(old)
	items := make([]closeItem, 0, len(ids)+len(retired))
	for _, name := range ids {
		id := ProviderID(name)
		if next[id] != nil {
			continue
		}
		p := old[id]
		items = append(items, closeItem{
			label: fmt.Sprintf("retire provider %q", id),
			close: func() error {
				close(p.stop)
				<-p.watchDone
				return p.supervisor.Close()
			},
		})
	}
	for _, name := range sortedProviderIDs(retired) {
		id := ProviderID(name)
		previous := retired[id]
		items = append(items, closeItem{
			label: fmt.Sprintf("retire previous harness of provider %q", id),
			close: func() error { return previous.Close() },
		})
	}
	return closeConcurrently(items)
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
