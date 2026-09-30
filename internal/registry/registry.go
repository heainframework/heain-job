// Package registry holds heain-job's own module registration bookkeeping —
// which data-type module implements which split/merge strategy, and how to
// reach it. This is Layer 3 service-local state, deliberately NOT
// Raft-replicated: it is not part of heain-core's Layer 2 consensus
// substrate, matching the same Layer 2/Layer 3 boundary already
// established for heain-job's own separate audit log (see
// internal/jobapp.AuditRecord's docs).
package registry

import (
	"errors"
	"sync"
	"time"

	"github.com/heainframework/heain-job/internal/jobapp"
)

// ErrNotRegistered is returned when no module is registered for a strategy.
var ErrNotRegistered = errors.New("registry: no module registered for this strategy")

// ErrAlreadyRegistered is returned by Register when the strategy name is
// already claimed by a different module (a module re-registering with the
// same name/base URL/token is treated as a refresh, not a conflict).
var ErrAlreadyRegistered = errors.New("registry: strategy name already registered to a different module")

// ErrStale is returned by Lookup when a module was registered but hasn't
// re-registered ("heartbeated") within the registry's TTL. It is treated
// as equivalent to not-registered by every caller: module-restart-mid-job
// is deliberately not special-cased beyond this -- a stale entry fails
// fast here instead of only being discovered after a callModule retry
// budget is exhausted, and recovery from there is the existing
// checkpoint/resume pattern, not new machinery.
var ErrStale = errors.New("registry: module registration expired (no re-registration within TTL); treating as unregistered")

// DefaultTTL is how long a registration stays valid without a
// re-registration before Lookup treats it as gone. heain-sdk's jobclient
// re-registers every DefaultReregisterInterval (15s), well inside this
// 30s window, so a healthy module is never mistaken for stale.
const DefaultTTL = 30 * time.Second

type entry struct {
	endpoint jobapp.ModuleEndpoint
	lastSeen time.Time
}

// Registry is a thread-safe, in-memory registration store. A production
// deployment may back this with a small local store for restart-durability
// (matching the same "operator needs this to survive a crash" reasoning
// already applied to heain-core's Duty Profile and pending-approval state),
// but v1 ships in-memory only — modules re-register on their own restart.
type Registry struct {
	mu        sync.RWMutex
	endpoints map[string]entry // keyed by StrategyName
	ttl       time.Duration
}

// New returns an empty Registry using DefaultTTL.
func New() *Registry {
	return NewWithTTL(DefaultTTL)
}

// NewWithTTL returns an empty Registry with a custom staleness TTL,
// mainly so tests can exercise expiry without waiting DefaultTTL in
// real time.
func NewWithTTL(ttl time.Duration) *Registry {
	return &Registry{endpoints: make(map[string]entry), ttl: ttl}
}

// Register adds or refreshes ("heartbeats") a module's endpoint for a
// given strategy name.
func (r *Registry) Register(ep jobapp.ModuleEndpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.endpoints[ep.StrategyName]; ok && existing.endpoint.ModuleName != ep.ModuleName {
		return ErrAlreadyRegistered
	}
	r.endpoints[ep.StrategyName] = entry{endpoint: ep, lastSeen: time.Now()}
	return nil
}

// Deregister removes a strategy's registration, e.g. on graceful module
// shutdown.
func (r *Registry) Deregister(strategyName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.endpoints, strategyName)
}

// Lookup returns the registered endpoint for a strategy name. It returns
// ErrStale, not the endpoint, if the module hasn't re-registered within
// the registry's TTL.
func (r *Registry) Lookup(strategyName string) (jobapp.ModuleEndpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	e, ok := r.endpoints[strategyName]
	if !ok {
		return jobapp.ModuleEndpoint{}, ErrNotRegistered
	}
	if time.Since(e.lastSeen) > r.ttl {
		return jobapp.ModuleEndpoint{}, ErrStale
	}
	return e.endpoint, nil
}

// All returns every currently-registered (non-expired-filtered) endpoint,
// for diagnostics/health reporting.
func (r *Registry) All() []jobapp.ModuleEndpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]jobapp.ModuleEndpoint, 0, len(r.endpoints))
	for _, e := range r.endpoints {
		out = append(out, e.endpoint)
	}
	return out
}
