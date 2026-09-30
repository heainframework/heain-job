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

	"github.com/heainframework/heain-job/internal/jobapp"
)

// ErrNotRegistered is returned when no module is registered for a strategy.
var ErrNotRegistered = errors.New("registry: no module registered for this strategy")

// ErrAlreadyRegistered is returned by Register when the strategy name is
// already claimed by a different module (a module re-registering with the
// same name/base URL/token is treated as a refresh, not a conflict).
var ErrAlreadyRegistered = errors.New("registry: strategy name already registered to a different module")

// Registry is a thread-safe, in-memory registration store. A production
// deployment may back this with a small local store for restart-durability
// (matching the same "operator needs this to survive a crash" reasoning
// already applied to heain-core's Duty Profile and pending-approval state),
// but v1 ships in-memory only — modules re-register on their own restart.
type Registry struct {
	mu        sync.RWMutex
	endpoints map[string]jobapp.ModuleEndpoint // keyed by StrategyName
}

// New returns an empty Registry.
func New() *Registry {
	return &Registry{endpoints: make(map[string]jobapp.ModuleEndpoint)}
}

// Register adds or refreshes a module's endpoint for a given strategy name.
func (r *Registry) Register(ep jobapp.ModuleEndpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.endpoints[ep.StrategyName]; ok && existing.ModuleName != ep.ModuleName {
		return ErrAlreadyRegistered
	}
	r.endpoints[ep.StrategyName] = ep
	return nil
}

// Deregister removes a strategy's registration, e.g. on graceful module
// shutdown.
func (r *Registry) Deregister(strategyName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.endpoints, strategyName)
}

// Lookup returns the registered endpoint for a strategy name.
func (r *Registry) Lookup(strategyName string) (jobapp.ModuleEndpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ep, ok := r.endpoints[strategyName]
	if !ok {
		return jobapp.ModuleEndpoint{}, ErrNotRegistered
	}
	return ep, nil
}

// All returns every currently-registered endpoint, for diagnostics/health
// reporting.
func (r *Registry) All() []jobapp.ModuleEndpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]jobapp.ModuleEndpoint, 0, len(r.endpoints))
	for _, ep := range r.endpoints {
		out = append(out, ep)
	}
	return out
}
