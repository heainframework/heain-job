// Package registry holds heain-job's own module registration bookkeeping —
// which data-type module implements which split/merge strategy, and how to
// reach it. This is Layer 3 service-local state, deliberately NOT
// Raft-replicated: it is not part of heain-core's Layer 2 consensus
// substrate, matching the same Layer 2/Layer 3 boundary already
// established for heain-job's own separate audit log (see
// internal/jobapp.AuditRecord's docs).
//
// Stage B (2026-10-01, see design-notes/n-tier-generalization.md
// "heain-job's load-aware scheduler"): a strategy name can now have more
// than one live replica registered under it at once (keyed by each
// replica's own ReplicaID), not just exactly one endpoint. This is an
// additive change: a module that never sets ReplicaID (the original
// single-instance case, e.g. heain-image) registers under the
// empty-string key, and Lookup's "any one live replica, deterministically
// -- lowest replica-id" rule degenerates to exactly the old
// single-endpoint behavior for it.
package registry

import (
	"errors"
	"sort"
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
	// currentLoad maps a SubUnitID this replica is actively executing to
	// its last-reported percent-complete (0-100). Only entries for
	// RUNNING sub-units are present -- DONE/FAILED clears the key.
	currentLoad map[string]float64
}

// ReplicaInfo is a live, non-stale replica returned by LookupPool,
// carrying enough of its current state for a scheduler to score it.
type ReplicaInfo struct {
	Endpoint    jobapp.ModuleEndpoint
	CurrentLoad map[string]float64 // SubUnitID -> percent complete (0-100)
}

// Registry is a thread-safe, in-memory registration store. A production
// deployment may back this with a small local store for restart-durability
// (matching the same "operator needs this to survive a crash" reasoning
// already applied to heain-core's Duty Profile and pending-approval state),
// but v1 ships in-memory only — modules re-register on their own restart.
type Registry struct {
	mu sync.RWMutex
	// endpoints[strategyName][replicaID] holds that replica's entry.
	endpoints map[string]map[string]entry
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
	return &Registry{endpoints: make(map[string]map[string]entry), ttl: ttl}
}

// Register adds or refreshes ("heartbeats") one replica's endpoint for a
// given strategy name. Two replicas of the same strategy must share the
// same ModuleName -- a different module trying to claim an
// already-registered strategy name is rejected with ErrAlreadyRegistered,
// regardless of replica pool size.
func (r *Registry) Register(ep jobapp.ModuleEndpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	replicas, ok := r.endpoints[ep.StrategyName]
	if !ok {
		replicas = make(map[string]entry)
		r.endpoints[ep.StrategyName] = replicas
	}
	for _, e := range replicas {
		if e.endpoint.ModuleName != ep.ModuleName {
			return ErrAlreadyRegistered
		}
		break // every existing replica shares one ModuleName by this same invariant
	}

	currentLoad := map[string]float64{}
	if existing, had := replicas[ep.ReplicaID]; had {
		currentLoad = existing.currentLoad
	}
	replicas[ep.ReplicaID] = entry{endpoint: ep, lastSeen: time.Now(), currentLoad: currentLoad}
	return nil
}

// Deregister removes the default (empty-ReplicaID) replica's registration
// for a strategy -- the original single-instance signature, kept
// unchanged for every existing caller (e.g. heain-image, heain-sdk's
// jobclient.Client.Deregister). Equivalent to DeregisterReplica(strategyName, "").
func (r *Registry) Deregister(strategyName string) {
	r.DeregisterReplica(strategyName, "")
}

// DeregisterReplica removes one specific replica's registration for a
// strategy (Stage B), e.g. on that replica's graceful shutdown, without
// affecting any of the strategy's other live replicas.
func (r *Registry) DeregisterReplica(strategyName, replicaID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	replicas, ok := r.endpoints[strategyName]
	if !ok {
		return
	}
	delete(replicas, replicaID)
	if len(replicas) == 0 {
		delete(r.endpoints, strategyName)
	}
}

// Lookup returns "any one live replica, deterministically -- lowest
// replica-id" for a strategy. For a strategy with exactly one replica
// (registered under the empty-string ReplicaID, the original
// single-instance case), this is exactly the old single-endpoint
// behavior, unchanged. Returns ErrStale only when every replica for the
// strategy has gone stale; ErrNotRegistered when none are registered at
// all.
func (r *Registry) Lookup(strategyName string) (jobapp.ModuleEndpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	replicas, ok := r.endpoints[strategyName]
	if !ok || len(replicas) == 0 {
		return jobapp.ModuleEndpoint{}, ErrNotRegistered
	}

	ids := make([]string, 0, len(replicas))
	for id := range replicas {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	now := time.Now()
	for _, id := range ids {
		e := replicas[id]
		if now.Sub(e.lastSeen) <= r.ttl {
			return e.endpoint, nil
		}
	}
	return jobapp.ModuleEndpoint{}, ErrStale
}

// LookupReplica returns one specific replica's endpoint, for callers
// (e.g. the /progress handler) that must authenticate a particular
// replica rather than accept any live one. Returns ErrNotRegistered if
// that (strategy, replica) pair is unknown or has gone stale.
func (r *Registry) LookupReplica(strategyName, replicaID string) (jobapp.ModuleEndpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	replicas, ok := r.endpoints[strategyName]
	if !ok {
		return jobapp.ModuleEndpoint{}, ErrNotRegistered
	}
	e, ok := replicas[replicaID]
	if !ok {
		return jobapp.ModuleEndpoint{}, ErrNotRegistered
	}
	if time.Since(e.lastSeen) > r.ttl {
		return jobapp.ModuleEndpoint{}, ErrStale
	}
	return e.endpoint, nil
}

// LookupPool returns every currently-live (non-stale) replica registered
// for a strategy, for Stage B's fan-out scheduler (internal/scheduler).
// A nil/empty result means no live replica exists for this strategy.
func (r *Registry) LookupPool(strategyName string) []ReplicaInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	replicas, ok := r.endpoints[strategyName]
	if !ok {
		return nil
	}
	now := time.Now()
	out := make([]ReplicaInfo, 0, len(replicas))
	for _, e := range replicas {
		if now.Sub(e.lastSeen) > r.ttl {
			continue
		}
		load := make(map[string]float64, len(e.currentLoad))
		for k, v := range e.currentLoad {
			load[k] = v
		}
		out = append(out, ReplicaInfo{Endpoint: e.endpoint, CurrentLoad: load})
	}
	return out
}

// ReportProgress records one replica's self-reported progress on one
// SubUnit (Stage B). A RUNNING report updates currentLoad[subUnitID]; a
// DONE or FAILED report clears it, since the SubUnit is no longer part
// of that replica's live load either way. A progress report also counts
// as a liveness signal, refreshing lastSeen exactly like a
// re-registration would. Returns ErrNotRegistered if the (strategy,
// replica) pair isn't known.
func (r *Registry) ReportProgress(strategyName, replicaID, subUnitID string, percentComplete float64, status jobapp.ProgressStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	replicas, ok := r.endpoints[strategyName]
	if !ok {
		return ErrNotRegistered
	}
	e, ok := replicas[replicaID]
	if !ok {
		return ErrNotRegistered
	}
	if e.currentLoad == nil {
		e.currentLoad = map[string]float64{}
	}
	switch status {
	case jobapp.ProgressDone, jobapp.ProgressFailed:
		delete(e.currentLoad, subUnitID)
	default:
		e.currentLoad[subUnitID] = percentComplete
	}
	e.lastSeen = time.Now()
	replicas[replicaID] = e
	return nil
}

// All returns every currently-registered (non-expired-filtered) endpoint,
// one per replica, for diagnostics/health reporting.
func (r *Registry) All() []jobapp.ModuleEndpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]jobapp.ModuleEndpoint, 0)
	for _, replicas := range r.endpoints {
		for _, e := range replicas {
			out = append(out, e.endpoint)
		}
	}
	return out
}
