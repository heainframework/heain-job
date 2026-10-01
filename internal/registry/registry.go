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
//
// Stale-SubUnit tracking (2026-10-02, closes "heain-job: stale-replica
// failover" — a replica that stops reporting progress on one specific
// SubUnit it claimed is reassigned to another live replica, rather than
// the orchestrator waiting on it forever): each SubUnit a replica is
// actively working gets its own last-activity timestamp, started the
// moment the orchestrator assigns it (AssignSubUnit) and refreshed on
// every /progress report (ReportProgress) -- not just the replica's own
// overall lastSeen, which only proves the replica process itself is
// alive, not that this one SubUnit is making progress.
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

type subUnitState struct {
	percentComplete float64
	lastActivity    time.Time
}

type entry struct {
	endpoint jobapp.ModuleEndpoint
	lastSeen time.Time
	// currentLoad maps a SubUnitID this replica is actively executing to
	// its last-known percent-complete and last-activity time. Only
	// entries for RUNNING sub-units are present -- DONE/FAILED/cleared
	// removes the key.
	currentLoad map[string]subUnitState
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

	currentLoad := map[string]subUnitState{}
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
			load[k] = v.percentComplete
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
// re-registration would, and refreshes that SubUnit's own last-activity
// timestamp (see SubUnitActivity). Returns ErrNotRegistered if the
// (strategy, replica) pair isn't known.
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
		e.currentLoad = map[string]subUnitState{}
	}
	switch status {
	case jobapp.ProgressDone, jobapp.ProgressFailed:
		delete(e.currentLoad, subUnitID)
	default:
		e.currentLoad[subUnitID] = subUnitState{percentComplete: percentComplete, lastActivity: time.Now()}
	}
	e.lastSeen = time.Now()
	replicas[replicaID] = e
	return nil
}

// AssignSubUnit records that a SubUnit has just been dispatched to a
// replica, starting its staleness clock immediately -- without this, a
// replica that crashes before ever sending its first /progress report
// would have no activity record at all, and SubUnitActivity would never
// be able to tell "never started" apart from "started and stalled".
// Call this once, right before sending the /process-subunit request.
// A no-op (returns ErrNotRegistered) if the (strategy, replica) pair
// isn't known -- callers that already looked the replica up via
// LookupPool/Lookup won't normally hit this.
func (r *Registry) AssignSubUnit(strategyName, replicaID, subUnitID string) error {
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
		e.currentLoad = map[string]subUnitState{}
	}
	if _, already := e.currentLoad[subUnitID]; !already {
		e.currentLoad[subUnitID] = subUnitState{percentComplete: 0, lastActivity: time.Now()}
		replicas[replicaID] = e
	}
	return nil
}

// ClearSubUnit removes a SubUnit from a replica's live load, regardless
// of outcome -- used by the orchestrator once a SubUnit has been
// reassigned away from this replica, or once its own /process-subunit
// call has returned (success or failure), so a completed/abandoned
// SubUnit never continues to count toward that replica's assumed load
// or get flagged stale after the fact.
func (r *Registry) ClearSubUnit(strategyName, replicaID, subUnitID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	replicas, ok := r.endpoints[strategyName]
	if !ok {
		return
	}
	e, ok := replicas[replicaID]
	if !ok {
		return
	}
	delete(e.currentLoad, subUnitID)
	replicas[replicaID] = e
}

// SubUnitActivity returns when a specific (strategy, replica, subUnit)
// last showed activity -- either its initial AssignSubUnit call or its
// most recent ReportProgress -- and whether that SubUnit is currently
// tracked at all for that replica (false once it's DONE/FAILED/cleared,
// or if it was never assigned there in the first place). Used by the
// orchestrator's staleness watcher to detect a SubUnit whose replica has
// stopped making progress on it specifically, independent of whether
// that replica's own overall registration (lastSeen) still looks fresh.
func (r *Registry) SubUnitActivity(strategyName, replicaID, subUnitID string) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	replicas, ok := r.endpoints[strategyName]
	if !ok {
		return time.Time{}, false
	}
	e, ok := replicas[replicaID]
	if !ok {
		return time.Time{}, false
	}
	st, ok := e.currentLoad[subUnitID]
	if !ok {
		return time.Time{}, false
	}
	return st.lastActivity, true
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
