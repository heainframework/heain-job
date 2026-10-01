// Package scheduler implements heain-job's Stage B load-aware SubUnit
// assignment (see design-notes/n-tier-generalization.md, "heain-job's
// load-aware scheduler: assigning each SubUnit to the best replica").
//
// v1 is a simple deterministic score, not AI-assisted -- matching
// heain-core's own P2 Dispatcher ranking precedent for the same reason
// recorded there: keeps this package independently testable without a
// dependency on an AI-advisor layer. Once Stage B is live and running
// real jobs, every scheduling decision and its real outcome is logged;
// that log becomes the training data for a regression model that later
// replaces this deterministic score behind the same Assign interface,
// following heain-image's own two-pass precedent (a deterministic
// function proven correct first, swapped for a trained model once real
// data exists to train on, with no caller-visible change) -- see
// "Stage B scheduler: deterministic first, AI-assisted regression model
// as an explicit follow-up" in the design note. Training that model is
// explicitly out of scope until Stage B is live and has produced real
// logs.
package scheduler

import (
	"sort"

	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
)

// CapacityHeadroom reports a replica's current spare capacity, higher
// meaning more headroom. A caller typically derives this from the same
// live RAM/GPU/CPU query heain-core's own P2 Dispatcher already uses
// (its real /capacity endpoint) -- Assign takes it as a plain input
// rather than fetching it itself, so this package has no HTTP/network
// dependency of its own and stays unit-testable with synthetic numbers.
type CapacityHeadroom func(ep jobapp.ModuleEndpoint) float64

// Assignment is one SubUnit's chosen replica.
type Assignment struct {
	SubUnit jobapp.SubUnit
	Replica jobapp.ModuleEndpoint
}

// Assign picks one replica per SubUnit from pool, using:
//
//	score(replica) = capacityHeadroom(replica) - sum(percent_remaining(s) for s in replica.CurrentLoad)
//
// where percent_remaining(s) = 100 - percent_complete(s). A replica deep
// into finishing its current chunks scores higher (lighter effective
// load) than one that just started, even at identical raw capacity
// headroom -- this is what lets the scheduler avoid piling new chunks
// onto an already-busy replica while an almost-done one sits idle a
// moment later, per the author's own requirement that drove this
// design. The highest-scoring replica is assigned each SubUnit in turn
// (its assumed load is updated in-memory between picks within this one
// call, so a pool smaller than len(subUnits) spreads chunks round-robin
// by current score rather than piling every chunk onto whichever
// replica scored highest at the very start). Ties break on the lowest
// replica-id, matching every other tie-break convention already used in
// this codebase (e.g. the dispatch ranking's worker-id final tiebreak).
//
// pool must be non-empty -- callers check that themselves (e.g. via
// registry.Registry.LookupPool) and decide what "no live replica" means
// for their own job, since that's an orchestration-level decision, not
// a scheduling one.
func Assign(subUnits []jobapp.SubUnit, pool []registry.ReplicaInfo, headroom CapacityHeadroom) []Assignment {
	// Work on a local copy of each replica's assumed remaining load, so
	// successive picks within this one call see the effect of earlier
	// picks without mutating the caller's pool.
	type candidate struct {
		ep               jobapp.ModuleEndpoint
		baseHeadroom     float64
		assumedRemaining float64 // sum of percent_remaining across real + provisionally-assigned sub-units
	}
	candidates := make([]candidate, 0, len(pool))
	for _, r := range pool {
		remaining := 0.0
		for _, pct := range r.CurrentLoad {
			remaining += 100 - pct
		}
		candidates = append(candidates, candidate{
			ep:               r.Endpoint,
			baseHeadroom:     headroom(r.Endpoint),
			assumedRemaining: remaining,
		})
	}

	assignments := make([]Assignment, 0, len(subUnits))
	for _, su := range subUnits {
		bestIdx := -1
		var bestScore float64
		for i, c := range candidates {
			score := c.baseHeadroom - c.assumedRemaining
			if bestIdx == -1 ||
				score > bestScore ||
				(score == bestScore && c.ep.ReplicaID < candidates[bestIdx].ep.ReplicaID) {
				bestIdx = i
				bestScore = score
			}
		}
		assignments = append(assignments, Assignment{SubUnit: su, Replica: candidates[bestIdx].ep})
		// A freshly-assigned SubUnit starts at 0% complete, i.e. 100
		// percent_remaining, until it actually reports progress.
		candidates[bestIdx].assumedRemaining += 100
	}

	sort.SliceStable(assignments, func(i, j int) bool { return assignments[i].SubUnit.SubUnitID < assignments[j].SubUnit.SubUnitID })
	return assignments
}
