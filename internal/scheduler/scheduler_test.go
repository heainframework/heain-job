package scheduler

import (
	"testing"

	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
)

func ep(replicaID string) jobapp.ModuleEndpoint {
	return jobapp.ModuleEndpoint{ModuleName: "m", StrategyName: "s", BaseURL: "http://" + replicaID, ReplicaID: replicaID}
}

func uniformHeadroom(h float64) CapacityHeadroom {
	return func(jobapp.ModuleEndpoint) float64 { return h }
}

func TestAssignPicksHighestScoringReplica(t *testing.T) {
	pool := []registry.ReplicaInfo{
		{Endpoint: ep("busy"), CurrentLoad: map[string]float64{"existing-su": 10}}, // 90% remaining
		{Endpoint: ep("idle"), CurrentLoad: map[string]float64{}},                  // 0% remaining
	}
	subUnits := []jobapp.SubUnit{{SubUnitID: "su1", JobID: "job1"}}

	got := Assign(subUnits, pool, uniformHeadroom(100))
	if len(got) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(got))
	}
	if got[0].Replica.ReplicaID != "idle" {
		t.Fatalf("expected the idle replica to win, got %q", got[0].Replica.ReplicaID)
	}
}

func TestAssignNearlyDoneBeatsHigherHeadroomJustStarted(t *testing.T) {
	// The author's own motivating case: a replica deep into finishing
	// (5% remaining) should win over a replica that just started a big
	// job (90% remaining), even if the just-started replica reports
	// slightly more raw capacity headroom.
	pool := []registry.ReplicaInfo{
		{Endpoint: ep("almost-done"), CurrentLoad: map[string]float64{"su-a": 95}}, // 5% remaining
		{Endpoint: ep("just-started"), CurrentLoad: map[string]float64{"su-b": 5}}, // 95% remaining
	}
	headroom := func(e jobapp.ModuleEndpoint) float64 {
		if e.ReplicaID == "just-started" {
			return 10 // slightly more raw headroom
		}
		return 5
	}
	subUnits := []jobapp.SubUnit{{SubUnitID: "su1", JobID: "job1"}}

	got := Assign(subUnits, pool, headroom)
	if got[0].Replica.ReplicaID != "almost-done" {
		t.Fatalf("expected almost-done to win (score %v) over just-started (score %v), got %q",
			5.0-5.0, 10.0-95.0, got[0].Replica.ReplicaID)
	}
}

func TestAssignSpreadsMultipleSubUnitsAcrossPool(t *testing.T) {
	pool := []registry.ReplicaInfo{
		{Endpoint: ep("r1"), CurrentLoad: map[string]float64{}},
		{Endpoint: ep("r2"), CurrentLoad: map[string]float64{}},
	}
	subUnits := []jobapp.SubUnit{
		{SubUnitID: "su1", JobID: "job1"},
		{SubUnitID: "su2", JobID: "job1"},
	}

	got := Assign(subUnits, pool, uniformHeadroom(100))
	if len(got) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(got))
	}
	if got[0].Replica.ReplicaID == got[1].Replica.ReplicaID {
		t.Fatalf("expected the two sub-units to spread across different replicas when starting equal, got both on %q", got[0].Replica.ReplicaID)
	}
}

func TestAssignTiebreaksOnLowestReplicaID(t *testing.T) {
	pool := []registry.ReplicaInfo{
		{Endpoint: ep("r2"), CurrentLoad: map[string]float64{}},
		{Endpoint: ep("r1"), CurrentLoad: map[string]float64{}},
	}
	subUnits := []jobapp.SubUnit{{SubUnitID: "su1", JobID: "job1"}}

	got := Assign(subUnits, pool, uniformHeadroom(100))
	if got[0].Replica.ReplicaID != "r1" {
		t.Fatalf("expected tie to break on lowest replica-id (r1), got %q", got[0].Replica.ReplicaID)
	}
}
