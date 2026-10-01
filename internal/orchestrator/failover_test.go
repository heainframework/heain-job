package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/heainframework/heain-job/internal/audit"
	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
)

// withShortStaleness temporarily shrinks the staleness-detection window
// so these tests don't have to sleep through the real multi-second
// production value, restoring it afterward.
func withShortStaleness(t *testing.T, staleAfter, pollInterval time.Duration) {
	t.Helper()
	origStale, origPoll := DefaultSubUnitStaleAfter, subUnitActivityPollInterval
	DefaultSubUnitStaleAfter = staleAfter
	subUnitActivityPollInterval = pollInterval
	t.Cleanup(func() {
		DefaultSubUnitStaleAfter = origStale
		subUnitActivityPollInterval = origPoll
	})
}

func successReplica(t *testing.T, output string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req processSubUnitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := processSubUnitResponse{Result: jobapp.SubUnitResult{SubUnitID: req.SubUnit.SubUnitID, Output: []byte(output)}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

// hangingReplica never responds -- it holds the connection open past
// the test's shrunk staleness window, simulating a replica that is
// still "alive" at the TCP level but has stopped making any real
// progress on the SubUnit it claimed.
func hangingReplica(t *testing.T, callCount *int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount != nil {
			atomic.AddInt32(callCount, 1)
		}
		select {
		case <-r.Context().Done():
		case <-time.After(500 * time.Millisecond):
		}
	}))
}

func failingReplica(t *testing.T, callCount *int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount != nil {
			atomic.AddInt32(callCount, 1)
		}
		http.Error(w, "boom", http.StatusBadRequest)
	}))
}

// TestStaleReplicaIsReassigned closes "heain-job: stale-replica
// failover" -- a SubUnit whose first replica stops making progress on
// it (here: never responds at all) is detected as stale and reassigned
// to another live, eligible replica, which completes the SubUnit
// successfully.
func TestStaleReplicaIsReassigned(t *testing.T) {
	withShortStaleness(t, 80*time.Millisecond, 20*time.Millisecond)

	var hangCalls int32
	hung := hangingReplica(t, &hangCalls)
	defer hung.Close()
	good := successReplica(t, "DONE-ON-B")
	defer good.Close()

	reg := registry.New()
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-videos", StrategyName: "heain-videos", BaseURL: hung.URL, Token: "t", ReplicaID: "a"})
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-videos", StrategyName: "heain-videos", BaseURL: good.URL, Token: "t", ReplicaID: "b"})

	orch := New(reg, audit.NewLogger(discard{}))
	job := jobapp.JobDescriptor{JobID: "job1", OwningModule: "heain-videos"}
	su := jobapp.SubUnit{SubUnitID: "su1", JobID: "job1"}
	first, _ := reg.LookupReplica("heain-videos", "a")

	res, err := orch.runSubUnitWithFailover(context.Background(), job, su, first, func(jobapp.ModuleEndpoint) float64 { return 100 })
	if err != nil {
		t.Fatalf("runSubUnitWithFailover: %v", err)
	}
	if string(res.Output) != "DONE-ON-B" {
		t.Fatalf("output = %q, want %q (expected reassignment to replica b)", res.Output, "DONE-ON-B")
	}
	if atomic.LoadInt32(&hangCalls) != 1 {
		t.Fatalf("expected the hanging replica to be called exactly once (then abandoned), got %d", hangCalls)
	}
}

// TestTransactionalSubUnitNeverReassigned confirms Group A item 5's
// resume-semantics distinction: a job explicitly marked
// ResumeTransactional must not have a failed SubUnit redone on a
// different replica, even when one is available -- redoing it risks
// double-processing, which the transactional guarantee forbids.
func TestTransactionalSubUnitNeverReassigned(t *testing.T) {
	var failCalls, goodCalls int32
	failing := failingReplica(t, &failCalls)
	defer failing.Close()
	good := successReplica(t, "SHOULD-NOT-BE-CALLED")
	defer good.Close()
	// wrap good to also count calls
	goodCounting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&goodCalls, 1)
		var req processSubUnitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		resp := processSubUnitResponse{Result: jobapp.SubUnitResult{SubUnitID: req.SubUnit.SubUnitID, Output: []byte("x")}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer goodCounting.Close()

	reg := registry.New()
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-sd", StrategyName: "heain-sd", BaseURL: failing.URL, Token: "t", ReplicaID: "a"})
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-sd", StrategyName: "heain-sd", BaseURL: goodCounting.URL, Token: "t", ReplicaID: "b"})

	orch := New(reg, audit.NewLogger(discard{}))
	job := jobapp.JobDescriptor{JobID: "job1", OwningModule: "heain-sd", Resume: jobapp.ResumeTransactional}
	su := jobapp.SubUnit{SubUnitID: "su1", JobID: "job1"}
	first, _ := reg.LookupReplica("heain-sd", "a")

	_, err := orch.runSubUnitWithFailover(context.Background(), job, su, first, func(jobapp.ModuleEndpoint) float64 { return 100 })
	if err == nil {
		t.Fatal("expected an error -- a transactional sub-unit must fail, not reassign")
	}
	if atomic.LoadInt32(&goodCalls) != 0 {
		t.Fatalf("expected replica b to never be called for a transactional sub-unit, got %d calls", goodCalls)
	}
	if atomic.LoadInt32(&failCalls) != 1 {
		t.Fatalf("expected the failing replica to be called exactly once (no retry-as-reassignment), got %d", failCalls)
	}
}

// TestReassignmentRespectsSovereigntyZone confirms Group A item 6: when
// a job declares a SovereigntyZone, reassignment only considers
// replicas in a matching zone (or ones that declare no zone
// restriction of their own) -- a wrong-zone replica must never be
// picked, even if it's the only "idle" one available.
func TestReassignmentRespectsSovereigntyZone(t *testing.T) {
	var wrongZoneCalls int32
	failingEU := failingReplica(t, nil)
	defer failingEU.Close()
	wrongZoneUS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&wrongZoneCalls, 1)
		http.Error(w, "should never be called", http.StatusInternalServerError)
	}))
	defer wrongZoneUS.Close()
	correctZoneEU := successReplica(t, "DONE-IN-EU")
	defer correctZoneEU.Close()

	reg := registry.New()
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-sd", StrategyName: "heain-sd", BaseURL: failingEU.URL, Token: "t", ReplicaID: "a", Zone: "EU"})
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-sd", StrategyName: "heain-sd", BaseURL: wrongZoneUS.URL, Token: "t", ReplicaID: "b", Zone: "US"})
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-sd", StrategyName: "heain-sd", BaseURL: correctZoneEU.URL, Token: "t", ReplicaID: "c", Zone: "EU"})

	orch := New(reg, audit.NewLogger(discard{}))
	job := jobapp.JobDescriptor{JobID: "job1", OwningModule: "heain-sd", SovereigntyZone: "EU"}
	su := jobapp.SubUnit{SubUnitID: "su1", JobID: "job1"}
	first, _ := reg.LookupReplica("heain-sd", "a")

	res, err := orch.runSubUnitWithFailover(context.Background(), job, su, first, func(jobapp.ModuleEndpoint) float64 { return 100 })
	if err != nil {
		t.Fatalf("runSubUnitWithFailover: %v", err)
	}
	if string(res.Output) != "DONE-IN-EU" {
		t.Fatalf("output = %q, want %q", res.Output, "DONE-IN-EU")
	}
	if atomic.LoadInt32(&wrongZoneCalls) != 0 {
		t.Fatalf("expected the wrong-zone (US) replica to never be called, got %d calls", wrongZoneCalls)
	}
}
