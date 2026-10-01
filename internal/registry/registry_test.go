package registry

import (
	"testing"
	"time"

	"github.com/heainframework/heain-job/internal/jobapp"
)

// TestAssignSubUnitStartsActivityClock confirms AssignSubUnit gives a
// SubUnit an initial activity timestamp immediately, before any
// /progress report ever arrives -- without this, a replica that
// crashes before its first progress report would never be flagged
// stale at all (SubUnitActivity would return "not tracked" forever,
// not "stale").
func TestAssignSubUnitStartsActivityClock(t *testing.T) {
	r := New()
	_ = r.Register(jobapp.ModuleEndpoint{ModuleName: "m", StrategyName: "s", BaseURL: "http://x", Token: "t", ReplicaID: "a"})

	if err := r.AssignSubUnit("s", "a", "su1"); err != nil {
		t.Fatalf("AssignSubUnit: %v", err)
	}
	last, tracked := r.SubUnitActivity("s", "a", "su1")
	if !tracked {
		t.Fatal("expected su1 to be tracked immediately after AssignSubUnit")
	}
	if time.Since(last) > time.Second {
		t.Fatalf("expected a fresh activity timestamp, got one %s old", time.Since(last))
	}
}

// TestReportProgressRefreshesActivity confirms a /progress report
// resets the staleness clock, not just the percent-complete value.
func TestReportProgressRefreshesActivity(t *testing.T) {
	r := New()
	_ = r.Register(jobapp.ModuleEndpoint{ModuleName: "m", StrategyName: "s", BaseURL: "http://x", Token: "t", ReplicaID: "a"})
	_ = r.AssignSubUnit("s", "a", "su1")

	first, _ := r.SubUnitActivity("s", "a", "su1")
	time.Sleep(5 * time.Millisecond)

	if err := r.ReportProgress("s", "a", "su1", 42, jobapp.ProgressRunning); err != nil {
		t.Fatalf("ReportProgress: %v", err)
	}
	second, _ := r.SubUnitActivity("s", "a", "su1")
	if !second.After(first) {
		t.Fatal("expected ReportProgress to advance the SubUnit's activity timestamp")
	}

	pool := r.LookupPool("s")
	if len(pool) != 1 || pool[0].CurrentLoad["su1"] != 42 {
		t.Fatalf("expected LookupPool to reflect the reported percent-complete, got %+v", pool)
	}
}

// TestClearSubUnitStopsTracking confirms ClearSubUnit removes a
// SubUnit from both the load map (used by the scheduler) and the
// activity clock (used by the staleness watcher) -- e.g. once the
// orchestrator has reassigned it away from this replica, it must never
// come back as "stale on replica A" after A is no longer working it.
func TestClearSubUnitStopsTracking(t *testing.T) {
	r := New()
	_ = r.Register(jobapp.ModuleEndpoint{ModuleName: "m", StrategyName: "s", BaseURL: "http://x", Token: "t", ReplicaID: "a"})
	_ = r.AssignSubUnit("s", "a", "su1")

	r.ClearSubUnit("s", "a", "su1")

	if _, tracked := r.SubUnitActivity("s", "a", "su1"); tracked {
		t.Fatal("expected su1 to no longer be tracked after ClearSubUnit")
	}
	pool := r.LookupPool("s")
	if len(pool) != 1 || len(pool[0].CurrentLoad) != 0 {
		t.Fatalf("expected an empty CurrentLoad after ClearSubUnit, got %+v", pool)
	}
}

// TestSubUnitActivityUnknownReplica confirms SubUnitActivity reports
// "not tracked" (rather than panicking or returning a stale zero time
// that looks real) for a replica/subunit pair that was never assigned.
func TestSubUnitActivityUnknownReplica(t *testing.T) {
	r := New()
	if _, tracked := r.SubUnitActivity("no-such-strategy", "a", "su1"); tracked {
		t.Fatal("expected tracked=false for an unregistered strategy")
	}
	_ = r.Register(jobapp.ModuleEndpoint{ModuleName: "m", StrategyName: "s", BaseURL: "http://x", Token: "t", ReplicaID: "a"})
	if _, tracked := r.SubUnitActivity("s", "a", "never-assigned"); tracked {
		t.Fatal("expected tracked=false for a sub-unit that was never assigned")
	}
}
