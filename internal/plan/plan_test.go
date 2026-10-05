package plan

import (
	"testing"
)

func TestPlannerLearns(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir, Params{TargetUnitSeconds: 10, MaxPartsPerWorker: 4})
	if err != nil {
		t.Fatal(err)
	}
	d := p.Decide("video.conform", 1000, 3)
	h0 := d.ModelHash
	if d.Parts != 3 || d.Factors["observations"] != 0 {
		t.Fatalf("no history: one per worker, got %+v", d)
	}
	for i := 0; i < 5; i++ { // 0.05 s/byte
		p.Observe("video.conform", 200, 10)
	}
	d = p.Decide("video.conform", 1000, 3) // predicted 50 s / 10 s -> 5 parts, cap 12
	if d.Parts != 5 || d.Confidence <= 0.5 || len(d.ModelHash) != 64 {
		t.Fatalf("learned: %+v", d)
	}
	if d2 := p.Decide("video.conform", 100000, 2); d2.Parts != 8 {
		t.Fatalf("capped at workers x MaxPartsPerWorker: %+v", d2)
	}
	if d3 := p.Decide("video.conform", 2, 3); d3.Parts > 2 {
		t.Fatalf("never more parts than bytes: %+v", d3)
	}
	if err := p.Save(); err != nil {
		t.Fatal(err)
	}
	q, _ := Open(dir, Params{TargetUnitSeconds: 10, MaxPartsPerWorker: 4})
	if q.Stats()["video.conform"].Observations != 5 {
		t.Fatal("learned state must survive a restart")
	}
	if h0 == d.ModelHash || q.Decide("x", 10, 1).ModelHash != d.ModelHash {
		t.Fatal("the model hash identifies the learned state")
	}
}
