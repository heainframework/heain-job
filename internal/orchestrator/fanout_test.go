package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/heainframework/heain-job/internal/audit"
	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
)

// fakeModuleReplica is a minimal httptest-backed stand-in for a real
// Layer 3 module replica: it answers /split (returning two fixed
// SubUnits derived from the request's Payload), /process-subunit
// (uppercasing the SubUnit's Payload -- a trivially checkable
// transform), and /merge (concatenating results in request order).
func fakeModuleReplica(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/split", func(w http.ResponseWriter, r *http.Request) {
		var req splitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		half := len(req.Payload) / 2
		resp := splitResponse{SubUnits: []jobapp.SubUnit{
			{SubUnitID: req.Job.JobID + "-su1", JobID: req.Job.JobID, Payload: req.Payload[:half]},
			{SubUnitID: req.Job.JobID + "-su2", JobID: req.Job.JobID, Payload: req.Payload[half:]},
		}}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/process-subunit", func(w http.ResponseWriter, r *http.Request) {
		var req processSubUnitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		out := make([]byte, len(req.SubUnit.Payload))
		for i, b := range req.SubUnit.Payload {
			if b >= 'a' && b <= 'z' {
				b -= 'a' - 'A'
			}
			out[i] = b
		}
		resp := processSubUnitResponse{Result: jobapp.SubUnitResult{SubUnitID: req.SubUnit.SubUnitID, Output: out}}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/merge", func(w http.ResponseWriter, r *http.Request) {
		var req mergeRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		// Order results by SubUnitID so the merged output is deterministic
		// regardless of which goroutine's HTTP call happened to land first.
		bySubUnit := map[string][]byte{}
		for _, res := range req.Results {
			bySubUnit[res.SubUnitID] = res.Output
		}
		merged := append(bySubUnit[req.Job.JobID+"-su1"], bySubUnit[req.Job.JobID+"-su2"]...)
		resp := mergeResponse{Result: jobapp.MergedResult{JobID: req.Job.JobID, Output: merged}}
		_ = json.NewEncoder(w).Encode(resp)
	})
	return httptest.NewServer(mux)
}

func TestRunFannedOutJobSplitsAssignsAndMerges(t *testing.T) {
	replicaA := fakeModuleReplica(t)
	defer replicaA.Close()
	replicaB := fakeModuleReplica(t)
	defer replicaB.Close()

	reg := registry.New()
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-videos", StrategyName: "heain-videos", BaseURL: replicaA.URL, Token: "t", ReplicaID: "a"})
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-videos", StrategyName: "heain-videos", BaseURL: replicaB.URL, Token: "t", ReplicaID: "b"})

	orch := New(reg, audit.NewLogger(discard{}))

	job := jobapp.JobDescriptor{JobID: "job1", OwningModule: "heain-videos"}
	result, err := orch.RunFannedOutJob(context.Background(), job, []byte("abcdwxyz"), func(jobapp.ModuleEndpoint) float64 { return 100 })
	if err != nil {
		t.Fatalf("RunFannedOutJob: %v", err)
	}
	if string(result.Output) != "ABCDWXYZ" {
		t.Fatalf("merged output = %q, want %q", result.Output, "ABCDWXYZ")
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
