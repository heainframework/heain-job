package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/heainframework/heain-job/internal/audit"
	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
)

func TestSplitCallsRegisteredModule(t *testing.T) {
	var gotToken string
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("Authorization")
		if r.URL.Path != "/split" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var req splitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		resp := splitResponse{SubUnits: []jobapp.SubUnit{
			{SubUnitID: "su1", JobID: req.Job.JobID},
			{SubUnitID: "su2", JobID: req.Job.JobID},
		}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer fake.Close()

	reg := registry.New()
	if err := reg.Register(jobapp.ModuleEndpoint{
		ModuleName:   "heain-image",
		StrategyName: "heain-image",
		BaseURL:      fake.URL,
		Token:        "shh",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var auditBuf bytes.Buffer
	orch := New(reg, audit.NewLogger(&auditBuf))

	subs, err := orch.Split(context.Background(), jobapp.JobDescriptor{JobID: "job1", OwningModule: "heain-image"})
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(subs) != 2 {
		t.Fatalf("got %d sub-units, want 2", len(subs))
	}
	if gotToken != "Bearer shh" {
		t.Fatalf("Authorization header = %q, want %q", gotToken, "Bearer shh")
	}
	if auditBuf.Len() == 0 {
		t.Fatal("expected a split event recorded to the audit log")
	}
}

func TestSplitUnregisteredStrategy(t *testing.T) {
	reg := registry.New()
	var auditBuf bytes.Buffer
	orch := New(reg, audit.NewLogger(&auditBuf))

	_, err := orch.Split(context.Background(), jobapp.JobDescriptor{JobID: "job1", OwningModule: "heain-image"})
	if err == nil {
		t.Fatal("expected an error for an unregistered strategy")
	}
}

func TestMergeCallsRegisteredModule(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/merge" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		resp := mergeResponse{Result: jobapp.MergedResult{JobID: "job1", Output: []byte("done")}}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer fake.Close()

	reg := registry.New()
	_ = reg.Register(jobapp.ModuleEndpoint{ModuleName: "heain-image", StrategyName: "heain-image", BaseURL: fake.URL, Token: "t"})

	var auditBuf bytes.Buffer
	orch := New(reg, audit.NewLogger(&auditBuf))

	result, err := orch.Merge(context.Background(), jobapp.JobDescriptor{JobID: "job1", OwningModule: "heain-image"}, []jobapp.SubUnitResult{
		{SubUnitID: "su1", Output: []byte("a")},
	})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if string(result.Output) != "done" {
		t.Fatalf("got Output %q, want %q", result.Output, "done")
	}
}
