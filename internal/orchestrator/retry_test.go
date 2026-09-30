package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/heainframework/heain-job/internal/audit"
	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
)

// TestSplitRetriesOnTransientFailureThenSucceeds verifies that a 503 on the
// first two attempts is retried, and the call succeeds on the third attempt.
// Note: this test sleeps through the real 1s+2s backoff (DefaultRetryBaseDelay
// is a package const, not overridden here), so it takes ~3s to run.
func TestSplitRetriesOnTransientFailureThenSucceeds(t *testing.T) {
	var calls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var req splitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		resp := splitResponse{SubUnits: []jobapp.SubUnit{
			{SubUnitID: "su1", JobID: req.Job.JobID},
		}}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	reg := registry.New()
	if err := reg.Register(jobapp.ModuleEndpoint{
		ModuleName:   "test-module",
		StrategyName: "test-module",
		BaseURL:      server.URL,
		Token:        "secret",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var auditBuf bytes.Buffer
	orch := New(reg, audit.NewLogger(&auditBuf))

	subUnits, err := orch.Split(context.Background(), jobapp.JobDescriptor{JobID: "job-1", OwningModule: "test-module"})
	if err != nil {
		t.Fatalf("expected Split to succeed after retries, got error: %v", err)
	}
	if len(subUnits) != 1 {
		t.Fatalf("expected 1 sub-unit, got %d", len(subUnits))
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expected server to be called 3 times, got %d", got)
	}
}

// TestSplitNeverRetries4xx verifies that a 400 response fails immediately,
// with no retry attempts.
func TestSplitNeverRetries4xx(t *testing.T) {
	var calls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	reg := registry.New()
	if err := reg.Register(jobapp.ModuleEndpoint{
		ModuleName:   "test-module",
		StrategyName: "test-module",
		BaseURL:      server.URL,
		Token:        "secret",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var auditBuf bytes.Buffer
	orch := New(reg, audit.NewLogger(&auditBuf))

	_, err := orch.Split(context.Background(), jobapp.JobDescriptor{JobID: "job-1", OwningModule: "test-module"})
	if err == nil {
		t.Fatal("expected Split to fail on 400, got nil error")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected server to be called exactly 1 time (no retries), got %d", got)
	}
}
