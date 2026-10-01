// Package httpserver exposes heain-job's own HTTP surface: module
// registration, progress reporting (Stage B), and (as the orchestrator
// grows) job submission/status.
//
// This is a plain, internal-network HTTP server — deliberately NOT the
// mTLS node-to-node transport heain-core's Layer 2 nodes use, per the
// "heain-job <-> module registration mechanism" design decision: heain-job
// and every Layer 3 module run inside the same internal Docker network,
// never exposed to the internet.
package httpserver

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/heainframework/heain-job/internal/capacityquery"
	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
	"github.com/heainframework/heain-job/internal/scheduler"
)

// Orchestrator is the subset of *orchestrator.Orchestrator that
// handleRunJob needs. Declared locally (not imported from
// internal/orchestrator) so httpserver doesn't need a direct package
// dependency beyond this one call -- satisfied structurally by
// *orchestrator.Orchestrator.
type Orchestrator interface {
	RunFannedOutJob(ctx context.Context, job jobapp.JobDescriptor, inputPayload []byte, headroom scheduler.CapacityHeadroom) (jobapp.MergedResult, error)
}

// Server wires the registry and orchestrator to HTTP handlers.
type Server struct {
	Registry     *registry.Registry
	Orchestrator Orchestrator // nil = /run-job disabled (e.g. a deployment with no orchestrator wired yet)
	mux          *http.ServeMux
}

// New builds a Server with its routes registered. orch may be nil,
// which disables /run-job (returns 503) without affecting any other
// route -- registration/progress/health never depended on it.
func New(reg *registry.Registry, orch Orchestrator) *Server {
	s := &Server{
		Registry:     reg,
		Orchestrator: orch,
		mux:          http.NewServeMux(),
	}
	s.mux.HandleFunc("/register", s.handleRegister)
	s.mux.HandleFunc("/deregister", s.handleDeregister)
	s.mux.HandleFunc("/registry/", s.handleRegistryLookup)
	s.mux.HandleFunc("/progress", s.handleProgress)
	s.mux.HandleFunc("/run-job", s.handleRunJob)
	s.mux.HandleFunc("/health", s.handleHealth)
	return s
}

// ServeHTTP satisfies http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var ep jobapp.ModuleEndpoint
	if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if ep.ModuleName == "" || ep.StrategyName == "" || ep.BaseURL == "" || ep.Token == "" {
		http.Error(w, "module_name, strategy_name, base_url, and token are all required", http.StatusBadRequest)
		return
	}

	if err := s.Registry.Register(ep); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	log.Printf("heain-job: registered module %q for strategy %q replica %q at %q", ep.ModuleName, ep.StrategyName, ep.ReplicaID, ep.BaseURL)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		StrategyName string `json:"strategy_name"`
		ReplicaID    string `json:"replica_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.Registry.DeregisterReplica(req.StrategyName, req.ReplicaID)
	w.WriteHeader(http.StatusOK)
}

// handleRegistryLookup is a read-only endpoint letting another service
// (e.g. heain-core's P3 Executor bridge) resolve a strategy name to its
// registered module endpoint, reusing heain-job's own TTL-backed
// registry as the single live source of truth rather than a second,
// separately-configured copy of the same mapping. Returns 404 if the
// strategy is unregistered or has gone stale (registry.ErrNotRegistered
// / registry.ErrStale) -- the caller treats both the same way. This
// stays single-endpoint ("any one live replica") on purpose -- callers
// needing the full Stage B pool use the registry's LookupPool directly
// in-process (the orchestrator), not over HTTP.
func (s *Server) handleRegistryLookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	strategy := strings.TrimPrefix(r.URL.Path, "/registry/")
	if strategy == "" {
		http.Error(w, "missing strategy name in path", http.StatusBadRequest)
		return
	}

	ep, err := s.Registry.Lookup(strategy)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ep)
}

// handleProgress is Stage B's self-reporting endpoint: a replica
// actively executing a SubUnit POSTs its progress here, authenticated
// with the same shared-secret token it registered with for this exact
// (strategy, replica) pair -- the same per-module-trust model already
// used for /split and /merge, just in the reverse call direction.
func (s *Server) handleProgress(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req jobapp.ProgressReport
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.StrategyName == "" || req.SubUnitID == "" {
		http.Error(w, "strategy_name and sub_unit_id are required", http.StatusBadRequest)
		return
	}

	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}
	token := h[len(prefix):]

	ep, err := s.Registry.LookupReplica(req.StrategyName, req.ReplicaID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(ep.Token)) != 1 {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	if err := s.Registry.ReportProgress(req.StrategyName, req.ReplicaID, req.SubUnitID, req.PercentComplete, req.Status); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleRunJob (Stage B) is heain-job's first real job-submission
// endpoint: POST {"job": {...}, "payload_b64": "..."} runs
// Orchestrator.RunFannedOutJob end-to-end (split across the live
// replica pool, load-aware assignment, concurrent /process-subunit
// calls, merge) and returns the merged result. v1 uses a constant
// capacity-headroom function (every replica reports the same raw
// headroom) -- see design-notes/n-tier-generalization.md's note that
// the remaining-load half of the score already captures the "near done
// vs. just started" distinction this was built for; querying each
// replica's real RAM/GPU/CPU is a tracked future increment, not a v1
// blocker.
func (s *Server) handleRunJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Orchestrator == nil {
		http.Error(w, "no orchestrator wired on this heain-job instance", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		Job        jobapp.JobDescriptor `json:"job"`
		PayloadB64 string               `json:"payload_b64"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	payload, err := base64.StdEncoding.DecodeString(req.PayloadB64)
	if err != nil {
		http.Error(w, "bad payload_b64: "+err.Error(), http.StatusBadRequest)
		return
	}

	result, err := s.Orchestrator.RunFannedOutJob(r.Context(), req.Job, payload, capacityquery.HTTP(nil))
	if err != nil {
		http.Error(w, "job failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":     result.JobID,
		"output_b64": base64.StdEncoding.EncodeToString(result.Output),
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":             "ok",
		"registered_modules": s.Registry.All(),
	})
}
