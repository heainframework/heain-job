// Package httpserver exposes heain-job's own HTTP surface: module
// registration, and (as the orchestrator grows) job submission/status.
//
// This is a plain, internal-network HTTP server — deliberately NOT the
// mTLS node-to-node transport heain-core's Layer 2 nodes use, per the
// "heain-job <-> module registration mechanism" design decision: heain-job
// and every Layer 3 module run inside the same internal Docker network,
// never exposed to the internet.
package httpserver

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
)

// Server wires the registry (and, later, the orchestrator) to HTTP
// handlers.
type Server struct {
	Registry *registry.Registry
	mux      *http.ServeMux
}

// New builds a Server with its routes registered.
func New(reg *registry.Registry) *Server {
	s := &Server{
		Registry: reg,
		mux:      http.NewServeMux(),
	}
	s.mux.HandleFunc("/register", s.handleRegister)
	s.mux.HandleFunc("/deregister", s.handleDeregister)
	s.mux.HandleFunc("/registry/", s.handleRegistryLookup)
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

	log.Printf("heain-job: registered module %q for strategy %q at %q", ep.ModuleName, ep.StrategyName, ep.BaseURL)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		StrategyName string `json:"strategy_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.Registry.Deregister(req.StrategyName)
	w.WriteHeader(http.StatusOK)
}

// handleRegistryLookup is a read-only endpoint letting another service
// (e.g. heain-core's P3 Executor bridge) resolve a strategy name to its
// registered module endpoint, reusing heain-job's own TTL-backed
// registry as the single live source of truth rather than a second,
// separately-configured copy of the same mapping. Returns 404 if the
// strategy is unregistered or has gone stale (registry.ErrNotRegistered
// / registry.ErrStale) -- the caller treats both the same way.
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

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":             "ok",
		"registered_modules": s.Registry.All(),
	})
}
