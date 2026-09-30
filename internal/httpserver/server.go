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

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":             "ok",
		"registered_modules": s.Registry.All(),
	})
}
