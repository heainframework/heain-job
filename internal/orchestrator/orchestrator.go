// Package orchestrator implements heain-job's core split/checkpoint/
// reassign/merge workflow. It never processes content itself — it drives
// the workflow and delegates the actual split/merge decisions to whichever
// module is registered for a job's strategy, over the HTTP callback
// mechanism designed in "heain-job <-> module registration mechanism"
// (see internal/jobapp.ModuleEndpoint's docs).
//
// Talking to heain-core itself (P1-P4: ingest/dispatch/execute/staging) is
// a separate concern, handled by the heain-sdk coreclient package (see
// cmd/heain-job) — this package only ever calls out to sibling Layer 3
// modules.
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/heainframework/heain-job/internal/audit"
	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
)

// DefaultCallbackTimeout bounds a single /split or /merge call to a
// registered module. Split/merge are expected to be fast planning/assembly
// calls, not the heavy processing itself (which still runs through
// heain-core's P2/P3 dispatch, unchanged).
const DefaultCallbackTimeout = 30 * time.Second

// Orchestrator ties the registry, audit log, and module HTTP callbacks
// together.
type Orchestrator struct {
	Registry *registry.Registry
	Audit    *audit.Logger
	Client   *http.Client
}

// New returns an Orchestrator with sane defaults.
func New(reg *registry.Registry, log *audit.Logger) *Orchestrator {
	return &Orchestrator{
		Registry: reg,
		Audit:    log,
		Client:   &http.Client{Timeout: DefaultCallbackTimeout},
	}
}

type splitRequest struct {
	Job jobapp.JobDescriptor `json:"job"`
}

type splitResponse struct {
	SubUnits []jobapp.SubUnit `json:"sub_units"`
}

type mergeRequest struct {
	Job     jobapp.JobDescriptor   `json:"job"`
	Results []jobapp.SubUnitResult `json:"results"`
}

type mergeResponse struct {
	Result jobapp.MergedResult `json:"result"`
}

// Split calls the registered module's /split endpoint for the job's
// strategy (j.OwningModule) and records the decision to the audit log.
func (o *Orchestrator) Split(ctx context.Context, j jobapp.JobDescriptor) ([]jobapp.SubUnit, error) {
	ep, err := o.Registry.Lookup(j.OwningModule)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: split lookup for %q: %w", j.OwningModule, err)
	}

	reqBody, err := json.Marshal(splitRequest{Job: j})
	if err != nil {
		return nil, fmt.Errorf("orchestrator: marshal split request: %w", err)
	}

	var resp splitResponse
	if err := o.callModule(ctx, ep, "/split", reqBody, &resp); err != nil {
		return nil, fmt.Errorf("orchestrator: split call to %q: %w", ep.ModuleName, err)
	}

	_ = o.Audit.Record(jobapp.AuditRecord{
		JobID:  j.JobID,
		Action: "split",
		Detail: fmt.Sprintf("module=%s sub_units=%d", ep.ModuleName, len(resp.SubUnits)),
	})

	return resp.SubUnits, nil
}

// Merge calls the registered module's /merge endpoint for the job's
// strategy, combining completed sub-unit results, and records the
// decision.
func (o *Orchestrator) Merge(ctx context.Context, j jobapp.JobDescriptor, results []jobapp.SubUnitResult) (jobapp.MergedResult, error) {
	ep, err := o.Registry.Lookup(j.OwningModule)
	if err != nil {
		return jobapp.MergedResult{}, fmt.Errorf("orchestrator: merge lookup for %q: %w", j.OwningModule, err)
	}

	reqBody, err := json.Marshal(mergeRequest{Job: j, Results: results})
	if err != nil {
		return jobapp.MergedResult{}, fmt.Errorf("orchestrator: marshal merge request: %w", err)
	}

	var resp mergeResponse
	if err := o.callModule(ctx, ep, "/merge", reqBody, &resp); err != nil {
		return jobapp.MergedResult{}, fmt.Errorf("orchestrator: merge call to %q: %w", ep.ModuleName, err)
	}

	_ = o.Audit.Record(jobapp.AuditRecord{
		JobID:  j.JobID,
		Action: "merge_complete",
		Detail: fmt.Sprintf("module=%s sub_unit_results=%d", ep.ModuleName, len(results)),
	})

	return resp.Result, nil
}

// callModule POSTs body to ep.BaseURL+path, authenticated with ep.Token,
// and decodes the JSON response into out.
func (o *Orchestrator) callModule(ctx context.Context, ep jobapp.ModuleEndpoint, path string, body []byte, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+ep.Token)

	resp, err := o.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("module returned %d: %s", resp.StatusCode, string(b))
	}

	return json.NewDecoder(resp.Body).Decode(out)
}
