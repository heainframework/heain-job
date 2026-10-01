package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
	"github.com/heainframework/heain-job/internal/scheduler"
)

const DefaultCallbackTimeout = 10 * time.Second
const DefaultMaxRetries = 3
const DefaultRetryBaseDelay = 1 * time.Second

type moduleHTTPError struct {
	StatusCode int
	Body       string
}

func (e *moduleHTTPError) Error() string {
	return fmt.Sprintf("module returned %d: %s", e.StatusCode, e.Body)
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if httpErr, ok := err.(*moduleHTTPError); ok {
		return httpErr.StatusCode >= 500
	}
	return true // transport-level error (connection refused, DNS, timeout, etc.)
}

// AuditRecorder is satisfied by *audit.Logger (and by any test double).
// Declared locally so this package doesn't need to import internal/audit.
type AuditRecorder interface {
	Record(jobapp.AuditRecord) error
}

type Orchestrator struct {
	Registry *registry.Registry
	Client   *http.Client
	Audit    AuditRecorder
}

func New(reg *registry.Registry, auditLog AuditRecorder) *Orchestrator {
	return &Orchestrator{
		Registry: reg,
		Client:   &http.Client{Timeout: DefaultCallbackTimeout},
		Audit:    auditLog,
	}
}

type splitRequest struct {
	Job jobapp.JobDescriptor `json:"job"`
	// Payload carries the job's real input bytes to the module's /split
	// handler (Stage B, 2026-10-01 -- see design-notes/n-tier-
	// generalization.md). Additive and optional: a module whose /split
	// doesn't need real bytes (e.g. heain-image's v1 whole-job-to-one-
	// worker placeholder) simply ignores it.
	Payload []byte `json:"payload,omitempty"`
	// DesiredChunks hints how many SubUnits to split into, based on how
	// many live replicas RunFannedOutJob actually found in the pool at
	// split time. A module whose /split has its own fixed chunking rule
	// may ignore this.
	DesiredChunks int `json:"desired_chunks,omitempty"`
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

// processSubUnitRequest/Response are Stage B's generic per-SubUnit
// processing call, mirroring jobwire.ProcessSubUnitRequest/Response
// field for field. Unlike /split and /merge (whole-job, single
// representative replica), this is sent to one specific assigned
// replica per SubUnit.
type processSubUnitRequest struct {
	Strategy  string         `json:"strategy"`
	ReplicaID string         `json:"replica_id"`
	SubUnit   jobapp.SubUnit `json:"sub_unit"`
}

type processSubUnitResponse struct {
	Result jobapp.SubUnitResult `json:"result"`
}

func (o *Orchestrator) Split(ctx context.Context, job jobapp.JobDescriptor) ([]jobapp.SubUnit, error) {
	ep, err := o.Registry.Lookup(job.OwningModule)
	if err != nil {
		_ = o.Audit.Record(jobapp.AuditRecord{
			Action: "module_lookup_failed",
			Detail: fmt.Sprintf("job=%s module=%s error=%s", job.JobID, job.OwningModule, err),
		})
		return nil, err
	}

	reqBody, err := json.Marshal(splitRequest{Job: job})
	if err != nil {
		return nil, err
	}

	var resp splitResponse
	if err := o.callModule(ctx, ep, "/split", reqBody, &resp); err != nil {
		return nil, err
	}

	_ = o.Audit.Record(jobapp.AuditRecord{
		Action: "split",
		Detail: fmt.Sprintf("job=%s module=%s sub_units=%d", job.JobID, job.OwningModule, len(resp.SubUnits)),
	})
	return resp.SubUnits, nil
}

func (o *Orchestrator) Merge(ctx context.Context, job jobapp.JobDescriptor, results []jobapp.SubUnitResult) (jobapp.MergedResult, error) {
	ep, err := o.Registry.Lookup(job.OwningModule)
	if err != nil {
		_ = o.Audit.Record(jobapp.AuditRecord{
			Action: "module_lookup_failed",
			Detail: fmt.Sprintf("job=%s module=%s error=%s", job.JobID, job.OwningModule, err),
		})
		return jobapp.MergedResult{}, err
	}

	reqBody, err := json.Marshal(mergeRequest{Job: job, Results: results})
	if err != nil {
		return jobapp.MergedResult{}, err
	}

	var resp mergeResponse
	if err := o.callModule(ctx, ep, "/merge", reqBody, &resp); err != nil {
		return jobapp.MergedResult{}, err
	}

	_ = o.Audit.Record(jobapp.AuditRecord{
		Action: "merge",
		Detail: fmt.Sprintf("job=%s module=%s", job.JobID, job.OwningModule),
	})
	return resp.Result, nil
}

// RunFannedOutJob is Stage B's entry point (see design-notes/
// n-tier-generalization.md): splits a job across heain-job's registered
// replica pool for job.OwningModule, rather than the single-instance
// whole-job-to-one-worker path Split/Merge alone provide. inputPayload
// is the job's real input bytes, threaded through to the module's
// /split handler via splitRequest's new Payload field. headroom lets
// the caller supply each replica's live capacity signal (e.g. from
// heain-core's own P2 /capacity query, once a module exposes an
// equivalent) -- v1 callers may legitimately pass a constant, since the
// remaining-load half of the score already captures the "near done vs.
// just started" distinction this was built for.
func (o *Orchestrator) RunFannedOutJob(ctx context.Context, job jobapp.JobDescriptor, inputPayload []byte, headroom scheduler.CapacityHeadroom) (jobapp.MergedResult, error) {
	pool := o.Registry.LookupPool(job.OwningModule)
	if len(pool) == 0 {
		_ = o.Audit.Record(jobapp.AuditRecord{
			Action: "module_lookup_failed",
			Detail: fmt.Sprintf("job=%s module=%s error=%s", job.JobID, job.OwningModule, registry.ErrNotRegistered),
		})
		return jobapp.MergedResult{}, registry.ErrNotRegistered
	}

	splitEp := lowestReplicaID(pool)
	reqBody, err := json.Marshal(splitRequest{Job: job, Payload: inputPayload, DesiredChunks: len(pool)})
	if err != nil {
		return jobapp.MergedResult{}, err
	}
	var splitResp splitResponse
	if err := o.callModule(ctx, splitEp, "/split", reqBody, &splitResp); err != nil {
		return jobapp.MergedResult{}, err
	}
	_ = o.Audit.Record(jobapp.AuditRecord{
		Action: "split",
		Detail: fmt.Sprintf("job=%s module=%s sub_units=%d (fanned out)", job.JobID, job.OwningModule, len(splitResp.SubUnits)),
	})

	assignments := scheduler.Assign(splitResp.SubUnits, pool, headroom)

	results := make([]jobapp.SubUnitResult, len(assignments))
	errs := make([]error, len(assignments))
	var wg sync.WaitGroup
	for i, a := range assignments {
		wg.Add(1)
		go func(i int, a scheduler.Assignment) {
			defer wg.Done()
			reqBody, err := json.Marshal(processSubUnitRequest{
				Strategy:  job.OwningModule,
				ReplicaID: a.Replica.ReplicaID,
				SubUnit:   a.SubUnit,
			})
			if err != nil {
				errs[i] = err
				return
			}
			var resp processSubUnitResponse
			if err := o.callModule(ctx, a.Replica, "/process-subunit", reqBody, &resp); err != nil {
				errs[i] = err
				return
			}
			results[i] = resp.Result
		}(i, a)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			_ = o.Audit.Record(jobapp.AuditRecord{
				Action: "subunit_process_failed",
				Detail: fmt.Sprintf("job=%s sub_unit=%s error=%s", job.JobID, assignments[i].SubUnit.SubUnitID, e),
			})
			return jobapp.MergedResult{}, fmt.Errorf("sub_unit %s failed: %w", assignments[i].SubUnit.SubUnitID, e)
		}
	}
	_ = o.Audit.Record(jobapp.AuditRecord{
		Action: "fanned_out_subunits_complete",
		Detail: fmt.Sprintf("job=%s module=%s sub_units=%d", job.JobID, job.OwningModule, len(results)),
	})

	return o.Merge(ctx, job, results)
}

// lowestReplicaID returns the pool's lowest-ReplicaID entry's endpoint,
// deterministically -- used wherever Stage B needs to pick "any one"
// representative replica (e.g. to call /split once), matching
// registry.Registry.Lookup's own tie-break convention.
func lowestReplicaID(pool []registry.ReplicaInfo) jobapp.ModuleEndpoint {
	sorted := make([]registry.ReplicaInfo, len(pool))
	copy(sorted, pool)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Endpoint.ReplicaID < sorted[j].Endpoint.ReplicaID })
	return sorted[0].Endpoint
}

func (o *Orchestrator) callModule(ctx context.Context, ep jobapp.ModuleEndpoint, path string, body []byte, out interface{}) error {
	var lastErr error
	for attempt := 0; attempt <= DefaultMaxRetries; attempt++ {
		if attempt > 0 {
			delay := DefaultRetryBaseDelay * time.Duration(1<<(attempt-1))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := o.doCallModule(ctx, ep, path, body, out)
		if err == nil {
			if attempt > 0 {
				_ = o.Audit.Record(jobapp.AuditRecord{
					Action: "module_call_retry_succeeded",
					Detail: fmt.Sprintf("module=%s path=%s attempt=%d", ep.ModuleName, path, attempt+1),
				})
			}
			return nil
		}

		lastErr = err
		_ = o.Audit.Record(jobapp.AuditRecord{
			Action: "module_call_failed",
			Detail: fmt.Sprintf("module=%s path=%s attempt=%d error=%s", ep.ModuleName, path, attempt+1, err),
		})

		if !isRetryable(err) {
			return err
		}
	}
	return fmt.Errorf("after %d attempts: %w", DefaultMaxRetries+1, lastErr)
}

func (o *Orchestrator) doCallModule(ctx context.Context, ep jobapp.ModuleEndpoint, path string, body []byte, out interface{}) error {
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
		return &moduleHTTPError{StatusCode: resp.StatusCode, Body: string(b)}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
