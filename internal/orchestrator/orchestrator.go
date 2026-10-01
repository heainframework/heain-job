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

// DefaultSubUnitStaleAfter is how long a SubUnit may go without any
// activity (its initial assignment, or its most recent /progress report)
// before the orchestrator treats its current replica as stalled on it
// and reassigns the SubUnit elsewhere (closes "heain-job: stale-replica
// failover" -- see design-notes/n-tier-generalization.md). This is
// deliberately well above registry.DefaultTTL: a replica's own
// re-registration heartbeat only proves the replica process is alive,
// never that any one specific SubUnit it claimed is actually making
// progress, so this is a second, independent clock.
//
// Declared as a var, not a const, purely so tests can shrink it (and
// subUnitActivityPollInterval below) to avoid sleeping through the real
// multi-second production window -- production code never reassigns
// these.
var DefaultSubUnitStaleAfter = 2 * registry.DefaultTTL

// subUnitActivityPollInterval is how often the staleness watcher checks
// an in-flight SubUnit's last-activity timestamp.
var subUnitActivityPollInterval = registry.DefaultTTL / 2

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
		// No fixed Client-level timeout: /split and /merge are bounded by
		// an explicit context deadline at their own call sites below
		// (DefaultCallbackTimeout), but /process-subunit's call is
		// bounded only by its own staleness watcher (DefaultSubUnitStaleAfter)
		// since real SubUnit processing (e.g. ML inference on a video
		// chunk) can legitimately run far longer than a fixed short
		// timeout -- a flat client-level Timeout here would have
		// silently aborted and retried every real long-running SubUnit
		// call against the *same* replica every few seconds, which is
		// exactly the bug this change closes.
		Client: &http.Client{},
		Audit:  auditLog,
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

	ctx, cancel := context.WithTimeout(ctx, DefaultCallbackTimeout)
	defer cancel()

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

	ctx, cancel := context.WithTimeout(ctx, DefaultCallbackTimeout)
	defer cancel()

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
//
// Stale-replica failover (2026-10-02): each SubUnit is watched
// independently while in flight. A SubUnit whose replica stops making
// progress on it (DefaultSubUnitStaleAfter) is reassigned to another
// live, eligible replica -- "eligible" meaning not yet tried for this
// SubUnit, and, if job.SovereigntyZone is set, in a matching zone (or a
// replica that declares no zone restriction of its own) -- as long as
// job.EffectiveResume() is ResumeIdempotent (the default). A
// ResumeTransactional job never reassigns a SubUnit that has already
// been dispatched once: the whole job fails instead, since redoing a
// transactional SubUnit risks double-processing it.
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
	splitCtx, splitCancel := context.WithTimeout(ctx, DefaultCallbackTimeout)
	var splitResp splitResponse
	splitErr := o.callModule(splitCtx, splitEp, "/split", reqBody, &splitResp)
	splitCancel()
	if splitErr != nil {
		return jobapp.MergedResult{}, splitErr
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
			res, err := o.runSubUnitWithFailover(ctx, job, a.SubUnit, a.Replica, headroom)
			results[i] = res
			errs[i] = err
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

// runSubUnitWithFailover drives one SubUnit to completion against
// `first`, reassigning to other eligible replicas on failure/staleness
// as long as job.EffectiveResume() allows it. tried accumulates every
// replica-id attempted for this SubUnit so a reassignment never retries
// one that already failed/stalled on it.
func (o *Orchestrator) runSubUnitWithFailover(ctx context.Context, job jobapp.JobDescriptor, su jobapp.SubUnit, first jobapp.ModuleEndpoint, headroom scheduler.CapacityHeadroom) (jobapp.SubUnitResult, error) {
	tried := map[string]bool{}
	current := first

	for {
		tried[current.ReplicaID] = true

		res, err := o.callProcessSubUnit(ctx, job, su, current)
		if err == nil {
			o.Registry.ClearSubUnit(job.OwningModule, current.ReplicaID, su.SubUnitID)
			return res, nil
		}

		o.Registry.ClearSubUnit(job.OwningModule, current.ReplicaID, su.SubUnitID)
		_ = o.Audit.Record(jobapp.AuditRecord{
			Action: "subunit_stalled_or_failed",
			Detail: fmt.Sprintf("job=%s sub_unit=%s replica=%s error=%s", job.JobID, su.SubUnitID, current.ReplicaID, err),
		})

		if job.EffectiveResume() == jobapp.ResumeTransactional {
			_ = o.Audit.Record(jobapp.AuditRecord{
				Action: "subunit_failed_transactional_no_reassign",
				Detail: fmt.Sprintf("job=%s sub_unit=%s replica=%s -- resume semantics forbid reassigning an already-dispatched transactional sub-unit", job.JobID, su.SubUnitID, current.ReplicaID),
			})
			return jobapp.SubUnitResult{}, fmt.Errorf("transactional sub-unit %s failed on replica %s, not reassigned: %w", su.SubUnitID, current.ReplicaID, err)
		}

		next, ok := o.pickReassignmentCandidate(job, tried)
		if !ok {
			return jobapp.SubUnitResult{}, fmt.Errorf("sub_unit %s exhausted eligible replicas after failure on %s: %w", su.SubUnitID, current.ReplicaID, err)
		}
		_ = o.Audit.Record(jobapp.AuditRecord{
			Action: "reassign",
			Detail: fmt.Sprintf("job=%s sub_unit=%s from_replica=%s to_replica=%s", job.JobID, su.SubUnitID, current.ReplicaID, next.ReplicaID),
		})
		current = next
	}
}

// pickReassignmentCandidate returns the best live replica for
// job.OwningModule that hasn't already been tried for this SubUnit and
// -- when job.SovereigntyZone is set -- is either in that same zone or
// declares no zone restriction of its own (Group A item 6: candidates
// are filtered through sovereignty/data-residency before capacity is
// even considered). Takes a fresh LookupPool snapshot rather than
// reusing the pool from the original assignment, so reassignment always
// sees current load/liveness, not a stale view from when the job
// started.
func (o *Orchestrator) pickReassignmentCandidate(job jobapp.JobDescriptor, tried map[string]bool) (jobapp.ModuleEndpoint, bool) {
	pool := o.Registry.LookupPool(job.OwningModule)
	var bestEp jobapp.ModuleEndpoint
	var bestScore float64
	found := false
	for _, r := range pool {
		if tried[r.Endpoint.ReplicaID] {
			continue
		}
		if job.SovereigntyZone != "" && r.Endpoint.Zone != "" && r.Endpoint.Zone != job.SovereigntyZone {
			continue
		}
		remaining := 0.0
		for _, pct := range r.CurrentLoad {
			remaining += 100 - pct
		}
		score := -remaining // no live capacity-headroom signal available at this call site yet; remaining load alone still distinguishes an idle replica from a busy one
		if !found || score > bestScore || (score == bestScore && r.Endpoint.ReplicaID < bestEp.ReplicaID) {
			bestEp = r.Endpoint
			bestScore = score
			found = true
		}
	}
	return bestEp, found
}

// callProcessSubUnit sends one SubUnit to one specific replica and waits
// for either a result, a hard failure, or staleness-triggered
// cancellation (no progress activity for DefaultSubUnitStaleAfter). It
// marks the SubUnit assigned in the registry (starting its staleness
// clock) before dispatching, and always stops its own watcher goroutine
// before returning.
func (o *Orchestrator) callProcessSubUnit(ctx context.Context, job jobapp.JobDescriptor, su jobapp.SubUnit, replica jobapp.ModuleEndpoint) (jobapp.SubUnitResult, error) {
	_ = o.Registry.AssignSubUnit(job.OwningModule, replica.ReplicaID, su.SubUnitID)

	subCtx, cancel := context.WithCancel(ctx)
	staleDetected := make(chan struct{})
	watcherDone := make(chan struct{})
	go o.watchSubUnitStaleness(subCtx, job.OwningModule, replica.ReplicaID, su.SubUnitID, cancel, staleDetected, watcherDone)
	defer func() {
		cancel()
		<-watcherDone
	}()

	reqBody, err := json.Marshal(processSubUnitRequest{
		Strategy:  job.OwningModule,
		ReplicaID: replica.ReplicaID,
		SubUnit:   su,
	})
	if err != nil {
		return jobapp.SubUnitResult{}, err
	}

	var resp processSubUnitResponse
	callErr := o.callModule(subCtx, replica, "/process-subunit", reqBody, &resp)
	if callErr != nil {
		select {
		case <-staleDetected:
			return jobapp.SubUnitResult{}, fmt.Errorf("replica %s went stale on sub_unit %s (no activity for %s): %w", replica.ReplicaID, su.SubUnitID, DefaultSubUnitStaleAfter, callErr)
		default:
			return jobapp.SubUnitResult{}, callErr
		}
	}
	return resp.Result, nil
}

// watchSubUnitStaleness polls the registry's last-activity timestamp for
// one (strategy, replica, subUnit) and cancels cancel() the moment it's
// been more than DefaultSubUnitStaleAfter since that SubUnit showed any
// activity (its initial assignment or a /progress report) -- signalling
// via staleDetected so the caller can distinguish "stale, cancelled by
// us" from any other cancellation/failure reason. Exits (closing
// watcherDone) as soon as ctx itself is done, for whatever reason.
func (o *Orchestrator) watchSubUnitStaleness(ctx context.Context, strategyName, replicaID, subUnitID string, cancel context.CancelFunc, staleDetected, watcherDone chan struct{}) {
	defer close(watcherDone)
	ticker := time.NewTicker(subUnitActivityPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lastActivity, tracked := o.Registry.SubUnitActivity(strategyName, replicaID, subUnitID)
			if !tracked {
				// Already cleared (completed/failed/reassigned elsewhere) --
				// nothing left for this watcher to do.
				return
			}
			if time.Since(lastActivity) > DefaultSubUnitStaleAfter {
				close(staleDetected)
				cancel()
				return
			}
		}
	}
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

		if ctx.Err() != nil {
			return ctx.Err()
		}
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
