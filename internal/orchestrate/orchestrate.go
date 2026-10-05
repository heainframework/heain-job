// Package orchestrate is heain-job's workflow (design note "heain-job",
// Group A; spec 02 §5: split/merge and fan-out are an app concern, done
// through the same App API). For a job on capability C it
//
//  1. asks the planner (the AI) how many sub-units to make, and files a
//     reasoning record for that decision;
//  2. calls the owning module's C.split (direct, over mTLS);
//  3. submits every sub-unit as a job on C.unit through core (P1-P4:
//     core encrypts, places, leases, retries and reassigns them);
//  4. waits for all of them, then calls the module's C.merge;
//  5. learns how long the sub-units took.
//
// The module contract (any module that wants orchestration):
//
//	capability C.split  direct  POST /v1/split/C  {"parts":N,"input_b64":...} -> {"units":[{"id","payload_b64"}],"state_b64"}
//	capability C.unit   job     payload = one unit's payload_b64, output = its result
//	capability C.merge  direct  POST /v1/merge/C  {"units":[{"id","output_b64"}],"state_b64"} -> {"output_b64"}
//
// heain-job's manifest declares these with uses[] patterns
// ({app: "*", capabilities: ["*.split", "*.unit", "*.merge"]}).
package orchestrate

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/heainframework/heain-job/internal/plan"
	"github.com/heainframework/heain-sdk/heain"
)

// Request is the payload of a job.orchestrate job.
type Request struct {
	Capability string `json:"capability"`  // the module capability C, e.g. "video.conform"
	Input      []byte `json:"input_b64"`   // the whole input (base64 in JSON)
	Resume     string `json:"resume"`      // idempotent (default) | transactional
	OriginZone string `json:"origin_zone"` // P7: sub-units carry the job's zone
	MaxParts   int    `json:"max_parts"`   // optional upper bound from the submitter
}

// A transactional (exactly-once) job is fanned out with sub-units submitted
// with max_attempts 1: core runs each at most once, and a failed or expired
// sub-unit stops (retries_exhausted, P5 to an Approver) instead of being
// run again; the orchestration then fails without merging. (Until core
// v1.3 step 4a-2 added per-job retry control, such jobs were refused.)

var capRe = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*$`)

// Orchestrator runs orchestration jobs.
type Orchestrator struct {
	App     *heain.App
	Planner *plan.Planner
	// Self is heain-job's own capability (for its reasoning records).
	Self string
	// UnitTimeout bounds the wait for all sub-units.
	UnitTimeout time.Duration
	Logf        func(string, ...any)
}

type unit struct {
	ID      string `json:"id"`
	Payload []byte `json:"payload_b64,omitempty"`
	Output  []byte `json:"output_b64,omitempty"`
}

// Run executes one orchestration job and returns the merged output.
func (o *Orchestrator) Run(ctx context.Context, job *heain.Job) ([]byte, error) {
	var req Request
	if err := json.Unmarshal(job.Payload, &req); err != nil {
		return nil, heain.Permanent(fmt.Errorf("payload is not an orchestration request: %w", err))
	}
	defer wipe(req.Input)
	if !capRe.MatchString(req.Capability) {
		return nil, heain.Permanent(fmt.Errorf("capability %q is not a capability name", req.Capability))
	}
	attempts := 0 // policy p3.max_retry
	switch req.Resume {
	case "", "idempotent":
	case "transactional":
		attempts = 1
	default:
		return nil, heain.Permanent(fmt.Errorf("resume %q is not idempotent or transactional", req.Resume))
	}
	c := req.Capability
	splitters, err := o.App.Discover(ctx, c+".split", 0)
	if err != nil {
		return nil, err
	}
	module := ""
	for _, in := range splitters {
		if in.Execution == "direct" {
			module = in.AppID
			break
		}
	}
	if module == "" {
		return nil, heain.Permanent(fmt.Errorf("no live module offers %s.split", c))
	}
	workers, err := o.App.Discover(ctx, c+".unit", 0)
	if err != nil {
		return nil, err
	}
	live := 0
	for _, w := range workers {
		if w.AppID == module {
			live++
		}
	}
	if live == 0 {
		return nil, fmt.Errorf("no live instance of %s offers %s.unit", module, c) // retryable: one may come back
	}

	// 1. the AI decides how to split, and says why
	d := o.Planner.Decide(c, len(req.Input), live)
	if req.MaxParts > 0 && d.Parts > req.MaxParts {
		d.Parts = req.MaxParts
		d.Summary += fmt.Sprintf("; submitter limit %d", req.MaxParts)
	}
	factors := make([]heain.Factor, 0, len(d.Factors))
	keys := make([]string, 0, len(d.Factors))
	for k := range d.Factors {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		factors = append(factors, heain.Factor{Name: k, Value: d.Factors[k]})
	}
	conf := d.Confidence
	if _, err := o.App.Reason(ctx, heain.Decision{Capability: o.Self, Input: req.Input, Decision: fmt.Sprintf("split %s into %d", c, d.Parts),
		Value: map[string]any{"parts": d.Parts, "module": module}, Confidence: &conf, Summary: d.Summary, Factors: factors,
		Role: "advisory", ModelSHA256: d.ModelHash, Runtime: "heain-job planner (go)"}); err != nil {
		return nil, err
	}

	// 2. split (owning module)
	var sp struct {
		Units []unit `json:"units"`
		State []byte `json:"state_b64,omitempty"`
	}
	if _, err := o.App.Call(ctx, heain.CallSpec{App: module, Capability: c + ".split", Method: "POST", Path: "/v1/split/" + c,
		Body: map[string]any{"parts": d.Parts, "input_b64": req.Input}, Out: &sp}); err != nil {
		return nil, fmt.Errorf("split: %w", err)
	}
	if len(sp.Units) == 0 {
		return nil, heain.Permanent(fmt.Errorf("%s.split returned no sub-units", c))
	}

	// 3. sub-units through core
	type res struct {
		i   int
		out []byte
		err error
	}
	results := make([]res, len(sp.Units))
	var wg sync.WaitGroup
	timeout := o.UnitTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for i := range sp.Units {
		wg.Add(1)
		go func(i int, u unit) {
			defer wg.Done()
			t, err := o.App.Submit(wctx, heain.JobRequest{Capability: c + ".unit", OriginZone: req.OriginZone, Payload: u.Payload,
				IdempotencyKey: job.TicketID + ":" + u.ID, Classification: heain.Classification{Delivery: "IMMEDIATE"}, MaxAttempts: attempts})
			wipe(u.Payload)
			if err != nil {
				results[i] = res{i: i, err: fmt.Errorf("submit %s: %w", u.ID, err)}
				return
			}
			st, err := o.App.WaitJob(wctx, t.TicketID)
			switch {
			case err != nil:
				results[i] = res{i: i, err: fmt.Errorf("sub-unit %s: %w", u.ID, err)}
			case st.State != heain.JobCompleted && st.State != heain.JobDelivered:
				results[i] = res{i: i, err: heain.Permanent(fmt.Errorf("sub-unit %s ended %s", u.ID, st.State))}
			default:
				results[i] = res{i: i, out: st.Output}
				if st.CompletedAt != nil {
					o.Planner.Observe(c, len(u.Payload), st.CompletedAt.Sub(st.CreatedAt).Seconds())
				}
			}
		}(i, sp.Units[i])
	}
	wg.Wait()
	_ = o.Planner.Save()
	merged := make([]unit, len(sp.Units))
	for i, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		merged[i] = unit{ID: sp.Units[i].ID, Output: r.out}
	}
	defer func() {
		for _, u := range merged {
			wipe(u.Output)
		}
	}()

	// 4. merge (owning module)
	var mr struct {
		Output []byte `json:"output_b64"`
	}
	if _, err := o.App.Call(ctx, heain.CallSpec{App: module, Capability: c + ".merge", Method: "POST", Path: "/v1/merge/" + c,
		Body: map[string]any{"units": merged, "state_b64": sp.State}, Out: &mr}); err != nil {
		return nil, fmt.Errorf("merge: %w", err)
	}
	if o.Logf != nil {
		o.Logf("heain-job: %s -> %d sub-units on %s, merged %d bytes", job.TicketID, len(sp.Units), module, len(mr.Output))
	}
	return mr.Output, nil
}

func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
