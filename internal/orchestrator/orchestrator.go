package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-job/internal/registry"
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
