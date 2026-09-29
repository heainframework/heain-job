// Package jobapp defines heain-job's Application Profile schema: the
// Layer 3 orchestration-facet types confirmed in heain-core's
// design-notes/n-tier-generalization.md ("Item 2 resolved" and
// "heain-job cross-industry universality requirements — Group A").
//
// heain-job never imports heain-core. It talks to a running heain-core
// node only over HTTP/mTLS (see internal/coreclient), using heain-core's
// already-implemented P1-P4 endpoints (/ingest, /dispatch, /execute,
// /staging/...) as its transport and execution primitives.
package jobapp

import "time"

// ResumeSemantics tells heain-job's reassignment logic whether a
// sub-unit may be safely redone from scratch (Idempotent) or must never
// be processed more than once (Transactional) — Group A item 5.
type ResumeSemantics string

const (
	ResumeIdempotent    ResumeSemantics = "idempotent"
	ResumeTransactional ResumeSemantics = "transactional"
)

// SplitStrategy decomposes a job into sub-units. The decomposition logic
// itself is supplied by whichever data-type module (heain-image,
// heain-video, heain-sound, heain-sd, ...) owns the job — heain-job only
// defines and drives this generic contract. Group A item 1.
type SplitStrategy interface {
	// Split returns the sub-units a job decomposes into. A strategy may
	// legitimately return a single sub-unit (the whole-job-to-one-worker
	// case), decided dynamically by the caller's own load evaluation,
	// not by a fixed rule based on job size.
	Split(job JobDescriptor) ([]SubUnit, error)
}

// MergeStrategy combines completed sub-units back into one result. The
// merge logic itself belongs to the owning data-type module. Group A
// item 3.
type MergeStrategy interface {
	Merge(job JobDescriptor, completed []SubUnitResult) (MergedResult, error)
}

// JobDescriptor is heain-job's view of a job it orchestrates. It is
// deliberately thin — the actual job payload/semantics live behind
// heain-core's own P1 Envelope (ticket_id), which heain-job references
// but never reinterprets.
type JobDescriptor struct {
	JobID           string
	TicketID        string // heain-core P1 ingestion ticket, from /ingest
	OwningModule    string // e.g. "heain-video"
	Resume          ResumeSemantics
	PriorityClass   string // Group A item 7 — reuses Duty Profile's own priority concept
	SovereigntyZone string // Group A item 6 — filters reassignment candidates via heain-core's P7 gate
}

// SubUnit is one decomposed piece of a job, dispatched to a worker via
// heain-core's own P2 /dispatch endpoint (see internal/coreclient).
type SubUnit struct {
	SubUnitID string
	JobID     string
	// Payload is opaque to heain-job; only the owning module's
	// SplitStrategy/MergeStrategy implementation interprets it.
	Payload []byte
}

// SubUnitResult is a completed sub-unit's output, opaque to heain-job.
type SubUnitResult struct {
	SubUnitID string
	Output    []byte
}

// MergedResult is a completed job's final, merged output.
type MergedResult struct {
	JobID  string
	Output []byte
}

// Checkpoint is heain-job's own orchestration-level bookkeeping for a
// sub-unit in flight — progress fraction, timestamp, and current holder.
// The checkpoint's *content* (e.g. "encoded through frame N") is opaque
// to heain-job and is written/interpreted only by the owning module.
// Group A item 2.
type Checkpoint struct {
	SubUnitID     string
	WorkerID      string
	Progress      float64 // 0.0-1.0
	At            time.Time
	OpaquePayload []byte
}

// AuditRecord captures one orchestration decision (split, reassign,
// merge-complete) for heain-job's own audit trail. Group A item 8 — this
// mirrors heain-core's internal/audit shape but is heain-job's own
// record, since heain-job has no access to heain-core's internal
// package; a deployment may forward these into the same audit sink if
// it wants one combined trail.
type AuditRecord struct {
	JobID  string
	Action string // "split" | "reassign" | "merge_complete"
	Detail string
	At     time.Time
}
