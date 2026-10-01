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

// DefaultResumeSemantics is applied whenever a JobDescriptor omits its
// own Resume value. Decided 2026-09-30: most media-processing sub-units
// tolerate redoing a reassigned chunk with no ill effect, so the cheaper
// guarantee is the default; a module whose duty genuinely needs
// exactly-once semantics must opt into ResumeTransactional explicitly.
const DefaultResumeSemantics = ResumeIdempotent

// EffectiveResume returns j.Resume, or DefaultResumeSemantics if unset.
func (j JobDescriptor) EffectiveResume() ResumeSemantics {
	if j.Resume == "" {
		return DefaultResumeSemantics
	}
	return j.Resume
}

// ModuleEndpoint is what a data-type module declares about itself when it
// registers a SplitStrategy/MergeStrategy implementation with heain-job:
// which strategy name it implements, its own base URL for the /split and
// /merge callback endpoints it exposes, and the shared-secret token
// heain-job must present when calling it.
//
// Registration mechanism decision (2026-09-30): HTTP callback, not an
// in-process plug-in — each Layer 3 module is its own container, so a
// SplitStrategy/MergeStrategy cannot be linked into heain-job's binary.
// Auth decision (2026-09-30): shared-secret token per module, not mTLS —
// heain-job and every module run inside the same internal Docker network,
// never exposed to the internet, so a per-module secret is enough to stop
// a wrong/unrelated container from calling another module's endpoint by
// mistake, without standing up a second PKI alongside heain-core's
// existing node-to-node mTLS (a different concern — that authenticates
// Layer 2 network identity, not Layer 3 container-to-container calls on a
// trusted internal network).
//
// ReplicaID (Stage B, 2026-10-01, see design-notes/n-tier-generalization.md
// "heain-job's load-aware scheduler"): distinguishes multiple concurrent
// instances of the same module/strategy from each other, so the registry
// can hold a pool of replicas per strategy instead of exactly one. Empty
// (the default) means "this module never runs more than one instance of
// itself" — the original, still-fully-supported single-instance case
// (e.g. heain-image today) — and is backward compatible with every
// existing caller: an empty ReplicaID is just one more replica, keyed by
// the empty string.
//
// Zone (added for reassignment-time P7 sovereignty filtering — Group A
// item 6, "heain-job cross-industry universality requirements"): the
// data-residency zone this replica physically runs in, matching
// heain-core's own P7 Sovereignty-Aware Broadcast concept. Empty (the
// default) means "unrestricted" — every existing module/caller that
// never sets this is unaffected, and a job with no SovereigntyZone set
// is never filtered by zone at all. Only a job that explicitly declares
// a SovereigntyZone narrows candidate replicas to ones whose own Zone
// matches (or is itself empty, meaning the replica hasn't declared a
// restriction and is eligible for any zone) — this is a reassignment-time
// eligibility filter, not a new consensus mechanism, reusing the exact
// zone-matching semantics already established for heain-core's own
// sovereignty gate.
type ModuleEndpoint struct {
	ModuleName   string `json:"module_name"`
	StrategyName string `json:"strategy_name"`
	BaseURL      string `json:"base_url"`
	Token        string `json:"token"`
	ReplicaID    string `json:"replica_id,omitempty"`
	Zone         string `json:"zone,omitempty"`
}
