// Package audit is heain-job's own, separate audit log.
//
// Decision (2026-09-30): heain-job keeps its own audit log at Layer 3,
// rather than writing into heain-core's internal/audit (Layer 2).
// Rationale: internal/audit is scoped to Layer 2 protocol/governance
// events (P5 decisions, Duty Profile changes, promotion/demotion) that
// every node in the Raft-replicated substrate needs a consistent view of;
// heain-job's own split/reassign/merge decisions are Layer 3 orchestration
// detail with a different audience (Application AI operators, not Layer 2
// network operators) and a different natural scope (per-job-lineage, not
// per-Zone). Keeping them separate preserves the same Layer 2/Layer 3
// boundary already established throughout the design note. A deployment
// that wants one combined trail may forward these records into the same
// sink as heain-core's own audit log — that's a deployment choice, not
// something heain-job assumes.
package audit

import (
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/heainframework/heain-job/internal/jobapp"
)

// Logger writes jobapp.AuditRecord values as newline-delimited JSON to an
// io.Writer.
//
// v1 is intentionally minimal (append-only stream, no retention/export
// policy yet — see "Not yet decided" in the schema design note). A real
// deployment points this at a durable file or forwards it to whatever
// Layer 3 storage backend the deployment has chosen, per the "Durable/
// external storage" design in heain-core's own design notes.
type Logger struct {
	mu sync.Mutex
	w  io.Writer
}

// NewLogger returns a Logger writing to w.
func NewLogger(w io.Writer) *Logger {
	return &Logger{w: w}
}

// Record appends one AuditRecord, filling in At if it's zero.
func (l *Logger) Record(rec jobapp.AuditRecord) error {
	if rec.At.IsZero() {
		rec.At = time.Now().UTC()
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	enc := json.NewEncoder(l.w)
	return enc.Encode(rec)
}
