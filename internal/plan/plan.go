// Package plan is heain-job's AI: a split planner that learns, per
// capability, how long work takes per byte from the sub-units it has
// already seen complete, and decides how many sub-units a new job should
// be split into (design note "heain-job" Group A items 1, 4 and 9;
// author decision 2026-10-06: a learned split planner).
//
// The model is deliberately small and explainable: an exponentially
// weighted mean of seconds per byte (and its spread) per capability.
// Every decision is returned with the factors that produced it, so the
// caller can file a reasoning record (spec 04).
package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Params are the planner's fixed settings.
type Params struct {
	TargetUnitSeconds float64 // aim for sub-units of about this length (default 20)
	MinUnitBytes      int     // never cut smaller than this (default 1)
	MaxPartsPerWorker int     // at most this many sub-units per live worker (default 2)
	Alpha             float64 // learning rate of the moving average (default 0.3)
}

func (p Params) withDefaults() Params {
	if p.TargetUnitSeconds <= 0 {
		p.TargetUnitSeconds = 20
	}
	if p.MinUnitBytes <= 0 {
		p.MinUnitBytes = 1
	}
	if p.MaxPartsPerWorker <= 0 {
		p.MaxPartsPerWorker = 2
	}
	if p.Alpha <= 0 || p.Alpha > 1 {
		p.Alpha = 0.3
	}
	return p
}

// Stat is what the planner has learned about one capability.
type Stat struct {
	SecPerByte   float64 `json:"sec_per_byte"`
	Var          float64 `json:"var"` // moving variance of sec_per_byte
	Observations int     `json:"observations"`
}

// Planner holds the learned statistics, persisted as JSON in its state
// file (numbers only -- no job data).
type Planner struct {
	P     Params
	mu    sync.Mutex
	stats map[string]Stat
	file  string
}

// Open loads (or starts) the planner's state in dir.
func Open(dir string, p Params) (*Planner, error) {
	pl := &Planner{P: p.withDefaults(), stats: map[string]Stat{}, file: filepath.Join(dir, "planner-stats.json")}
	raw, err := os.ReadFile(pl.file)
	if err == nil {
		if err := json.Unmarshal(raw, &pl.stats); err != nil {
			return nil, fmt.Errorf("plan: %s: %w", pl.file, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return pl, nil
}

// Decision is one split decision and why.
type Decision struct {
	Parts      int
	Confidence float64
	Summary    string
	Factors    map[string]float64
	ModelHash  string // identifies the parameters and the learned state used
}

// Decide chooses the number of sub-units for a job of sizeBytes on
// capability, with workers live providers of its sub-unit capability.
func (pl *Planner) Decide(capability string, sizeBytes, workers int) Decision {
	pl.mu.Lock()
	st, known := pl.stats[capability]
	hash := pl.hashLocked()
	pl.mu.Unlock()
	p := pl.P
	if workers < 1 {
		workers = 1
	}
	maxParts := workers * p.MaxPartsPerWorker
	if bySize := sizeBytes / p.MinUnitBytes; bySize < maxParts {
		maxParts = int(math.Max(1, float64(bySize)))
	}
	parts := workers // no history yet: one sub-unit per live worker
	predicted := 0.0
	if known && st.Observations > 0 {
		predicted = st.SecPerByte * float64(sizeBytes)
		parts = int(math.Ceil(predicted / p.TargetUnitSeconds))
		if parts < 1 {
			parts = 1
		}
	}
	if parts > maxParts {
		parts = maxParts
	}
	if parts < 1 {
		parts = 1
	}
	conf := 0.5
	summary := fmt.Sprintf("no history for %s yet: one sub-unit per live worker (%d), capped at %d", capability, workers, maxParts)
	if known && st.Observations > 0 {
		conf = math.Min(0.95, 0.5+0.05*float64(st.Observations))
		summary = fmt.Sprintf("learned %.3g s/byte over %d sub-units: predicted %.1f s for %d bytes, target %.0f s per sub-unit -> %d (cap %d)",
			st.SecPerByte, st.Observations, predicted, sizeBytes, p.TargetUnitSeconds, parts, maxParts)
	}
	return Decision{Parts: parts, Confidence: conf, Summary: summary, ModelHash: hash,
		Factors: map[string]float64{"size_bytes": float64(sizeBytes), "live_workers": float64(workers), "observations": float64(st.Observations),
			"sec_per_byte": st.SecPerByte, "predicted_seconds": predicted, "target_unit_seconds": p.TargetUnitSeconds, "max_parts": float64(maxParts)}}
}

// Observe learns from one finished sub-unit.
func (pl *Planner) Observe(capability string, sizeBytes int, seconds float64) {
	if sizeBytes <= 0 || seconds < 0 {
		return
	}
	x := seconds / float64(sizeBytes)
	pl.mu.Lock()
	st := pl.stats[capability]
	if st.Observations == 0 {
		st.SecPerByte = x
	} else {
		d := x - st.SecPerByte
		st.SecPerByte += pl.P.Alpha * d
		st.Var = (1 - pl.P.Alpha) * (st.Var + pl.P.Alpha*d*d)
	}
	st.Observations++
	pl.stats[capability] = st
	pl.mu.Unlock()
}

// Stats returns a copy of what has been learned.
func (pl *Planner) Stats() map[string]Stat {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	out := map[string]Stat{}
	for k, v := range pl.stats {
		out[k] = v
	}
	return out
}

// Save writes the learned state (atomically).
func (pl *Planner) Save() error {
	pl.mu.Lock()
	raw, err := json.MarshalIndent(pl.stats, "", "  ")
	pl.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := pl.file + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, pl.file)
}

func (pl *Planner) hashLocked() string {
	keys := make([]string, 0, len(pl.stats))
	for k := range pl.stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	fmt.Fprintf(h, "heain-job split planner v1|%v|", pl.P)
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%v;", k, pl.stats[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}
