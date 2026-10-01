package jobapp

// ProgressStatus is a replica's self-reported state for one SubUnit it
// is executing (Stage B — see design-notes/n-tier-generalization.md
// "Progress reporting: replicas self-report, not polled from queue
// depth").
type ProgressStatus string

const (
	ProgressRunning ProgressStatus = "RUNNING"
	ProgressDone    ProgressStatus = "DONE"
	ProgressFailed  ProgressStatus = "FAILED"
)

// ProgressReport is the wire shape a replica POSTs to heain-job's
// /progress endpoint while it works on one SubUnit. RUNNING reports
// carry PercentComplete; DONE/FAILED clear that SubUnit from the
// replica's live load regardless of the percentage given.
type ProgressReport struct {
	ReplicaID       string         `json:"replica_id"`
	StrategyName    string         `json:"strategy_name"`
	SubUnitID       string         `json:"sub_unit_id"`
	PercentComplete float64        `json:"percent_complete"`
	Status          ProgressStatus `json:"status"`
}
