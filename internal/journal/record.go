package journal

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

// RetentionPolicy is the durable, user-declared traceability window.
//
// Mode "sequence" keeps every snapshot at sequence Horizon or newer.
// Mode "duration" keeps snapshots whose commit time is at least Duration
// behind the current head; sequences only map to time through the durable
// per-record CommitTime stamps, so an idle window never advances the boundary
// and the number of sequences is never interpreted as elapsed time.
type RetentionPolicy struct {
	Mode       string `json:"mode"`
	Horizon    uint64 `json:"horizon,omitempty"`
	DurationNS int64  `json:"duration_ns,omitempty"`
}

type Record struct {
	Format      int                 `json:"format"`
	Sequence    uint64              `json:"sequence"`
	Kind        string              `json:"kind"`
	Transaction string              `json:"transaction,omitempty"`
	Relation    *catalog.Relation   `json:"relation,omitempty"`
	Index       *catalog.Index      `json:"index,omitempty"`
	Writes      []versions.Mutation `json:"writes,omitempty"`
	// Horizon is the physically reclaimed low water mark on vacuum records.
	Horizon uint64 `json:"horizon,omitempty"`
	// CommitTime is the wall-clock instant the durable record was published,
	// recorded once and replayed verbatim, so time windows stay anchored
	// across restarts and never restart counting from zero.
	CommitTime string `json:"commit_time,omitempty"`
	// Retention records carry the declared logical retention policy.
	Policy *RetentionPolicy `json:"policy,omitempty"`
	// LogicalHorizon is the admission boundary in force when a vacuum record
	// was published. It never moves backwards and always leads the physical
	// horizon, which may lag because of long-lived readers.
	LogicalHorizon uint64 `json:"logical_horizon,omitempty"`
	Removed        int    `json:"removed,omitempty"`
	Retained       int    `json:"retained,omitempty"`
}
