package engine

import (
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"time"
)

// RetentionRequest is the user-declared traceability window. Exactly one mode
// must be supplied: a sequence boundary or a time duration.
type RetentionRequest struct {
	Mode       string `json:"mode,omitempty"`
	Horizon    uint64 `json:"horizon,omitempty"`
	DurationNS int64  `json:"duration_ns,omitempty"`
}

// Pin names a still-active reader that blocks physical reclamation past its
// snapshot. Logical admission is never blocked by a pin; the promise simply
// explains why the physical boundary lags.
type Pin struct {
	Transaction string `json:"transaction"`
	Snapshot    uint64 `json:"snapshot"`
	Started     string `json:"started"`
	ReadOnly    bool   `json:"read_only"`
}

// RetentionStatus is the auditable retention contract. It reports the declared
// policy, the derived logical admission boundary, the physical reclamation
// boundary, the pins separating them, and the space accounted at each stage.
type RetentionStatus struct {
	Policy journal.RetentionPolicy `json:"retention_policy"`
	// WindowStart / WindowEnd bound the promised readable sequences.
	WindowStart uint64 `json:"window_start"`
	WindowEnd   uint64 `json:"window_end"`
	// CutoffTime and AnchorSequence explain how a duration window was mapped
	// onto sequence space using durable commit timestamps.
	CutoffTime     string `json:"cutoff_time,omitempty"`
	AnchorSequence uint64 `json:"anchor_sequence,omitempty"`
	// PhysicalHorizon is the last sequence actually reclaimed through vacuum.
	PhysicalHorizon uint64 `json:"physical_horizon"`
	// PinnedHorizon is the furthest a vacuum could advance right now given the
	// active readers; it equals WindowStart when nothing pins.
	PinnedHorizon uint64 `json:"pinned_horizon"`
	Pins          []Pin  `json:"pins"`
	// ExpiredVersions are versions already outside the logical window;
	// PurgedVersions are the subset that has also been physically removed.
	ExpiredVersions    int `json:"expired_versions"`
	ExpiredButRetained int `json:"expired_but_retained"`
	PurgedVersions     int `json:"purged_versions_since_open"`
	RetainedVersions   int `json:"retained_versions"`
	RemovableNow       int `json:"removable_now"`
}

// RetentionUpdate extends the status with the explanation attached to a policy
// change, including any monotonicity clamping performed under space pressure.
type RetentionUpdate struct {
	RetentionStatus
	Adjusted bool   `json:"adjusted"`
	Reason   string `json:"reason,omitempty"`
	Sequence uint64 `json:"sequence"`
}

// resolveLogical maps the declared policy onto a sequence-space admission
// boundary at the given instant. It never returns a value below the current
// retainFrom: the traceability promise can only advance, never restart.
func (d *DB) resolveLogical(p journal.RetentionPolicy, now time.Time) uint64 {
	candidate := d.retainFrom
	switch p.Mode {
	case "sequence":
		candidate = p.Horizon
	case "duration":
		cutoff := now.Add(-time.Duration(p.DurationNS))
		// Anchor at the newest commit still inside the window: every earlier
		// in-window commit must remain readable, while anything older is
		// inadmissible. Records without timestamps (older journal segments
		// removed by checkpoint/restore) cannot be used as time evidence.
		anchor := uint64(0)
		found := false
		for _, r := range d.wal.Records {
			if r.CommitTime == "" {
				continue
			}
			ts, err := time.Parse(time.RFC3339Nano, r.CommitTime)
			if err != nil {
				continue
			}
			if !ts.Before(cutoff) && r.Sequence > anchor {
				anchor = r.Sequence
				found = true
			}
		}
		if !found {
			anchor = d.sequence
		}
		candidate = anchor
	}
	if candidate < d.retainFrom {
		candidate = d.retainFrom
	}
	if candidate > d.sequence {
		candidate = d.sequence
	}
	return candidate
}

func (d *DB) pins() ([]Pin, uint64) {
	pins := make([]Pin, 0)
	pinned := d.retainFrom
	for _, tx := range d.transactions {
		if tx.Snapshot < pinned {
			pinned = tx.Snapshot
		}
		pins = append(pins, Pin{Transaction: tx.ID, Snapshot: tx.Snapshot, Started: tx.Started, ReadOnly: tx.ReadOnly})
	}
	sort.Slice(pins, func(i, j int) bool { return pins[i].Snapshot < pins[j].Snapshot })
	return pins, pinned
}

func (d *DB) retentionStatusLocked() RetentionStatus {
	pins, pinned := d.pins()
	_, retained := d.store.Count()
	// Versions outside the window still held because no vacuum has reached
	// them yet. Anchor versions kept to reconstruct a row at the window start
	// are not removable and therefore not counted as expired.
	expiredPresent := d.store.CountRemovable(d.retainFrom)
	removable := d.store.CountRemovable(pinned)
	status := RetentionStatus{
		Policy:             d.policy,
		WindowStart:        d.retainFrom,
		WindowEnd:          d.sequence,
		PhysicalHorizon:    d.horizon,
		PinnedHorizon:      pinned,
		Pins:               pins,
		ExpiredVersions:    expiredPresent + d.purgedVersions,
		ExpiredButRetained: expiredPresent,
		PurgedVersions:     d.purgedVersions,
		RetainedVersions:   retained,
		RemovableNow:       removable,
	}
	if d.policy.Mode == "duration" {
		now := time.Now().UTC()
		cutoff := now.Add(-time.Duration(d.policy.DurationNS))
		status.CutoffTime = cutoff.Format(time.RFC3339Nano)
		status.AnchorSequence = d.retainFrom
	}
	return status
}

func (d *DB) RetentionStatus() (RetentionStatus, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return RetentionStatus{}, err
	}
	return d.retentionStatusLocked(), nil
}

// SetRetention publishes a new traceability promise. Shortening the window to
// relieve space pressure is allowed, but the effective logical boundary can
// never move backwards; such a request is clamped and reported with a reason
// rather than silently widening or silently ignoring the request.
func (d *DB) SetRetention(req RetentionRequest) (RetentionUpdate, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return RetentionUpdate{}, err
	}
	policy := journal.RetentionPolicy{Mode: req.Mode, Horizon: req.Horizon, DurationNS: req.DurationNS}
	switch policy.Mode {
	case "sequence":
		if policy.Horizon > d.sequence {
			return RetentionUpdate{}, fault("invalid", "retention horizon cannot exceed current sequence %d", d.sequence)
		}
	case "duration":
		if policy.DurationNS <= 0 {
			return RetentionUpdate{}, fault("invalid", "duration_ns must be positive")
		}
	default:
		return RetentionUpdate{}, fault("invalid", "mode must be \"sequence\" or \"duration\"")
	}
	before := d.retainFrom
	resolved := d.resolveLogical(policy, time.Now().UTC())
	adjusted := false
	reason := ""
	if resolved < before {
		resolved = before
		adjusted = true
		reason = "requested window would move the admission boundary backwards; clamped to the existing boundary"
	} else if policy.Mode == "sequence" && policy.Horizon < before {
		// Even when an earlier clamp already moved the effective boundary to
		// exactly this position, the request itself still asked to go back.
		adjusted = true
		reason = "requested window would move the admission boundary backwards; clamped to the existing boundary"
	}
	if err := d.publish(journal.Record{Kind: "retention", Policy: &policy, LogicalHorizon: resolved}); err != nil {
		return RetentionUpdate{}, err
	}
	d.policy = policy
	d.retainFrom = resolved
	update := RetentionUpdate{RetentionStatus: d.retentionStatusLocked(), Sequence: d.sequence}
	update.Adjusted = adjusted
	update.Reason = reason
	return update, nil
}

// enforceVacuum fulfils as much of the traceability promise as active readers
// currently permit. The logical window says which history is already
// inadmissible; the physical vacuum only removes versions up to the lowest
// pinned snapshot, and reports the gap honestly.
func (d *DB) enforceVacuum(requested *uint64) (versions.VacuumResult, RetentionStatus, error) {
	target := d.retainFrom
	if requested != nil {
		if *requested > d.retainFrom {
			return versions.VacuumResult{}, RetentionStatus{}, fault("invalid", "vacuum horizon %d would exceed the retention window start %d", *requested, d.retainFrom)
		}
		if *requested < d.horizon {
			return versions.VacuumResult{}, RetentionStatus{}, fault("invalid", "physical horizon cannot move backwards; retained interval %d..%d", d.horizon, d.sequence)
		}
		target = *requested
	}
	_, pinned := d.pins()
	physical := target
	if physical > pinned {
		physical = pinned
	}
	beforeRemoved := d.store.CountRemovable(physical)
	if err := d.publish(journal.Record{Kind: "vacuum", Horizon: physical, LogicalHorizon: d.retainFrom, Removed: beforeRemoved}); err != nil {
		return versions.VacuumResult{}, RetentionStatus{}, err
	}
	_, remaining := d.store.Count()
	d.indexes = index.Build(d.catalog, d.store)
	// Only versions removed while the physical horizon actually advances are
	// newly purged; a pinned no-op vacuum must not inflate the session counter.
	if physical > d.horizon {
		d.purgedVersions += beforeRemoved
	}
	result := versions.VacuumResult{Horizon: physical, Removed: beforeRemoved, Retained: remaining, Pinned: physical < target}
	return result, d.retentionStatusLocked(), nil
}
