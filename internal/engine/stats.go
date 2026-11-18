package engine

import (
	"lamina/internal/journal"
	"sort"
)

type Stats struct {
	Sequence       uint64                  `json:"sequence"`
	Horizon        uint64                  `json:"horizon"`
	LogicalHorizon uint64                  `json:"logical_horizon"`
	Policy         journal.RetentionPolicy `json:"retention_policy"`
	Active         []Transaction           `json:"active"`
	Keys           int                     `json:"keys"`
	Versions       int                     `json:"versions"`
	// ExpiredVersions counts every version outside the logical window,
	// including ones already removed; PurgedVersions are the subset physically
	// reclaimed since open. The gap is the space still held by active pins
	// even though the history no longer backs any read promise.
	ExpiredVersions int            `json:"expired_versions"`
	PurgedVersions  int            `json:"purged_versions_since_open"`
	IndexEntries    map[string]int `json:"index_entries"`
	JournalRecords  int            `json:"journal_records"`
	HistoryBase     uint64         `json:"history_base"`
	RecoveredTail   int64          `json:"recovered_tail_bytes"`
	Commits         uint64         `json:"commits_since_open"`
	Conflicts       uint64         `json:"conflicts_since_open"`
	Queries         uint64         `json:"queries_since_open"`
}

func (d *DB) Stats() (Stats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Stats{}, err
	}
	keys, versions := d.store.Count()
	s := Stats{Sequence: d.sequence, Horizon: d.horizon, LogicalHorizon: d.retainFrom, Policy: d.policy, Active: []Transaction{}, Keys: keys, Versions: versions, ExpiredVersions: d.store.CountRemovable(d.retainFrom) + d.purgedVersions, PurgedVersions: d.purgedVersions, IndexEntries: map[string]int{}, JournalRecords: len(d.wal.Records), HistoryBase: d.wal.Base, RecoveredTail: d.wal.RecoveredTail, Commits: d.commits, Conflicts: d.conflicts, Queries: d.queries}
	for _, tx := range d.transactions {
		copy := *tx
		copy.writes = nil
		s.Active = append(s.Active, copy)
	}
	sort.Slice(s.Active, func(i, j int) bool { return s.Active[i].ID < s.Active[j].ID })
	for name, t := range d.indexes.Trees {
		s.IndexEntries[name] = t.Size()
	}
	return s, nil
}
