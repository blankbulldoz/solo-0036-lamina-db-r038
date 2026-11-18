package versions

type VacuumResult struct {
	Horizon  uint64 `json:"horizon"`
	Removed  int    `json:"removed"`
	Retained int    `json:"retained"`
	Pinned   bool   `json:"pinned"`
}

// chainPlan describes what would happen to one version chain at a physical
// horizon. keep is the index of the first retained version; dropKey marks a
// chain whose final tombstone is entirely below the horizon and whose key can
// therefore be removed from the table.
type chainPlan struct {
	keep    int
	dropKey bool
}

func planChain(chain []Version, horizon uint64) chainPlan {
	keep := 0
	for keep+1 < len(chain) && chain[keep+1].Sequence <= horizon {
		keep++
	}
	dropKey := len(chain) > 0 && chain[len(chain)-1].Sequence <= horizon && chain[len(chain)-1].Deleted
	return chainPlan{keep: keep, dropKey: dropKey}
}

// CountRemovable reports how many version records a physical vacuum at the
// given horizon would remove, without touching the store. It lets callers
// separate "logically expired" from "physically reclaimed" facts.
func (s *Store) CountRemovable(horizon uint64) int {
	removed := 0
	for _, rows := range s.Tables {
		for _, chain := range rows {
			plan := planChain(chain, horizon)
			if plan.dropKey {
				removed += len(chain)
				continue
			}
			removed += plan.keep
		}
	}
	return removed
}

func (s *Store) Vacuum(horizon uint64) VacuumResult {
	result := VacuumResult{Horizon: horizon}
	for _, rows := range s.Tables {
		for key, chain := range rows {
			plan := planChain(chain, horizon)
			if plan.dropKey {
				result.Removed += len(chain)
				delete(rows, key)
				continue
			}
			result.Removed += plan.keep
			remaining := append([]Version(nil), chain[plan.keep:]...)
			rows[key] = remaining
			result.Retained += len(remaining)
		}
	}
	return result
}
