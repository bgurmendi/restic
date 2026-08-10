package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/restic/restic/internal/data"
	rtest "github.com/restic/restic/internal/test"
)

// TestSelectSnapshotsForPolicy checks that the extracted selection step groups
// snapshots and applies the policy exactly like the inlined code it replaced:
// no side effects, and per-group results matching data.GroupSnapshots +
// data.ApplyPolicy called directly.
func TestSelectSnapshotsForPolicy(t *testing.T) {
	newTime := func(s string) time.Time {
		tm, err := time.Parse("2006-01-02", s)
		rtest.OK(t, err)
		return tm
	}

	snapshots := data.Snapshots{
		{Hostname: "a", Time: newTime("2023-01-01")},
		{Hostname: "a", Time: newTime("2023-01-02")},
		{Hostname: "a", Time: newTime("2023-01-03")},
		{Hostname: "b", Time: newTime("2023-01-01")},
		{Hostname: "b", Time: newTime("2023-01-02")},
	}

	groupBy := data.SnapshotGroupByOptions{Host: true}
	policy := data.ExpirePolicy{Last: 1}

	selections, err := selectSnapshotsForPolicy(snapshots, groupBy, policy)
	rtest.OK(t, err)
	rtest.Equals(t, 2, len(selections))

	// Build the same grouping directly, to compare against, and confirm the
	// extraction did not change what gets grouped/kept/removed.
	wantGroups, _, err := data.GroupSnapshots(snapshots, groupBy)
	rtest.OK(t, err)
	rtest.Equals(t, len(wantGroups), len(selections))

	seenHosts := make(map[string]bool)
	for _, selection := range selections {
		// GroupKeyJSON must decode to the same Key already attached.
		var key data.SnapshotGroupKey
		rtest.OK(t, json.Unmarshal([]byte(selection.GroupKeyJSON), &key))
		rtest.Equals(t, selection.Key, key)

		wantGroup, ok := wantGroups[selection.GroupKeyJSON]
		rtest.Assert(t, ok, "unexpected group key %q", selection.GroupKeyJSON)

		wantKeep, wantRemove, wantReasons := data.ApplyPolicy(wantGroup, policy)
		rtest.Equals(t, wantKeep, selection.Keep)
		rtest.Equals(t, wantRemove, selection.Remove)
		rtest.Equals(t, wantReasons, selection.Reasons)

		// keep-last 1 per host: exactly one kept, the rest removed.
		rtest.Equals(t, 1, len(selection.Keep))
		seenHosts[key.Hostname] = true
	}
	rtest.Assert(t, seenHosts["a"] && seenHosts["b"], "expected groups for both hosts, got %v", seenHosts)
}
