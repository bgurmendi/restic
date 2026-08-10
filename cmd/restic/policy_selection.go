package main

// policy_selection.go holds the "--keep-*" snapshot-selection logic shared by
// forget and protect (see PRD "protect reutiliza Forget"). Kept in its own
// file, rather than cmd_forget.go, because it is not forget-specific.

import (
	"encoding/json"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/spf13/pflag"
)

// PolicySelectionOptions collects the "--keep-*" flags that decide which
// snapshots a retention policy currently keeps. `forget` and `protect` share
// this struct (and its AddFlags/Policy methods) instead of each defining
// their own copy of these flags, so the two commands can never disagree
// about "the snapshots the policy would keep" given the same flags -- see
// the PRD's "protect reutiliza Forget" requirement.
type PolicySelectionOptions struct {
	Last          ForgetPolicyCount
	Hourly        ForgetPolicyCount
	Daily         ForgetPolicyCount
	Weekly        ForgetPolicyCount
	Monthly       ForgetPolicyCount
	Yearly        ForgetPolicyCount
	Within        data.Duration
	WithinHourly  data.Duration
	WithinDaily   data.Duration
	WithinWeekly  data.Duration
	WithinMonthly data.Duration
	WithinYearly  data.Duration
	KeepTags      data.TagLists
}

func (opts *PolicySelectionOptions) AddFlags(f *pflag.FlagSet) {
	f.VarP(&opts.Last, "keep-last", "l", "keep the last `n` snapshots (use 'unlimited' to keep all snapshots)")
	f.VarP(&opts.Hourly, "keep-hourly", "H", "keep the last `n` hourly snapshots (use 'unlimited' to keep all hourly snapshots)")
	f.VarP(&opts.Daily, "keep-daily", "d", "keep the last `n` daily snapshots (use 'unlimited' to keep all daily snapshots)")
	f.VarP(&opts.Weekly, "keep-weekly", "w", "keep the last `n` weekly snapshots (use 'unlimited' to keep all weekly snapshots)")
	f.VarP(&opts.Monthly, "keep-monthly", "m", "keep the last `n` monthly snapshots (use 'unlimited' to keep all monthly snapshots)")
	f.VarP(&opts.Yearly, "keep-yearly", "y", "keep the last `n` yearly snapshots (use 'unlimited' to keep all yearly snapshots)")
	f.VarP(&opts.Within, "keep-within", "", "keep snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.VarP(&opts.WithinHourly, "keep-within-hourly", "", "keep hourly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.VarP(&opts.WithinDaily, "keep-within-daily", "", "keep daily snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.VarP(&opts.WithinWeekly, "keep-within-weekly", "", "keep weekly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.VarP(&opts.WithinMonthly, "keep-within-monthly", "", "keep monthly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.VarP(&opts.WithinYearly, "keep-within-yearly", "", "keep yearly snapshots that are newer than `duration` (eg. 1y5m7d2h) relative to the latest snapshot")
	f.Var(&opts.KeepTags, "keep-tag", "keep snapshots with this `taglist` (can be specified multiple times)")
}

// Policy builds the data.ExpirePolicy described by the parsed flags.
func (opts *PolicySelectionOptions) Policy() data.ExpirePolicy {
	return data.ExpirePolicy{
		Last:          int(opts.Last),
		Hourly:        int(opts.Hourly),
		Daily:         int(opts.Daily),
		Weekly:        int(opts.Weekly),
		Monthly:       int(opts.Monthly),
		Yearly:        int(opts.Yearly),
		Within:        opts.Within,
		WithinHourly:  opts.WithinHourly,
		WithinDaily:   opts.WithinDaily,
		WithinWeekly:  opts.WithinWeekly,
		WithinMonthly: opts.WithinMonthly,
		WithinYearly:  opts.WithinYearly,
		Tags:          opts.KeepTags,
	}
}

func verifyPolicySelectionOptions(opts *PolicySelectionOptions) error {
	if opts.Last < -1 || opts.Hourly < -1 || opts.Daily < -1 || opts.Weekly < -1 ||
		opts.Monthly < -1 || opts.Yearly < -1 {
		return errors.Fatal("negative values other than -1 are not allowed for --keep-*")
	}

	for _, d := range []data.Duration{opts.Within, opts.WithinHourly, opts.WithinDaily,
		opts.WithinMonthly, opts.WithinWeekly, opts.WithinYearly} {
		if d.Hours < 0 || d.Days < 0 || d.Months < 0 || d.Years < 0 {
			return errors.Fatal("durations containing negative values are not allowed for --keep-within*")
		}
	}

	return nil
}

// ForgetPolicySelection is the pure result of applying a forget policy to one
// snapshot group: which snapshots the policy would keep (with their
// data.KeepReason) and which are selected for removal.
type ForgetPolicySelection struct {
	// GroupKeyJSON is the raw JSON-encoded data.SnapshotGroupKey identifying the
	// group, exactly as produced by data.GroupSnapshots.
	GroupKeyJSON string
	Key          data.SnapshotGroupKey
	Keep         data.Snapshots
	Remove       data.Snapshots
	Reasons      []data.KeepReason
}

// selectSnapshotsForPolicy groups snapshots according to groupBy and applies policy
// to each group, returning the resulting keep/remove selection per group. It has no
// side effects: it does not print or delete anything. This is the pure "what would
// forget do" step shared by the `forget` and `protect` commands.
func selectSnapshotsForPolicy(snapshots data.Snapshots, groupBy data.SnapshotGroupByOptions, policy data.ExpirePolicy) ([]ForgetPolicySelection, error) {
	snapshotGroups, _, err := data.GroupSnapshots(snapshots, groupBy)
	if err != nil {
		return nil, err
	}

	selections := make([]ForgetPolicySelection, 0, len(snapshotGroups))
	for k, snapshotGroup := range snapshotGroups {
		var key data.SnapshotGroupKey
		if err := json.Unmarshal([]byte(k), &key); err != nil {
			return nil, err
		}

		keep, remove, reasons := data.ApplyPolicy(snapshotGroup, policy)
		selections = append(selections, ForgetPolicySelection{
			GroupKeyJSON: k,
			Key:          key,
			Keep:         keep,
			Remove:       remove,
			Reasons:      reasons,
		})
	}

	return selections, nil
}
