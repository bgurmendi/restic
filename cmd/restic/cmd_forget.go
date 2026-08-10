package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func newForgetCommand(globalOptions *global.Options) *cobra.Command {
	var opts ForgetOptions
	var pruneOpts PruneOptions

	cmd := &cobra.Command{
		Use:   "forget [flags] [snapshot ID] [...]",
		Short: "Remove snapshots from the repository",
		Long: `
The "forget" command removes snapshots according to a policy. All snapshots are
first divided into groups according to "--group-by", and after that the policy
specified by the "--keep-*" options is applied to each group individually.
If there are not enough snapshots to keep one for each duration related
"--keep-{within-,}*" option, the oldest snapshot in the group is kept
additionally.

Please note that this command really only deletes the snapshot object in the
repository, which is a reference to data stored there. In order to remove the
unreferenced data after "forget" was run successfully, see the "prune" command.

Please also read the documentation for "forget" to learn about some important
security considerations.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 3 if there was an error removing one or more snapshots.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			finalizeSnapshotFilter(&opts.SnapshotFilter)
			return runForget(cmd.Context(), opts, pruneOpts, *globalOptions, globalOptions.Term, args)
		},
	}

	opts.AddFlags(cmd.Flags())
	pruneOpts.AddLimitedFlags(cmd.Flags())
	return cmd
}

type ForgetPolicyCount int

var ErrNegativePolicyCount = errors.New("negative values not allowed, use 'unlimited' instead")
var ErrFailedToRemoveOneOrMoreSnapshots = errors.New("failed to remove one or more snapshots")

func (c *ForgetPolicyCount) Set(s string) error {
	switch s {
	case "unlimited":
		*c = -1
	default:
		val, err := strconv.ParseInt(s, 10, 0)
		if err != nil {
			return err
		}
		if val < 0 {
			return ErrNegativePolicyCount
		}
		*c = ForgetPolicyCount(val)
	}

	return nil
}

func (c *ForgetPolicyCount) String() string {
	switch *c {
	case -1:
		return "unlimited"
	default:
		return strconv.FormatInt(int64(*c), 10)
	}
}

func (c *ForgetPolicyCount) Type() string {
	return "n"
}

// ForgetOptions collects all options for the forget command.
type ForgetOptions struct {
	PolicySelectionOptions

	UnsafeAllowRemoveAll bool

	data.SnapshotFilter
	Compact bool

	// Grouping
	GroupBy data.SnapshotGroupByOptions
	DryRun  bool
	Prune   bool

	// SkipObjectLocked skips snapshot files still under an active Object
	// Lock retention instead of failing the whole run (experimental,
	// requires the object-lock feature flag).
	SkipObjectLocked bool
}

func (opts *ForgetOptions) AddFlags(f *pflag.FlagSet) {
	opts.PolicySelectionOptions.AddFlags(f)
	f.BoolVar(&opts.UnsafeAllowRemoveAll, "unsafe-allow-remove-all", false, "allow deleting all snapshots of a snapshot group")

	f.StringArrayVar(&opts.Hosts, "hostname", nil, "only consider snapshots with the given `hostname` (can be specified multiple times)")
	err := f.MarkDeprecated("hostname", "use --host")
	if err != nil {
		// MarkDeprecated only returns an error when the flag is not found
		panic(err)
	}
	// must be defined after `--hostname` to not override the default value from the environment
	initMultiSnapshotFilter(f, &opts.SnapshotFilter, false)

	f.BoolVarP(&opts.Compact, "compact", "c", false, "use compact output format")
	opts.GroupBy = data.SnapshotGroupByOptions{Host: true, Path: true}
	f.VarP(&opts.GroupBy, "group-by", "g", "`group` snapshots by host, paths and/or tags, separated by comma (disable grouping with '')")
	f.BoolVarP(&opts.DryRun, "dry-run", "n", false, "do not delete anything, just print what would be done")
	f.BoolVar(&opts.Prune, "prune", false, "automatically run the 'prune' command if snapshots have been removed")
	f.BoolVar(&opts.SkipObjectLocked, "skip-object-locked", false, "skip snapshot files still under an active Object Lock retention instead of failing (experimental, requires the object-lock feature flag)")

	f.SortFlags = false
}

func verifyForgetOptions(opts *ForgetOptions) error {
	if opts.SkipObjectLocked && !feature.Flag.Enabled(feature.ObjectLock) {
		return errors.Fatal("feature flag `object-lock` is required to use `--skip-object-locked`, enable it by setting RESTIC_FEATURES=object-lock")
	}
	return verifyPolicySelectionOptions(&opts.PolicySelectionOptions)
}

func runForget(ctx context.Context, opts ForgetOptions, pruneOptions PruneOptions, gopts global.Options, term ui.Terminal, args []string) error {
	err := verifyForgetOptions(&opts)
	if err != nil {
		return err
	}

	err = verifyPruneOptions(&pruneOptions)
	if err != nil {
		return err
	}

	if gopts.NoLock && !opts.DryRun {
		return errors.Fatal("--no-lock is only applicable in combination with --dry-run for forget command")
	}

	printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)
	ctx, repo, unlock, err := openWithExclusiveLock(ctx, gopts, opts.DryRun && gopts.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	// Resolve the backend.ObjectLocker capability once, upfront, rather than
	// per-file: --skip-object-locked needs it (lazily, see
	// removeForgetSnapshots) only for the rare locked case, but a backend
	// that doesn't support it at all must fail clearly now, not silently
	// ignore the flag partway through the removal loop.
	var locker backend.ObjectLocker
	if opts.SkipObjectLocked {
		locker, err = requireObjectLocker(repo.Backend(), "forget")
		if err != nil {
			return err
		}
	}

	var snapshots data.Snapshots
	removeSnIDs := restic.NewIDSet()

	for sn := range FindFilteredSnapshots(ctx, repo, repo, &opts.SnapshotFilter, args, printer) {
		snapshots = append(snapshots, sn)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	var jsonGroups []*ForgetGroup

	if len(args) > 0 {
		// When explicit snapshots args are given, remove them immediately.
		for _, sn := range snapshots {
			removeSnIDs.Insert(*sn.ID())
		}
	} else {
		policy := opts.Policy()

		if policy.Empty() {
			if opts.UnsafeAllowRemoveAll {
				if opts.SnapshotFilter.Empty() {
					return errors.Fatal("--unsafe-allow-remove-all is not allowed unless a snapshot filter option is specified")
				}
				// UnsafeAllowRemoveAll together with snapshot filter is fine
			} else {
				return errors.Fatal("no policy was specified, no snapshots will be removed")
			}
		}

		printer.P("Applying Policy: %v\n", policy)

		selections, err := selectSnapshotsForPolicy(snapshots, opts.GroupBy, policy)
		if err != nil {
			return err
		}

		for _, selection := range selections {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if gopts.Verbose >= 1 && !gopts.JSON {
				err = PrintSnapshotGroupHeader(gopts.Term.OutputWriter(), selection.GroupKeyJSON)
				if err != nil {
					return err
				}
			}

			key := selection.Key

			var fg ForgetGroup
			fg.Tags = key.Tags
			fg.Host = key.Hostname
			fg.Paths = key.Paths

			keep, remove, reasons := selection.Keep, selection.Remove, selection.Reasons

			if !policy.Empty() && len(keep) == 0 {
				return fmt.Errorf("refusing to delete last snapshot of snapshot group \"%v\"", key.String())
			}
			if len(keep) != 0 && !gopts.Quiet && !gopts.JSON {
				printer.P("keep %d snapshots:\n", len(keep))
				if err := PrintSnapshots(gopts.Term.OutputWriter(), keep, reasons, opts.Compact); err != nil {
					return err
				}
				printer.P("\n")
			}
			fg.Keep = asJSONSnapshots(keep)

			if len(remove) != 0 && !gopts.Quiet && !gopts.JSON {
				printer.P("remove %d snapshots:\n", len(remove))
				if err := PrintSnapshots(gopts.Term.OutputWriter(), remove, nil, opts.Compact); err != nil {
					return err
				}
				printer.P("\n")
			}
			fg.Remove = asJSONSnapshots(remove)

			fg.Reasons = asJSONKeeps(reasons)

			jsonGroups = append(jsonGroups, &fg)

			for _, sn := range remove {
				removeSnIDs.Insert(*sn.ID())
			}
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// these are the snapshots that failed to be removed
	failedSnIDs := restic.NewIDSet()
	var removalStats ForgetRemovalStats
	if len(removeSnIDs) > 0 {
		if !opts.DryRun {
			bar := printer.NewCounter("files deleted")
			removalStats, failedSnIDs, err = removeForgetSnapshots(ctx, repo, removeSnIDs, opts.SkipObjectLocked, locker, printer, bar)
			bar.Done()
			if err != nil {
				return err
			}
		} else {
			removalStats.SelectedForRemoval = len(removeSnIDs)
			printer.P("Would have removed the following snapshots:\n%v\n\n", removeSnIDs)
		}
	}

	// removalStats.ObjectLocked is only ever non-empty when SkipObjectLocked
	// is set (removeForgetSnapshots only classifies ErrObjectLocked that
	// way when told to). Print the summary BASE.md documents ("selected for
	// removal: 15 / removed: 11 / object locked: 4", one count per line) via
	// printer.P: like the "Applying Policy" line above, P() is a no-op both
	// under --quiet (verbosity 0) and under --json (NewTerminalPrinter forces
	// verbosity to 0 whenever json is true), so this needs no extra
	// !gopts.JSON guard and leaves default (non-flag) output untouched.
	if opts.SkipObjectLocked {
		printer.P("selected for removal: %d\n", removalStats.SelectedForRemoval)
		printer.P("removed: %d\n", removalStats.Removed)
		printer.P("object locked: %d\n", len(removalStats.ObjectLocked))
	}

	if gopts.JSON && len(jsonGroups) > 0 {
		if opts.SkipObjectLocked {
			// Emitting the SkipObjectLocked counts as structured fields
			// requires wrapping the existing bare `[]*ForgetGroup` array in
			// an object -- there is no way to add fields to a JSON array.
			// That's only acceptable because it happens exclusively behind
			// the opt-in --skip-object-locked flag: without the flag,
			// printJSONForget below still emits the exact bare-array shape
			// forget --json has always produced, so existing scripts that
			// don't pass the new flag see byte-for-byte unchanged output.
			err = printJSONForgetSkipObjectLocked(gopts.Term.OutputWriter(), jsonGroups, removalStats)
		} else {
			err = printJSONForget(gopts.Term.OutputWriter(), jsonGroups)
		}
		if err != nil {
			return err
		}
	}

	if len(failedSnIDs) > 0 {
		return ErrFailedToRemoveOneOrMoreSnapshots
	}

	if len(removeSnIDs) > 0 && opts.Prune {
		if opts.DryRun {
			printer.P("%d snapshots would be removed, running prune dry run\n", len(removeSnIDs))
		} else {
			printer.P("%d snapshots have been removed, running prune\n", len(removeSnIDs))
		}
		pruneOptions.DryRun = opts.DryRun
		return runPruneWithRepo(ctx, pruneOptions, gopts, repo, removeSnIDs, printer)
	}

	return nil
}

// ForgetGroup helps to print what is forgotten in JSON.
type ForgetGroup struct {
	Tags    []string     `json:"tags"`
	Host    string       `json:"host"`
	Paths   []string     `json:"paths"`
	Keep    []Snapshot   `json:"keep"`
	Remove  []Snapshot   `json:"remove"`
	Reasons []KeepReason `json:"reasons"`
}

func asJSONSnapshots(list data.Snapshots) []Snapshot {
	var resultList []Snapshot
	for _, sn := range list {
		k := Snapshot{
			Snapshot: sn,
			ID:       sn.ID(),
			ShortID:  sn.ID().Str(),
		}
		resultList = append(resultList, k)
	}
	return resultList
}

// KeepReason helps to print KeepReasons as JSON with Snapshots with their ID included.
type KeepReason struct {
	Snapshot Snapshot `json:"snapshot"`
	Matches  []string `json:"matches"`
}

func asJSONKeeps(list []data.KeepReason) []KeepReason {
	var resultList []KeepReason
	for _, keep := range list {
		k := KeepReason{
			Snapshot: Snapshot{
				Snapshot: keep.Snapshot,
				ID:       keep.Snapshot.ID(),
				ShortID:  keep.Snapshot.ID().Str(),
			},
			Matches: keep.Matches,
		}
		resultList = append(resultList, k)
	}
	return resultList
}

func printJSONForget(stdout io.Writer, forgets []*ForgetGroup) error {
	return json.NewEncoder(stdout).Encode(forgets)
}
