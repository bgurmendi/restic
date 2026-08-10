package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

func newProtectCommand(globalOptions *global.Options) *cobra.Command {
	var opts ProtectOptions

	cmd := &cobra.Command{
		Use:   "protect [flags]",
		Short: "Extend Object Lock retention on the snapshots a keep-* policy currently keeps",
		Long: `
The "protect" command is EXPERIMENTAL and requires the "object-lock" feature
flag (run "restic help features" for details on enabling feature flags).

It applies the same "--keep-*" retention policy "forget" uses to decide which
snapshots are currently kept, then extends S3 Object Lock retention on those
snapshots (and the data they need to restore) so they cannot be deleted for
at least "--for" <duration>, even by someone holding delete-capable
credentials.

Repeated runs only ever extend retention, never shorten it, and "protect"
never touches "locks/*".

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was any error.
Exit status is 10 if the repository does not exist.
Exit status is 11 if the repository is already locked.
Exit status is 12 if the password is incorrect.
`,
		GroupID:           cmdGroupDefault,
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runProtect(cmd.Context(), opts, *globalOptions, globalOptions.Term)
		},
	}

	opts.AddFlags(cmd.Flags())
	return cmd
}

// ProtectOptions collects all options for the protect command: the same
// "--keep-*" policy flags "forget" exposes (shared via PolicySelectionOptions
// rather than duplicated, see cmd_forget.go), plus protect's own "--for" and
// "--dry-run" flags. There is no protect-specific "--json" flag: protect
// reuses the global "--json" flag (global.Options.JSON) the same way forget
// does, rather than defining a parallel one.
type ProtectOptions struct {
	PolicySelectionOptions

	For    data.Duration
	DryRun bool
}

// AddFlags adds protect's flags to f: the shared "--keep-*" policy flags,
// plus "--for" and "--dry-run".
func (opts *ProtectOptions) AddFlags(f *pflag.FlagSet) {
	opts.PolicySelectionOptions.AddFlags(f)
	f.VarP(&opts.For, "for", "", "extend protection so kept snapshots remain recoverable for this long, e.g. 120d (required)")
	f.BoolVarP(&opts.DryRun, "dry-run", "n", false, "do not set any object lock retention, just print what would be protected")
	f.SortFlags = false
}

// verifyProtectOptions validates the parsed flags, analogous to
// verifyForgetOptions/verifyPruneOptions.
//
// --for is required: the protection duration is the whole point of this
// command, so unlike forget's --keep-within* flags (which default to unset,
// i.e. "don't apply this rule"), there is no sensible default to fall back
// to here - running protect without it would silently do nothing useful.
func verifyProtectOptions(opts *ProtectOptions) error {
	if err := verifyPolicySelectionOptions(&opts.PolicySelectionOptions); err != nil {
		return err
	}

	if opts.For.Zero() {
		return errors.Fatal("--for is required")
	}

	if opts.For.Hours < 0 || opts.For.Days < 0 || opts.For.Months < 0 || opts.For.Years < 0 {
		return errors.Fatal("durations containing negative values are not allowed for --for")
	}

	return nil
}

// protectRetainUntil computes the single Object Lock retention target date
// this run of protect will attempt to set: now + the --for duration,
// truncated to the second (S3/MinIO retention dates are second-precision,
// see TASK-8's spike).
//
// TASK-8 confirmed that MinIO rejects an attempt to shorten an active
// COMPLIANCE retention the same way documented AWS S3 behavior does, and
// TASK-15's SetRetention already treats such a rejection as a benign no-op
// rather than an error. That means protect does not need to read each
// object's current retention and compute max(current, retainUntil) itself
// before writing - it can call SetRetention with this one computed date
// directly on every object, every run, and repeated runs will only ever
// extend retention, never shorten it, without an extra read per object.
func protectRetainUntil(now time.Time, forDuration data.Duration) time.Time {
	return now.
		AddDate(forDuration.Years, forDuration.Months, forDuration.Days).
		Add(time.Duration(forDuration.Hours) * time.Hour).
		Truncate(time.Second)
}

// protectDefaultGroupBy is the grouping protect applies before running the
// policy, matching forget's own default (see ForgetOptions.AddFlags). Unlike
// forget, protect does not (yet) expose a "--group-by" flag of its own: the
// PRD's "protect reutiliza Forget" principle is about not reimplementing the
// selection *logic*, not about mirroring every one of forget's flags one for
// one; the default grouping is a reasonable, unsurprising starting point.
var protectDefaultGroupBy = data.SnapshotGroupByOptions{Host: true, Path: true}

// protectObjectSet is the exact set of repository objects protect extends
// retention on, split by category for reporting (config/keys/index/
// snapshots/packs, per this task's acceptance criteria). There is
// deliberately no field for locks/* anywhere in this struct or the code that
// builds it: protect must never touch locks/*, see BASE.md's "objects
// protected" section.
type protectObjectSet struct {
	Config    []backend.Handle
	Keys      []backend.Handle
	Index     []backend.Handle
	Snapshots []backend.Handle
	Packs     []backend.Handle
}

// all returns every handle in s, config+keys+index+snapshots+packs
// combined - the exact set SetRetention is applied to.
func (s protectObjectSet) all() []backend.Handle {
	handles := make([]backend.Handle, 0, len(s.Config)+len(s.Keys)+len(s.Index)+len(s.Snapshots)+len(s.Packs))
	handles = append(handles, s.Config...)
	handles = append(handles, s.Keys...)
	handles = append(handles, s.Index...)
	handles = append(handles, s.Snapshots...)
	handles = append(handles, s.Packs...)
	return handles
}

// collectProtectObjectSet enumerates the exact object set BASE.md's
// algorithm protects: the config file, every key, every index file, the
// given (already policy-selected) snapshots, and the packs those specific
// snapshots need to restore - computed via TASK-19's
// repository.LivePacksForSnapshots, which is why toProtect must be exactly
// the KEPT snapshot set (TASK-18's output), not all snapshots and not the
// removal set: protecting packs only needed by snapshots the policy wants to
// discard would defeat the whole design.
//
// repo's index must already be loaded (repo.LoadIndex) before calling this,
// the same precondition prune's own live-pack computation has.
func collectProtectObjectSet(ctx context.Context, repo *repository.Repository, toProtect data.Snapshots, printer progress.Printer) (protectObjectSet, error) {
	var set protectObjectSet

	// config is always exactly this one well-known handle - the same one
	// Repository itself uses to Stat/Load it - never listed.
	set.Config = []backend.Handle{{Type: restic.ConfigFile}}

	if err := repo.List(ctx, restic.KeyFile, func(id restic.ID, _ int64) error {
		set.Keys = append(set.Keys, backend.Handle{Type: restic.KeyFile, Name: id.String()})
		return nil
	}); err != nil {
		return protectObjectSet{}, fmt.Errorf("listing keys: %w", err)
	}

	if err := repo.List(ctx, restic.IndexFile, func(id restic.ID, _ int64) error {
		set.Index = append(set.Index, backend.Handle{Type: restic.IndexFile, Name: id.String()})
		return nil
	}); err != nil {
		return protectObjectSet{}, fmt.Errorf("listing index files: %w", err)
	}

	snapshotIDs := restic.NewIDSet()
	for _, sn := range toProtect {
		id := *sn.ID()
		snapshotIDs.Insert(id)
		set.Snapshots = append(set.Snapshots, backend.Handle{Type: restic.SnapshotFile, Name: id.String()})
	}

	livePacks, err := repository.LivePacksForSnapshots(ctx, repo, snapshotIDs, printer)
	if err != nil {
		return protectObjectSet{}, fmt.Errorf("computing live packs: %w", err)
	}
	for id := range livePacks {
		set.Packs = append(set.Packs, backend.Handle{Type: restic.PackFile, Name: id.String()})
	}

	// Deliberately: no restic.LockFile handles are ever added above, so
	// locks/* can never end up in the returned set.
	return set, nil
}

// protectSetRetention calls locker.SetRetention(ctx, h, retainUntil) once
// for every handle in handles, using up to connections concurrent calls -
// the same errgroup-with-SetLimit bounded-concurrency pattern
// restic.ParallelRemove already uses for per-object backend operations,
// rather than spawning one goroutine per object. bar is advanced once per
// successfully-protected object.
func protectSetRetention(ctx context.Context, locker backend.ObjectLocker, connections uint, handles []backend.Handle, retainUntil time.Time, bar restic.Counter) error {
	wg, ctx := errgroup.WithContext(ctx)
	wg.SetLimit(int(connections))

	bar.SetMax(uint64(len(handles)))

loop:
	for _, h := range handles {
		select {
		case <-ctx.Done():
			break loop
		default:
		}

		h := h
		wg.Go(func() error {
			if err := locker.SetRetention(ctx, h, retainUntil); err != nil {
				return fmt.Errorf("setting object lock retention on %v: %w", h, err)
			}
			bar.Add(1)
			return nil
		})
	}
	return wg.Wait()
}

// ProtectResult is protect's "--json" output shape (TASK-27). It mirrors
// forget's snapshot JSON shape for the protected snapshot list by reusing
// asJSONSnapshots (see cmd_forget.go) rather than defining a parallel one,
// and adds the fields specific to protect: the computed retain_until date,
// the count of protected packs, and whether this was a dry run.
type ProtectResult struct {
	DryRun      bool       `json:"dry_run"`
	RetainUntil time.Time  `json:"retain_until"`
	Snapshots   []Snapshot `json:"snapshots"`
	Packs       int        `json:"packs"`
}

// printJSONProtect encodes result to stdout as a single JSON object,
// mirroring printJSONForget's shape in cmd_forget.go.
func printJSONProtect(stdout io.Writer, result ProtectResult) error {
	return json.NewEncoder(stdout).Encode(result)
}

// backendTypeName returns a short, human-readable name for be's underlying
// concrete backend type, for use in error messages that need to name "which
// backend" a repository uses. It walks be's Unwrap() chain (mirroring
// backend.AsObjectLocker's own walk) down to the innermost backend, so
// wrapping layers like retry/cache/limiter never leak into the name, then
// keeps only the package name of that concrete type's reflected string
// (e.g. "*local.Local" -> "local", "*s3.s3" -> "s3") - which conveniently
// matches the backend scheme name most restic backends are addressed by in a
// repository location (e.g. "local:...", "s3:...").
func backendTypeName(be backend.Backend) string {
	for {
		uw, ok := be.(backend.Unwrapper)
		if !ok {
			break
		}
		next := uw.Unwrap()
		if next == nil {
			break
		}
		be = next
	}

	name := strings.TrimPrefix(fmt.Sprintf("%T", be), "*")
	if idx := strings.Index(name, "."); idx != -1 {
		name = name[:idx]
	}
	return name
}

// requireObjectLocker resolves be's backend.ObjectLocker capability
// (TASK-13), or returns a clear, actionable "unsupported backend" error
// naming be's actual backend type if it doesn't implement one - Object Lock
// support is currently s3-only. command names the calling command (e.g.
// "protect", "forget") in that error message. runProtect's pre-flight check
// (TASK-24), runProtectApply below, and forget's own upfront
// --skip-object-locked check (TASK-29) all share this single implementation,
// so no call site can drift into reporting the requirement differently.
func requireObjectLocker(be backend.Backend, command string) (backend.ObjectLocker, error) {
	locker, ok := backend.AsObjectLocker(be)
	if !ok {
		return nil, errors.Fatal(fmt.Sprintf(
			"the `%s` command requires a backend with S3 Object Lock support (currently only the `s3` backend); this repository uses the %q backend, which does not support it",
			command, backendTypeName(be)))
	}
	return locker, nil
}

// runProtectApply is runProtect's core, factored out so it can be exercised
// directly against a real repository/backend in integration tests without
// going through cobra flag parsing and global.OpenRepository. It resolves
// repo's backend.ObjectLocker capability, collects the object set (TASK-18's
// selected snapshots + TASK-19's live packs for them + config/keys/index),
// and, unless dryRun is set, applies retainUntil to every object in it, once
// each, never touching locks/*.
//
// When dryRun is true (TASK-27), the object set is still computed exactly as
// it would be for a real run - so the caller can report accurate would-be
// counts - but SetRetention is never called, so a dry run makes zero writes.
func runProtectApply(ctx context.Context, repo *repository.Repository, printer progress.Printer, toProtect data.Snapshots, retainUntil time.Time, dryRun bool) (protectObjectSet, error) {
	locker, err := requireObjectLocker(repo.Backend(), "protect")
	if err != nil {
		return protectObjectSet{}, err
	}

	if err := repo.LoadIndex(ctx, printer); err != nil {
		return protectObjectSet{}, err
	}

	objects, err := collectProtectObjectSet(ctx, repo, toProtect, printer)
	if err != nil {
		return protectObjectSet{}, err
	}

	if dryRun {
		return objects, nil
	}

	bar := printer.NewCounter("objects protected")
	defer bar.Done()
	if err := protectSetRetention(ctx, locker, repo.Connections(), objects.all(), retainUntil, bar); err != nil {
		return protectObjectSet{}, err
	}

	return objects, nil
}

// runProtectPreflight is TASK-24's pre-flight bucket Object Lock check: it
// resolves repo's backend.ObjectLocker capability, failing with a clear
// "unsupported backend" error naming the actual backend type if repo's
// backend doesn't implement one at all, then confirms the bucket itself has
// Object Lock enabled, propagating TASK-6's clear error unmodified if not.
// Factored out from runProtect so it can be exercised directly against a
// real repository/backend in integration tests, the same way runProtectApply
// already is - and so runProtect itself can call it as the very first thing
// that touches the backend, before snapshot loading or any other work.
func runProtectPreflight(ctx context.Context, repo *repository.Repository) error {
	locker, err := requireObjectLocker(repo.Backend(), "protect")
	if err != nil {
		return err
	}
	_, err = locker.IsObjectLockEnabled(ctx)
	return err
}

// runProtect wires the feature-flag gate, flag validation, and TASK-18's
// snapshot selection to runProtectApply, then prints a per-category summary.
func runProtect(ctx context.Context, opts ProtectOptions, gopts global.Options, term ui.Terminal) error {
	// Gate on the feature flag before touching the repository at all, so a
	// disabled flag fails fast with a clear, actionable error.
	if !feature.Flag.Enabled(feature.ObjectLock) {
		return errors.Fatal("feature flag `object-lock` is required to use the `protect` command, enable it by setting RESTIC_FEATURES=object-lock")
	}

	if err := verifyProtectOptions(&opts); err != nil {
		return err
	}

	printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, term)

	ctx, repo, unlock, err := openWithAppendLock(ctx, gopts, gopts.NoLock, printer)
	if err != nil {
		return err
	}
	defer unlock()

	// Pre-flight bucket Object Lock check (TASK-24): this must be the very
	// first thing that touches the backend/repository after opening it -
	// before snapshot loading, before TASK-18's selection call - so a
	// misconfigured backend or bucket never results in partial,
	// silently-unprotected work.
	if err := runProtectPreflight(ctx, repo); err != nil {
		return err
	}

	var snapshots data.Snapshots
	for sn := range FindFilteredSnapshots(ctx, repo, repo, &data.SnapshotFilter{}, nil, printer) {
		snapshots = append(snapshots, sn)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	policy := opts.Policy()
	printer.P("Applying Policy: %v\n", policy)

	selections, err := selectSnapshotsForPolicy(snapshots, protectDefaultGroupBy, policy)
	if err != nil {
		return err
	}

	var toProtect data.Snapshots
	for _, selection := range selections {
		toProtect = append(toProtect, selection.Keep...)
	}

	// retainUntil is the single target date this run attempts to set on
	// every object it touches; see protectRetainUntil's doc comment for why
	// no pre-read of each object's current retention is needed here. It is
	// computed up front, before the zero-snapshot check below, so both the
	// "nothing to protect" and the normal path can report it in --json
	// output.
	retainUntil := protectRetainUntil(time.Now(), opts.For)

	if len(toProtect) == 0 {
		printer.P("no snapshots are currently kept by this policy, nothing to protect\n")
		if gopts.JSON {
			return printJSONProtect(gopts.Term.OutputWriter(), ProtectResult{
				DryRun:      opts.DryRun,
				RetainUntil: retainUntil,
				Snapshots:   asJSONSnapshots(toProtect),
			})
		}
		return nil
	}

	printer.P("selected %d snapshot(s) to protect until %v (%v from now):\n", len(toProtect), retainUntil, opts.For)
	for _, sn := range toProtect {
		printer.P("  %s\n", sn.ID().Str())
	}

	objects, err := runProtectApply(ctx, repo, printer, toProtect, retainUntil, opts.DryRun)
	if err != nil {
		return err
	}

	verb := "protected"
	if opts.DryRun {
		verb = "would protect"
	}
	printer.P("%s until %v: config: %d, keys: %d, index: %d, snapshots: %d, packs: %d\n",
		verb, retainUntil, len(objects.Config), len(objects.Keys), len(objects.Index), len(objects.Snapshots), len(objects.Packs))

	if gopts.JSON {
		return printJSONProtect(gopts.Term.OutputWriter(), ProtectResult{
			DryRun:      opts.DryRun,
			RetainUntil: retainUntil,
			Snapshots:   asJSONSnapshots(toProtect),
			Packs:       len(objects.Packs),
		})
	}

	return nil
}
