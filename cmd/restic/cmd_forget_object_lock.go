package main

// cmd_forget_object_lock.go holds forget's entire --skip-object-locked
// implementation: which snapshot files were selected/removed/still-locked,
// and how that's reported in text and JSON output. Not used by any other
// command.

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui/progress"
)

// ObjectLockedSnapshot records a snapshot file forget left in place, when
// --skip-object-locked is set, because it is still under an active Object
// Lock retention -- along with the retention date obtained from the lazy
// backend.ObjectLocker.RetainedUntil lookup, for TASK-30's reporting.
type ObjectLockedSnapshot struct {
	ID          restic.ID
	RetainUntil time.Time
}

// ForgetRemovalStats records the outcome of removeForgetSnapshots's attempt
// to delete the snapshot files forget selected for removal: how many were
// selected, how many were actually removed, and -- only ever non-empty when
// SkipObjectLocked was set -- which ones were left in place because they are
// still object-locked. TASK-30 surfaces these counts in forget's plain-text
// summary and --json output.
type ForgetRemovalStats struct {
	SelectedForRemoval int
	Removed            int
	ObjectLocked       []ObjectLockedSnapshot
}

// removeForgetSnapshots deletes every snapshot file in removeSnIDs, exactly
// as `forget` has always done, and classifies the result of each of those
// delete calls into ForgetRemovalStats. It never makes a delete call beyond
// the one `forget` already made for every file: when skipObjectLocked is
// true, a failure with errors.Is(err, backend.ErrObjectLocked) is treated as
// "leave it, record it, keep going" -- from the very error the unconditional
// delete already returned -- and locker.RetainedUntil is called exactly
// once, only for that file, purely to obtain a date for reporting. Any other
// error is handled exactly as when skipObjectLocked is false: the file's ID
// is returned in the failedSnIDs set, unchanged from forget's behavior
// before this function existed.
//
// locker is unused (and may be nil) when skipObjectLocked is false.
//
// ParallelRemove runs its report callback from up to repo.Connections()
// goroutines concurrently, so every mutation of the shared stats/failedSnIDs
// is guarded by mu -- without it, concurrent "stats.Removed++"/append/Insert
// calls race (IDSet.Insert is a plain, non-thread-safe map write) and can
// silently lose updates or crash with "concurrent map writes".
func removeForgetSnapshots(ctx context.Context, repo restic.RemoverUnpacked[restic.WriteableFileType], removeSnIDs restic.IDSet, skipObjectLocked bool, locker backend.ObjectLocker, printer progress.Printer, bar restic.Counter) (ForgetRemovalStats, restic.IDSet, error) {
	stats := ForgetRemovalStats{SelectedForRemoval: len(removeSnIDs)}
	failedSnIDs := restic.NewIDSet()

	var mu sync.Mutex
	err := restic.ParallelRemove(ctx, repo, removeSnIDs, restic.WriteableSnapshotFile, func(id restic.ID, err error) error {
		switch {
		case err == nil:
			mu.Lock()
			stats.Removed++
			mu.Unlock()
			printer.VV("removed %v/%v\n", restic.SnapshotFile, id)
		case skipObjectLocked && errors.Is(err, backend.ErrObjectLocked):
			retainUntil, _, lookupErr := locker.RetainedUntil(ctx, backend.Handle{Type: restic.SnapshotFile, Name: id.String()})
			if lookupErr != nil {
				printer.E("unable to determine retention for object-locked %v/%v: %v\n", restic.SnapshotFile, id, lookupErr)
			}
			mu.Lock()
			stats.ObjectLocked = append(stats.ObjectLocked, ObjectLockedSnapshot{ID: id, RetainUntil: retainUntil})
			mu.Unlock()
			printer.VV("%v/%v is still object-locked, skipping\n", restic.SnapshotFile, id)
		default:
			printer.E("unable to remove %v/%v from the repository\n", restic.SnapshotFile, id)
			mu.Lock()
			failedSnIDs.Insert(id)
			mu.Unlock()
		}
		return nil
	}, bar)

	return stats, failedSnIDs, err
}

// ForgetJSONResult is the top-level shape of `forget --json`'s output when
// --skip-object-locked is set: the same per-group Keep/Remove listing
// printJSONForget has always produced, plus the ForgetRemovalStats counts as
// additive, structured fields (not embedded in a text string), per TASK-30.
// It is only ever encoded from the --skip-object-locked branch in
// runForget; without the flag, forget --json keeps emitting the bare
// []*ForgetGroup array via printJSONForget, unchanged.
type ForgetJSONResult struct {
	Groups             []*ForgetGroup `json:"groups"`
	SelectedForRemoval int            `json:"selected_for_removal"`
	Removed            int            `json:"removed"`
	ObjectLocked       int            `json:"object_locked"`
}

func printJSONForgetSkipObjectLocked(stdout io.Writer, forgets []*ForgetGroup, stats ForgetRemovalStats) error {
	return json.NewEncoder(stdout).Encode(ForgetJSONResult{
		Groups:             forgets,
		SelectedForRemoval: stats.SelectedForRemoval,
		Removed:            stats.Removed,
		ObjectLocked:       len(stats.ObjectLocked),
	})
}
