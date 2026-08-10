package repository

// prune_object_lock.go holds prune's --skip-object-locked pack-deletion path:
// which unused pack files were selected/removed/still-locked. Not used
// outside prune.

import (
	"context"
	"sync"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui/progress"
)

// ObjectLockedPack records an unused pack file Execute left in place, when
// PruneOptions.SkipObjectLocked is set, because it is still under an active
// Object Lock retention -- along with the retention date obtained from the
// lazy backend.ObjectLocker.RetainedUntil lookup, for reporting. Mirrors
// ObjectLockedSnapshot in cmd/restic's forget implementation.
type ObjectLockedPack struct {
	ID          restic.ID
	RetainUntil time.Time
}

// PruneRemovalStats records the outcome of Execute's attempt to actually
// delete the unused/unreferenced pack files a PrunePlan selected for
// removal: how many were candidates, how many were actually removed, and --
// only ever non-empty when SkipObjectLocked was set -- which ones were left
// in place because they are still object-locked. A locked pack is not
// retried within the same run; a future prune run picks it up again once
// its lock expires.
type PruneRemovalStats struct {
	SelectedForRemoval uint
	Removed            uint
	ObjectLocked       []ObjectLockedPack
}

// deleteUnusedPacks deletes the given pack files exactly as
// deleteFiles(ctx, true, ...) has always done -- ignoring individual
// failures and only printing a warning -- and additionally classifies each
// outcome into stats. No delete is preceded by a lock check: when
// skipObjectLocked is false, a pack that fails to delete is simply logged
// and left in place, byte-for-byte as before this function existed. When
// skipObjectLocked is true, a failure with errors.Is(err,
// backend.ErrObjectLocked) -- from the very delete call already made, no
// extra API call for the common unlocked case -- is instead recorded in
// stats.ObjectLocked, after one additional locker.RetainedUntil lookup
// purely to obtain a date for reporting; any other error is handled exactly
// as when skipObjectLocked is false. locker is unused (and may be nil) when
// skipObjectLocked is false.
//
// ParallelRemove runs its report callback from up to repo.Connections()
// goroutines concurrently, so every mutation of the shared *stats is guarded
// by mu -- without it, concurrent "stats.Removed++"/append calls race and
// silently lose updates (observed as an under-reported "removed" count).
func deleteUnusedPacks(ctx context.Context, repo restic.RemoverUnpacked[restic.FileType], fileList restic.IDSet, skipObjectLocked bool, locker backend.ObjectLocker, stats *PruneRemovalStats, printer progress.Printer) {
	bar := printer.NewCounter("files deleted")
	defer bar.Done()

	stats.SelectedForRemoval += uint(len(fileList))

	var mu sync.Mutex
	_ = restic.ParallelRemove(ctx, repo, fileList, restic.PackFile, func(id restic.ID, err error) error {
		switch {
		case err == nil:
			mu.Lock()
			stats.Removed++
			mu.Unlock()
			printer.VV("removed %v/%v", restic.PackFile, id)
		case skipObjectLocked && errors.Is(err, backend.ErrObjectLocked):
			retainUntil, _, lookupErr := locker.RetainedUntil(ctx, backend.Handle{Type: restic.PackFile, Name: id.String()})
			if lookupErr != nil {
				printer.E("unable to determine retention for object-locked %v/%v: %v\n", restic.PackFile, id, lookupErr)
			}
			mu.Lock()
			stats.ObjectLocked = append(stats.ObjectLocked, ObjectLockedPack{ID: id, RetainUntil: retainUntil})
			mu.Unlock()
			printer.VV("%v/%v is still object-locked, skipping\n", restic.PackFile, id)
		default:
			printer.E("unable to remove %v/%v from the repository", restic.PackFile, id)
		}
		return nil
	}, bar)
}
