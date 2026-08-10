package repository

// TASK-37: regression test proving that prune's pack-deletion path
// (deleteUnusedPacks in prune.go) behaves exactly as it did before this
// feature existed when skipObjectLocked is false -- the internal-package
// counterpart to cmd/restic's TestPruneWithoutSkipObjectLockedUnchanged
// (cmd_prune_object_lock_regression_test.go), mirroring
// TestRemoveForgetSnapshotsSkipObjectLockedFalseDoesNotClassify
// (cmd_forget_object_lock_regression_test.go) for forget.
//
// With skipObjectLocked false, a delete failing with backend.ErrObjectLocked
// must be treated exactly like any other delete failure -- logged via
// printer.E and left out of stats.ObjectLocked, exactly as the pre-feature
// deleteFiles(ctx, true, ...) call this replaced always did -- and the
// ObjectLocker passed in must never be touched.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

// pruneRegressionMockRemover is a minimal restic.RemoverUnpacked[restic.FileType]
// fake: it fails exactly like a real backend.Remove would for a single,
// designated locked pack ID (wrapping backend.ErrObjectLocked, mirroring
// what the s3 backend actually returns), and otherwise succeeds -- so
// deleteUnusedPacks can be exercised without a real backend.
type pruneRegressionMockRemover struct {
	lockedID restic.ID
}

func (m pruneRegressionMockRemover) Connections() uint { return 2 }

func (m pruneRegressionMockRemover) RemoveUnpacked(_ context.Context, _ restic.FileType, id restic.ID) error {
	if id == m.lockedID {
		return fmt.Errorf("%w: simulated locked delete failure", backend.ErrObjectLocked)
	}
	return nil
}

// prunePoisonObjectLocker is a backend.ObjectLocker whose every method just
// records that it was called, via an atomic flag safe to read from the test
// goroutine after deleteUnusedPacks (whose restic.ParallelRemove call
// invokes the report callback, and so any locker method, from worker
// goroutines) returns. deleteUnusedPacks's only locker use is
// locker.RetainedUntil, gated by `skipObjectLocked && errors.Is(err,
// backend.ErrObjectLocked)` -- so with skipObjectLocked false, `called` must
// stay false no matter what error a delete returns.
type prunePoisonObjectLocker struct {
	called atomic.Bool
}

func (p *prunePoisonObjectLocker) IsObjectLockEnabled(_ context.Context) (bool, error) {
	p.called.Store(true)
	return false, nil
}

func (p *prunePoisonObjectLocker) RetainedUntil(_ context.Context, _ backend.Handle) (time.Time, bool, error) {
	p.called.Store(true)
	return time.Time{}, false, nil
}

func (p *prunePoisonObjectLocker) SetRetention(_ context.Context, _ backend.Handle, _ time.Time) error {
	p.called.Store(true)
	return nil
}

// TestDeleteUnusedPacksSkipObjectLockedFalseDoesNotClassify proves that with
// skipObjectLocked false, a delete failing with backend.ErrObjectLocked
// falls into deleteUnusedPacks's `default:` branch (the same "log and
// leave in place" path used for any other delete failure) instead of the
// `skipObjectLocked && errors.Is(err, backend.ErrObjectLocked)` branch, and
// that the ObjectLocker passed in is never touched.
func TestDeleteUnusedPacksSkipObjectLockedFalseDoesNotClassify(t *testing.T) {
	lockedID := restic.Hash([]byte("locked"))
	unlockedIDs := []restic.ID{restic.Hash([]byte("unlocked-1")), restic.Hash([]byte("unlocked-2"))}

	fileList := restic.NewIDSet(lockedID)
	for _, id := range unlockedIDs {
		fileList.Insert(id)
	}

	remover := pruneRegressionMockRemover{lockedID: lockedID}
	locker := &prunePoisonObjectLocker{}

	var stats PruneRemovalStats
	deleteUnusedPacks(context.Background(), remover, fileList, false, locker, &stats, progress.NewNoopPrinter())

	rtest.Assert(t, !locker.called.Load(),
		"the ObjectLocker was used even though skipObjectLocked was false: the `skipObjectLocked && errors.Is(err, backend.ErrObjectLocked)` guard in deleteUnusedPacks (prune.go) must have been bypassed")

	rtest.Equals(t, uint(len(fileList)), stats.SelectedForRemoval)
	rtest.Equals(t, uint(len(unlockedIDs)), stats.Removed)
	rtest.Assert(t, len(stats.ObjectLocked) == 0,
		"expected no ObjectLocked entries when skipObjectLocked is false (the TASK-34 classification branch must not run), got %v", stats.ObjectLocked)
}
