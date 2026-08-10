package main

// TASK-32: regression test proving `restic forget` run WITHOUT
// --skip-object-locked behaves exactly as it did before this whole feature
// existed, even against a repository that contains locked objects. Two
// angles, both against the exact same TASK-29 code
// (removeForgetSnapshots in cmd_forget.go):
//
//  1. TestRemoveForgetSnapshotsSkipObjectLockedFalseDoesNotClassify is a
//     fast, MinIO-free unit test that proves -- by direct observation, not
//     assumption -- that the `skipObjectLocked && errors.Is(err,
//     backend.ErrObjectLocked)` classification branch is never entered when
//     skipObjectLocked is false, using a poison backend.ObjectLocker that
//     records whether any of its methods were ever called. Even when the
//     underlying delete fails with backend.ErrObjectLocked (exactly as it
//     would against a real locked object), the poison locker must stay
//     untouched and the failure must be handled by the same generic
//     failedSnIDs path forget has always used for any delete error.
//  2. TestForgetWithoutSkipObjectLockedUnchanged is the end-to-end version
//     against a real local MinIO Object-Lock-enabled bucket: a snapshot file
//     is locked directly via minio-go's PutObjectRetention (not via `restic
//     protect`), a `--keep-last` policy selects it for removal, and forget
//     is run without --skip-object-locked. Step 1 of the spec ("establish
//     the baseline") was run manually against this exact fixture before
//     writing the assertions below: the observed, current behavior is that
//     the whole run fails with ErrFailedToRemoveOneOrMoreSnapshots -- the
//     same generic error forget has always returned when any delete call
//     among many fails for any reason -- the locked snapshot survives, and
//     every unlocked, selected snapshot is removed. That observed behavior
//     is what this test pins down.

import (
	"context"
	"fmt"
	"os/exec"
	"path"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/layout"
	"github.com/restic/restic/internal/backend/s3"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/options"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

// forgetRegressionMockRemover is a minimal restic.RemoverUnpacked fake: it
// fails exactly like a real backend.Remove would for a single, designated
// locked ID (wrapping backend.ErrObjectLocked, mirroring what the s3
// backend actually returns), and otherwise succeeds -- so
// removeForgetSnapshots can be exercised without a real backend.
type forgetRegressionMockRemover struct {
	lockedID restic.ID
}

func (m forgetRegressionMockRemover) Connections() uint { return 2 }

func (m forgetRegressionMockRemover) RemoveUnpacked(_ context.Context, _ restic.WriteableFileType, id restic.ID) error {
	if id == m.lockedID {
		return fmt.Errorf("%w: simulated locked delete failure", backend.ErrObjectLocked)
	}
	return nil
}

// poisonObjectLocker is a backend.ObjectLocker whose every method just
// records that it was called, via an atomic flag safe to read from the test
// goroutine after removeForgetSnapshots (whose ParallelRemove call invokes
// the report callback, and so any locker method, from worker goroutines)
// returns. removeForgetSnapshots's only locker use is
// locker.RetainedUntil, gated by `skipObjectLocked &&
// errors.Is(err, backend.ErrObjectLocked)` -- so with skipObjectLocked
// false, `called` must stay false no matter what error a delete returns.
type poisonObjectLocker struct {
	called atomic.Bool
}

func (p *poisonObjectLocker) IsObjectLockEnabled(_ context.Context) (bool, error) {
	p.called.Store(true)
	return false, nil
}

func (p *poisonObjectLocker) RetainedUntil(_ context.Context, _ backend.Handle) (time.Time, bool, error) {
	p.called.Store(true)
	return time.Time{}, false, nil
}

func (p *poisonObjectLocker) SetRetention(_ context.Context, _ backend.Handle, _ time.Time) error {
	p.called.Store(true)
	return nil
}

// TestRemoveForgetSnapshotsSkipObjectLockedFalseDoesNotClassify is angle 1
// described above: with skipObjectLocked false, a delete failing with
// backend.ErrObjectLocked must be treated exactly like any other delete
// failure -- added to failedSnIDs, not recorded in stats.ObjectLocked --
// and the ObjectLocker passed in must never be touched.
func TestRemoveForgetSnapshotsSkipObjectLockedFalseDoesNotClassify(t *testing.T) {
	lockedID := restic.Hash([]byte("locked"))
	unlockedIDs := []restic.ID{restic.Hash([]byte("unlocked-1")), restic.Hash([]byte("unlocked-2"))}

	removeSnIDs := restic.NewIDSet(lockedID)
	for _, id := range unlockedIDs {
		removeSnIDs.Insert(id)
	}

	remover := forgetRegressionMockRemover{lockedID: lockedID}
	locker := &poisonObjectLocker{}

	stats, failedSnIDs, err := removeForgetSnapshots(context.Background(), remover, removeSnIDs, false, locker, progress.NewNoopPrinter(), restic.NoopCounter)
	rtest.OK(t, err)

	rtest.Assert(t, !locker.called.Load(),
		"the ObjectLocker was used even though skipObjectLocked was false: the `skipObjectLocked && errors.Is(err, backend.ErrObjectLocked)` guard in removeForgetSnapshots (cmd_forget.go) must have been bypassed")

	rtest.Equals(t, len(removeSnIDs), stats.SelectedForRemoval)
	rtest.Equals(t, len(unlockedIDs), stats.Removed)
	rtest.Assert(t, len(stats.ObjectLocked) == 0,
		"expected no ObjectLocked entries when skipObjectLocked is false (the TASK-29 classification branch must not run), got %v", stats.ObjectLocked)

	rtest.Assert(t, failedSnIDs.Has(lockedID),
		"expected the locked snapshot's delete failure to be treated as an ordinary failure (added to failedSnIDs), exactly as forget has always done before this feature existed")
	rtest.Equals(t, 1, len(failedSnIDs))
}

// TestForgetWithoutSkipObjectLockedUnchanged is angle 2 described above: the
// full command, run against a real local MinIO Object-Lock-enabled bucket
// with one pre-locked, policy-selected snapshot, without
// --skip-object-locked.
func TestForgetWithoutSkipObjectLockedUnchanged(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tempdir := rtest.TempDir(t)
	key, secret := protectRandomCredentials(t)
	srv, cleanup := protectRunMinio(ctx, t, tempdir, key, secret)
	t.Cleanup(cleanup)

	client := protectNewMinioClient(t, srv)
	bucket := protectCreateLockedBucket(ctx, t, client)

	// The object-lock feature flag is enabled here (unlike a literal
	// pre-feature restic) so that the s3 backend's Remove call actually
	// observes the lock at all -- see removeObjectLockAware's comment in
	// internal/backend/s3/objectlock.go: without the flag, delete never
	// targets a specific object version and so can never fail on a lock in
	// the first place, which would make this test unable to exercise the
	// no-flag code path in removeForgetSnapshots at all. What this test
	// pins down is specifically forget's own default handling of that
	// failure once it occurs.
	t.Cleanup(feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true))

	prefix := fmt.Sprintf("forget-no-skip-object-locked-%d", time.Now().UnixNano())

	cfg := s3.NewConfig()
	cfg.Endpoint = srv.Endpoint
	cfg.Bucket = bucket
	cfg.Prefix = prefix
	cfg.UseHTTP = true
	cfg.KeyID = srv.KeyID
	cfg.Secret = options.NewSecretString(srv.Secret)

	be, err := s3.Open(ctx, cfg, nil, nil)
	rtest.OK(t, err)

	repo, _ := repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})

	// Two snapshots sharing one group: "--keep-last 1" selects "oldest" for
	// removal and keeps "newest".
	oldestBlob := protectSaveBlob(ctx, t, repo, []byte("forget no-skip-object-locked: oldest"))
	newestBlob := protectSaveBlob(ctx, t, repo, []byte("forget no-skip-object-locked: newest"))

	oldest := protectSaveSnapshot(ctx, t, repo, time.Unix(1500000000, 0), map[string]restic.IDs{"file": {oldestBlob}})
	newest := protectSaveSnapshot(ctx, t, repo, time.Unix(1600000000, 0), map[string]restic.IDs{"file": {newestBlob}})

	// Lock "oldest"'s snapshot file directly via the raw minio-go client --
	// not via `restic protect` -- so this test is not coupled to protect's
	// own behavior.
	l := layout.NewDefaultLayout(prefix, path.Join)
	lockedKey := l.Filename(backend.Handle{Type: restic.SnapshotFile, Name: oldest.ID().String()})
	retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	mode := minio.Compliance
	rtest.OK(t, client.PutObjectRetention(ctx, bucket, lockedKey, minio.PutObjectRetentionOptions{
		Mode:            &mode,
		RetainUntilDate: &retainUntil,
	}))

	t.Setenv("AWS_ACCESS_KEY_ID", srv.KeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", srv.Secret)

	gopts := protectTestGopts(srv, bucket, prefix, filepath.Join(tempdir, "cache"))

	opts := ForgetOptions{
		PolicySelectionOptions: PolicySelectionOptions{Last: 1},
		GroupBy:                data.SnapshotGroupByOptions{Host: true, Path: true},
		// SkipObjectLocked deliberately left false (the zero value): this is
		// the whole point of the test.
	}
	pruneOpts := PruneOptions{MaxUnused: "5%"}

	runErr := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runForget(ctx, opts, pruneOpts, gopts, gopts.Term, nil)
	})

	// Baseline behavior (established by running this exact fixture before
	// writing this assertion, per the spec's step 1): forget's default,
	// pre-existing generic delete-failure handling kicks in -- the same
	// ErrFailedToRemoveOneOrMoreSnapshots any other delete failure has
	// always produced, not a silent skip and not a different, new error.
	rtest.Equals(t, ErrFailedToRemoveOneOrMoreSnapshots, runErr)

	remaining := restic.NewIDSet()
	rtest.OK(t, repo.List(ctx, restic.SnapshotFile, func(id restic.ID, _ int64) error {
		remaining.Insert(id)
		return nil
	}))

	rtest.Assert(t, remaining.Has(*oldest.ID()), "expected the locked, selected-for-removal snapshot to survive forget without --skip-object-locked")
	rtest.Assert(t, remaining.Has(*newest.ID()), "expected the policy-kept snapshot to be untouched")
	rtest.Equals(t, 2, len(remaining))
}
