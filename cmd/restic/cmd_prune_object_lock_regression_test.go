package main

// TASK-37: regression test proving `restic prune` run WITHOUT
// --skip-object-locked behaves exactly as it did before this whole feature
// existed, even against a repository that contains a locked, unreferenced
// pack -- mirroring TASK-32's forget regression test
// (cmd_forget_object_lock_regression_test.go), adapted to prune.
//
// The pinned-down baseline differs from forget's, and step 1 of the spec
// ("establish the baseline") was run manually against this exact fixture
// before writing the assertions below to confirm it: prune's PackFile
// deletion has always ignored individual delete failures (the pre-TASK-34
// call was deleteFiles(ctx, true, ...), i.e. ignoreError=true -- see prune.go
// git history), simply logging a warning and leaving the file in place
// rather than failing the whole run. TASK-34's deleteUnusedPacks preserves
// that exact behavior: `_ = restic.ParallelRemove(...)` discards the
// aggregate error, and its `default:` branch (whichever error a delete
// returns, whenever `skipObjectLocked && errors.Is(err,
// backend.ErrObjectLocked)` doesn't apply) is byte-for-byte the same
// "log and continue" as the pre-feature call. So the observed, current
// behavior with the flag omitted is: runPrune returns nil (no error), the
// locked pack survives (left in place, exactly as any other undeletable pack
// always was), and the unlocked, unreferenced pack is removed. That is what
// this test pins down.
//
// This is the end-to-end angle; see
// internal/repository/prune_object_lock_regression_test.go's
// TestDeleteUnusedPacksSkipObjectLockedFalseDoesNotClassify for the fast,
// MinIO-free unit-test angle proving the TASK-34 classification branch
// itself is never entered when skipObjectLocked is false.

import (
	"context"
	"fmt"
	"os/exec"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/layout"
	"github.com/restic/restic/internal/backend/s3"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/options"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// TestPruneWithoutSkipObjectLockedUnchanged runs `restic prune` (no
// --skip-object-locked) against a real local MinIO Object-Lock-enabled
// bucket containing one still-referenced pack, one locked-and-unreferenced
// pack, and one unlocked-and-unreferenced pack, and asserts the pinned-down
// baseline behavior above still holds.
func TestPruneWithoutSkipObjectLockedUnchanged(t *testing.T) {
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
	// pre-feature restic) purely so the s3 backend's Remove call actually
	// targets a specific object version and can observe the lock at all --
	// see removeObjectLockAware's comment in
	// internal/backend/s3/objectlock.go: without the flag, delete never
	// targets a specific object version and so can never fail on a lock in
	// the first place, which would make this test unable to exercise the
	// no-flag code path in deleteUnusedPacks at all. What this test pins
	// down is specifically prune's own default handling of that failure
	// once it occurs, not whether the failure occurs.
	t.Cleanup(feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true))

	prefix := fmt.Sprintf("prune-no-skip-object-locked-%d", time.Now().UnixNano())

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

	// Each protectSaveBlob call flushes its own pack immediately, so these
	// three blobs land in three separate, individually addressable packs.
	usedBlob := protectSaveBlob(ctx, t, repo, []byte("prune no-skip-object-locked: used"))
	lockedBlob := protectSaveBlob(ctx, t, repo, []byte("prune no-skip-object-locked: locked"))
	freeBlob := protectSaveBlob(ctx, t, repo, []byte("prune no-skip-object-locked: free"))

	// Only usedBlob is referenced by a snapshot; lockedBlob's and freeBlob's
	// packs are therefore wholly unreferenced from the moment they're
	// created.
	protectSaveSnapshot(ctx, t, repo, time.Unix(1600000000, 0), map[string]restic.IDs{"file": {usedBlob}})

	packOf := func(id restic.ID) restic.ID {
		packs := repo.LookupBlob(restic.BlobHandle{Type: restic.DataBlob, ID: id})
		rtest.Assert(t, len(packs) == 1, "expected exactly one pack for blob %v, got %d", id, len(packs))
		return packs[0].PackID()
	}
	usedPack := packOf(usedBlob)
	lockedPack := packOf(lockedBlob)
	freePack := packOf(freeBlob)

	// Lock lockedPack's pack file directly via the raw minio-go client --
	// not via `restic protect` -- so this test is not coupled to protect's
	// own behavior.
	l := layout.NewDefaultLayout(prefix, path.Join)
	lockedKey := l.Filename(backend.Handle{Type: restic.PackFile, Name: lockedPack.String()})
	retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	mode := minio.Compliance
	rtest.OK(t, client.PutObjectRetention(ctx, bucket, lockedKey, minio.PutObjectRetentionOptions{
		Mode:            &mode,
		RetainUntilDate: &retainUntil,
	}))

	t.Setenv("AWS_ACCESS_KEY_ID", srv.KeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", srv.Secret)

	gopts := protectTestGopts(srv, bucket, prefix, filepath.Join(tempdir, "cache"))

	opts := PruneOptions{
		MaxUnused: "0%",
		// SkipObjectLocked deliberately left false (the zero value): this is
		// the whole point of the test.
	}

	runErr := withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runPrune(ctx, opts, gopts, gopts.Term)
	})

	// Baseline behavior (established by running this exact fixture before
	// writing this assertion, per the spec's step 1): prune's pre-existing,
	// pre-this-feature pack deletion ignores individual delete failures --
	// the run succeeds with no error, the locked pack is left in place, and
	// the unlocked one is removed.
	rtest.OK(t, runErr)

	remaining := restic.NewIDSet()
	rtest.OK(t, repo.List(ctx, restic.PackFile, func(id restic.ID, _ int64) error {
		remaining.Insert(id)
		return nil
	}))

	rtest.Assert(t, remaining.Has(usedPack), "expected the still-used pack to survive prune")
	rtest.Assert(t, remaining.Has(lockedPack), "expected the object-locked, unreferenced pack to survive prune without --skip-object-locked (the pre-feature baseline: individual delete failures are logged and left in place, not fatal)")
	rtest.Assert(t, !remaining.Has(freePack), "expected the unlocked, unreferenced pack to be removed by prune")
}
