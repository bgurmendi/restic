package main

// TASK-34: integration test proving `prune --skip-object-locked`'s
// delete-and-classify pack-deletion path (deleteUnusedPacks in
// internal/repository/prune.go) against a real, locally-run MinIO
// Object-Lock-enabled bucket. Reuses the MinIO process-spawning harness and
// protectSaveBlob/protectSaveSnapshot helpers already established for
// `protect`'s and `forget`'s own integration tests (cmd_protect_integration_test.go,
// cmd_forget_object_lock_integration_test.go, same package), so this file
// only adds what's specific to prune's scenario.
//
// TASK-36 adds the larger, mixed-scale integration test on top of this one
// (TestPruneSkipObjectLockedIntegration below), mirroring how TASK-31 built
// on top of TASK-29's forget test.

import (
	"bytes"
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

// pruneSkipObjectLockedFixture is what setupPruneSkipObjectLockedFixture
// builds: a real local MinIO Object-Lock-enabled bucket containing one
// snapshot that keeps one pack in use, plus two further, wholly
// unreferenced (never part of any snapshot) packs -- exactly what prune
// considers "unused" and selects for removal, with no forget step needed to
// get there -- of which one is pre-locked directly via the raw minio-go
// client (simulating an earlier `restic protect` run) and the other left
// unlocked.
type pruneSkipObjectLockedFixture struct {
	ctx        context.Context
	gopts      global.Options
	opts       PruneOptions
	repo       *repository.Repository
	usedPack   restic.ID // still referenced by the one snapshot: must survive
	lockedPack restic.ID // unreferenced, object-locked: must survive, left pending
	freePack   restic.ID // unreferenced, unlocked: must be removed
}

func setupPruneSkipObjectLockedFixture(t *testing.T) pruneSkipObjectLockedFixture {
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

	t.Cleanup(feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true))

	prefix := fmt.Sprintf("prune-skip-locked-%d", time.Now().UnixNano())

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
	usedBlob := protectSaveBlob(ctx, t, repo, []byte("prune skip-object-locked: used"))
	lockedBlob := protectSaveBlob(ctx, t, repo, []byte("prune skip-object-locked: locked"))
	freeBlob := protectSaveBlob(ctx, t, repo, []byte("prune skip-object-locked: free"))

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
	// simulating an earlier `restic protect` run, not going through protect
	// itself -- leaving freePack unlocked.
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

	opts := PruneOptions{MaxUnused: "0%", SkipObjectLocked: true}

	return pruneSkipObjectLockedFixture{
		ctx:        ctx,
		gopts:      gopts,
		opts:       opts,
		repo:       repo,
		usedPack:   usedPack,
		lockedPack: lockedPack,
		freePack:   freePack,
	}
}

// TestPruneSkipObjectLocked runs `restic prune --skip-object-locked` against
// the fixture and asserts the locked, unreferenced pack survives (left
// pending for a future run), the unlocked, unreferenced pack is actually
// removed, and the pack still needed by a live snapshot is untouched.
func TestPruneSkipObjectLocked(t *testing.T) {
	f := setupPruneSkipObjectLockedFixture(t)

	err := withTermStatus(t, f.gopts, func(ctx context.Context, gopts global.Options) error {
		return runPrune(ctx, f.opts, gopts, gopts.Term)
	})
	rtest.OK(t, err)

	remaining := restic.NewIDSet()
	rtest.OK(t, f.repo.List(f.ctx, restic.PackFile, func(id restic.ID, _ int64) error {
		remaining.Insert(id)
		return nil
	}))

	rtest.Assert(t, remaining.Has(f.usedPack), "expected the still-used pack to survive prune")
	rtest.Assert(t, remaining.Has(f.lockedPack), "expected the object-locked, unreferenced pack to survive prune --skip-object-locked")
	rtest.Assert(t, !remaining.Has(f.freePack), "expected the unlocked, unreferenced pack to be removed by prune")
}

// TestPruneSkipObjectLockedSummaryOutput is TASK-35's plain-text acceptance
// test: against the same fixture as TestPruneSkipObjectLocked (one used
// pack, one locked-and-unreferenced pack, one unlocked-and-unreferenced
// pack), running `prune --skip-object-locked` at default verbosity must
// print the BASE.md-documented summary for prune ("unused packs: 2",
// "removed: 1", "object locked: 1" -- of the two unused packs, one removed,
// one still locked).
func TestPruneSkipObjectLockedSummaryOutput(t *testing.T) {
	f := setupPruneSkipObjectLockedFixture(t)
	// printer.P requires verbosity >= 1 to print; the normal CLI entrypoint
	// sets this default (global.go's ApplyEnvironment) before tests here
	// bypass it by calling runPrune directly.
	f.gopts.Verbosity = 1

	buf, err := withCaptureStdout(t, f.gopts, func(ctx context.Context, gopts global.Options) error {
		return runPrune(ctx, f.opts, gopts, gopts.Term)
	})
	rtest.OK(t, err)

	output := buf.String()
	rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("unused packs: 2\n")),
		"expected \"unused packs: 2\" in output, got:\n%s", output)
	rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("removed: 1\n")),
		"expected \"removed: 1\" in output, got:\n%s", output)
	rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("object locked: 1\n")),
		"expected \"object locked: 1\" in output, got:\n%s", output)
}

// TestPruneSkipObjectLockedRequiresObjectLockerBackend confirms the upfront
// capability check: `prune --skip-object-locked` against a backend that
// doesn't implement backend.ObjectLocker at all (a plain local filesystem
// repository, standing in for "any non-s3 backend") fails clearly before
// attempting any removal, rather than silently ignoring the flag --
// mirroring TestForgetSkipObjectLockedRequiresObjectLockerBackend.
func TestPruneSkipObjectLockedRequiresObjectLockerBackend(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env.gopts.BackendTestHook = nil

	testRunInit(t, env.gopts)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	opts := PruneOptions{MaxUnused: "5%", SkipObjectLocked: true}

	err := withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		return runPrune(ctx, opts, gopts, gopts.Term)
	})
	rtest.Assert(t, err != nil, "expected an error against a backend without Object Lock support")
	rtest.Equals(t,
		"Fatal: the `prune` command requires a backend with S3 Object Lock support (currently only the `s3` backend); this repository uses the \"local\" backend, which does not support it",
		err.Error())
}

// pruneSkipObjectLockedIntegrationFixture is TASK-36's own, larger fixture:
// one snapshot keeping one pack in use (itself locked too, to prove locking
// has no bearing on packs prune never selects), plus four further, wholly
// unreferenced packs of which only one is pre-locked -- so unused/removed/
// locked (4/3/1) mirrors the shape (not the exact numbers) of BASE.md's
// worked example ("unused packs: 93 / removed: 74 / object locked: 19") at a
// small, deterministic scale, the same way
// forgetSkipObjectLockedIntegrationFixture mirrors it for forget.
type pruneSkipObjectLockedIntegrationFixture struct {
	ctx        context.Context
	gopts      global.Options
	opts       PruneOptions
	repo       *repository.Repository
	usedPack   restic.ID   // still referenced by the kept snapshot, itself locked too: must survive regardless of lock status
	lockedPack restic.ID   // unreferenced, object-locked: must survive, left pending
	freePacks  []restic.ID // unreferenced, unlocked: must be removed
}

func setupPruneSkipObjectLockedIntegrationFixture(t *testing.T) pruneSkipObjectLockedIntegrationFixture {
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

	t.Cleanup(feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true))

	prefix := fmt.Sprintf("prune-skip-locked-integration-%d", time.Now().UnixNano())

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
	// five blobs land in five separate, individually addressable packs.
	usedBlob := protectSaveBlob(ctx, t, repo, []byte("prune skip-object-locked integration: used"))
	lockedBlob := protectSaveBlob(ctx, t, repo, []byte("prune skip-object-locked integration: locked"))
	var freeBlobs []restic.ID
	for i := 0; i < 3; i++ {
		freeBlobs = append(freeBlobs, protectSaveBlob(ctx, t, repo, []byte(fmt.Sprintf("prune skip-object-locked integration: free %d", i))))
	}

	// Only usedBlob is referenced by a snapshot; lockedBlob's and the three
	// freeBlobs' packs are therefore wholly unreferenced from the moment
	// they're created.
	protectSaveSnapshot(ctx, t, repo, time.Unix(1600000000, 0), map[string]restic.IDs{"file": {usedBlob}})

	packOf := func(id restic.ID) restic.ID {
		packs := repo.LookupBlob(restic.BlobHandle{Type: restic.DataBlob, ID: id})
		rtest.Assert(t, len(packs) == 1, "expected exactly one pack for blob %v, got %d", id, len(packs))
		return packs[0].PackID()
	}
	usedPack := packOf(usedBlob)
	lockedPack := packOf(lockedBlob)
	var freePacks []restic.ID
	for _, blob := range freeBlobs {
		freePacks = append(freePacks, packOf(blob))
	}

	// Lock usedPack's and lockedPack's pack files directly via the raw
	// minio-go client -- simulating an earlier `restic protect` run, not
	// going through protect itself -- leaving the three freePacks unlocked.
	// usedPack is locked too so the outcome proves prune never even attempts
	// to touch a still-referenced pack, regardless of its lock status.
	l := layout.NewDefaultLayout(prefix, path.Join)
	retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	mode := minio.Compliance
	for _, id := range []restic.ID{usedPack, lockedPack} {
		key := l.Filename(backend.Handle{Type: restic.PackFile, Name: id.String()})
		rtest.OK(t, client.PutObjectRetention(ctx, bucket, key, minio.PutObjectRetentionOptions{
			Mode:            &mode,
			RetainUntilDate: &retainUntil,
		}))
	}

	t.Setenv("AWS_ACCESS_KEY_ID", srv.KeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", srv.Secret)

	gopts := protectTestGopts(srv, bucket, prefix, filepath.Join(tempdir, "cache"))

	opts := PruneOptions{MaxUnused: "0%", SkipObjectLocked: true}

	return pruneSkipObjectLockedIntegrationFixture{
		ctx:        ctx,
		gopts:      gopts,
		opts:       opts,
		repo:       repo,
		usedPack:   usedPack,
		lockedPack: lockedPack,
		freePacks:  freePacks,
	}
}

// TestPruneSkipObjectLockedIntegration is the definitive, consolidated
// acceptance test for this feature slice (TASK-36), the prune-side
// counterpart to TestForgetSkipObjectLockedIntegration: against one
// realistic mixed locked/unlocked/used scenario (see
// pruneSkipObjectLockedIntegrationFixture above), it combines TASK-34's
// delete-and-classify pack removal (which packs survive/are removed) and
// TASK-35's reporting (the exact unused/removed/object-locked counts) into a
// single scenario.
func TestPruneSkipObjectLockedIntegration(t *testing.T) {
	f := setupPruneSkipObjectLockedIntegrationFixture(t)
	f.gopts.Verbosity = 1

	buf, err := withCaptureStdout(t, f.gopts, func(ctx context.Context, gopts global.Options) error {
		return runPrune(ctx, f.opts, gopts, gopts.Term)
	})
	rtest.OK(t, err)

	remaining := restic.NewIDSet()
	rtest.OK(t, f.repo.List(f.ctx, restic.PackFile, func(id restic.ID, _ int64) error {
		remaining.Insert(id)
		return nil
	}))

	rtest.Assert(t, remaining.Has(f.usedPack), "expected the still-used, locked pack to survive prune --skip-object-locked regardless of its lock status")
	rtest.Assert(t, remaining.Has(f.lockedPack), "expected the object-locked, unreferenced pack to survive prune --skip-object-locked")
	for _, id := range f.freePacks {
		rtest.Assert(t, !remaining.Has(id), "expected unlocked, unreferenced pack %v to be removed by prune", id)
	}

	output := buf.String()
	rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("unused packs: 4\n")),
		"expected \"unused packs: 4\" in output, got:\n%s", output)
	rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("removed: 3\n")),
		"expected \"removed: 3\" in output, got:\n%s", output)
	rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("object locked: 1\n")),
		"expected \"object locked: 1\" in output, got:\n%s", output)
}
