package main

// TASK-29: integration test proving `forget --skip-object-locked`'s
// delete-and-classify deletion path (removeForgetSnapshots) against a real,
// locally-run MinIO Object-Lock-enabled bucket. Reuses the TASK-2/3 MinIO
// process-spawning harness and the protectSaveBlob/protectSaveSnapshot
// deterministic-sharing helpers already established for `protect`'s own
// integration tests in cmd_protect_integration_test.go (same package), so
// this file only adds what's specific to forget's scenario.
//
// TASK-30 adds two more tests on top of the same fixture (factored out into
// setupForgetSkipObjectLockedFixture below): one asserting the plain-text
// "selected for removal / removed / object locked" summary, one asserting
// the equivalent --json fields.

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/options"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// forgetSkipObjectLockedFixture is what setupForgetSkipObjectLockedFixture
// builds: a real local MinIO Object-Lock-enabled bucket with three
// snapshots sharing one group (host+paths), of which a "--keep-last 1"
// policy selects "oldest" and "middle" for removal, "oldest" pre-locked
// directly via the raw minio-go client (simulating an earlier `restic
// protect` run), "middle" left unlocked, and "newest" kept by the policy.
type forgetSkipObjectLockedFixture struct {
	ctx       context.Context
	gopts     global.Options
	opts      ForgetOptions
	pruneOpts PruneOptions
	repo      *repository.Repository
	oldest    *data.Snapshot
	middle    *data.Snapshot
	newest    *data.Snapshot
}

func setupForgetSkipObjectLockedFixture(t *testing.T) forgetSkipObjectLockedFixture {
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

	prefix := fmt.Sprintf("forget-skip-locked-%d", time.Now().UnixNano())

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

	// Three snapshots sharing hostname/paths (via protectSaveSnapshot), so
	// they fall into a single protectDefaultGroupBy group: "--keep-last 1"
	// keeps only "newest" and selects "oldest" and "middle" for removal.
	oldestBlob := protectSaveBlob(ctx, t, repo, []byte("forget skip-object-locked: oldest"))
	middleBlob := protectSaveBlob(ctx, t, repo, []byte("forget skip-object-locked: middle"))
	newestBlob := protectSaveBlob(ctx, t, repo, []byte("forget skip-object-locked: newest"))

	oldest := protectSaveSnapshot(ctx, t, repo, time.Unix(1500000000, 0), map[string]restic.IDs{"file": {oldestBlob}})
	middle := protectSaveSnapshot(ctx, t, repo, time.Unix(1550000000, 0), map[string]restic.IDs{"file": {middleBlob}})
	newest := protectSaveSnapshot(ctx, t, repo, time.Unix(1600000000, 0), map[string]restic.IDs{"file": {newestBlob}})

	// Lock "oldest"'s snapshot file directly via the raw minio-go client --
	// simulating an earlier `restic protect` run, not going through protect
	// itself -- leaving "middle" unlocked.
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
		SkipObjectLocked:       true,
	}
	pruneOpts := PruneOptions{MaxUnused: "5%"}

	return forgetSkipObjectLockedFixture{
		ctx:       ctx,
		gopts:     gopts,
		opts:      opts,
		pruneOpts: pruneOpts,
		repo:      repo,
		oldest:    oldest,
		middle:    middle,
		newest:    newest,
	}
}

// TestForgetSkipObjectLocked runs `restic forget --skip-object-locked`
// against the fixture and asserts the locked snapshot file survives, the
// unlocked one is actually removed, and the kept (newest) snapshot is
// untouched.
func TestForgetSkipObjectLocked(t *testing.T) {
	f := setupForgetSkipObjectLockedFixture(t)

	err := withTermStatus(t, f.gopts, func(ctx context.Context, gopts global.Options) error {
		return runForget(ctx, f.opts, f.pruneOpts, gopts, gopts.Term, nil)
	})
	rtest.OK(t, err)

	remaining := restic.NewIDSet()
	rtest.OK(t, f.repo.List(f.ctx, restic.SnapshotFile, func(id restic.ID, _ int64) error {
		remaining.Insert(id)
		return nil
	}))

	rtest.Assert(t, remaining.Has(*f.oldest.ID()), "expected the locked snapshot to survive forget --skip-object-locked")
	rtest.Assert(t, !remaining.Has(*f.middle.ID()), "expected the unlocked, selected-for-removal snapshot to be removed")
	rtest.Assert(t, remaining.Has(*f.newest.ID()), "expected the policy-kept snapshot to be untouched")
	rtest.Equals(t, 2, len(remaining))
}

// TestForgetSkipObjectLockedSummaryOutput is TASK-30's plain-text
// acceptance test: against the same mixed locked/unlocked fixture, running
// `forget --skip-object-locked` at default verbosity must print the
// BASE.md-documented summary ("selected for removal: 2", "removed: 1",
// "object locked: 1" -- one snapshot removed, one still locked, out of the
// two selected).
func TestForgetSkipObjectLockedSummaryOutput(t *testing.T) {
	f := setupForgetSkipObjectLockedFixture(t)
	// printer.P requires verbosity >= 1 to print; the normal CLI entrypoint
	// sets this default (global.go's ApplyEnvironment) before tests here
	// bypass it by calling runForget directly.
	f.gopts.Verbosity = 1

	buf, err := withCaptureStdout(t, f.gopts, func(ctx context.Context, gopts global.Options) error {
		return runForget(ctx, f.opts, f.pruneOpts, gopts, gopts.Term, nil)
	})
	rtest.OK(t, err)

	output := buf.String()
	rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("selected for removal: 2\n")),
		"expected \"selected for removal: 2\" in output, got:\n%s", output)
	rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("removed: 1\n")),
		"expected \"removed: 1\" in output, got:\n%s", output)
	rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("object locked: 1\n")),
		"expected \"object locked: 1\" in output, got:\n%s", output)
}

// TestForgetSkipObjectLockedJSONOutput is TASK-30's --json acceptance test:
// against the same fixture, `forget --skip-object-locked --json` must
// produce a JSON object (not the plain []*ForgetGroup array `forget --json`
// emits without the flag) with the same counts as structured, numeric
// fields.
func TestForgetSkipObjectLockedJSONOutput(t *testing.T) {
	f := setupForgetSkipObjectLockedFixture(t)
	f.gopts.JSON = true

	buf, err := withCaptureStdout(t, f.gopts, func(ctx context.Context, gopts global.Options) error {
		return runForget(ctx, f.opts, f.pruneOpts, gopts, gopts.Term, nil)
	})
	rtest.OK(t, err)

	var result ForgetJSONResult
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &result))

	rtest.Equals(t, 2, result.SelectedForRemoval)
	rtest.Equals(t, 1, result.Removed)
	rtest.Equals(t, 1, result.ObjectLocked)
	rtest.Assert(t, len(result.Groups) == 1, "expected 1 snapshot group, got %v", len(result.Groups))
}

// forgetSkipObjectLockedIntegrationFixture is TASK-31's own, larger fixture:
// six snapshots sharing one protectDefaultGroupBy group, "--keep-last 2"
// selecting the four oldest for removal, of which only the very oldest is
// pre-locked -- so selected/removed/locked (4/3/1) has more removed than
// locked, mirroring the shape (not the exact numbers) of BASE.md's worked
// example ("selected for removal: 15 / removed: 11 / object locked: 4") at a
// small, deterministic scale, rather than TestForgetSkipObjectLocked's
// minimal 2/1/1 fixture above.
type forgetSkipObjectLockedIntegrationFixture struct {
	gopts     global.Options
	opts      ForgetOptions
	pruneOpts PruneOptions
	repo      *repository.Repository
	locked    *data.Snapshot   // still object-locked: selected for removal, must survive
	removed   []*data.Snapshot // unlocked: selected for removal, must be gone
	kept      []*data.Snapshot // outside the policy's selection: must be untouched
}

func setupForgetSkipObjectLockedIntegrationFixture(t *testing.T) forgetSkipObjectLockedIntegrationFixture {
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

	prefix := fmt.Sprintf("forget-skip-locked-integration-%d", time.Now().UnixNano())

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

	// Six snapshots, oldest to newest, all in one group: "--keep-last 2"
	// keeps the two newest and selects the four oldest for removal.
	var snapshots []*data.Snapshot
	for i := 0; i < 6; i++ {
		blob := protectSaveBlob(ctx, t, repo, []byte(fmt.Sprintf("forget skip-object-locked integration: snapshot %d", i)))
		at := time.Unix(1500000000+int64(i)*100000, 0)
		snapshots = append(snapshots, protectSaveSnapshot(ctx, t, repo, at, map[string]restic.IDs{"file": {blob}}))
	}

	// Lock only the very oldest (of the four selected for removal) directly
	// via the raw minio-go client -- simulating an earlier `restic protect`
	// run -- leaving the other three selected snapshots unlocked.
	l := layout.NewDefaultLayout(prefix, path.Join)
	lockedKey := l.Filename(backend.Handle{Type: restic.SnapshotFile, Name: snapshots[0].ID().String()})
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
		PolicySelectionOptions: PolicySelectionOptions{Last: 2},
		GroupBy:                data.SnapshotGroupByOptions{Host: true, Path: true},
		SkipObjectLocked:       true,
	}
	pruneOpts := PruneOptions{MaxUnused: "5%"}

	return forgetSkipObjectLockedIntegrationFixture{
		gopts:     gopts,
		opts:      opts,
		pruneOpts: pruneOpts,
		repo:      repo,
		locked:    snapshots[0],
		removed:   snapshots[1:4],
		kept:      snapshots[4:6],
	}
}

// assertOutcome checks the post-forget repository state common to both
// subtests below: the locked snapshot survives, every unlocked
// selected-for-removal snapshot is gone, and every kept snapshot is
// untouched.
func (f forgetSkipObjectLockedIntegrationFixture) assertOutcome(ctx context.Context, t *testing.T) {
	t.Helper()

	remaining := restic.NewIDSet()
	rtest.OK(t, f.repo.List(ctx, restic.SnapshotFile, func(id restic.ID, _ int64) error {
		remaining.Insert(id)
		return nil
	}))

	rtest.Assert(t, remaining.Has(*f.locked.ID()), "expected the locked snapshot to survive forget --skip-object-locked")
	for _, sn := range f.removed {
		rtest.Assert(t, !remaining.Has(*sn.ID()), "expected unlocked, selected-for-removal snapshot %v to be removed", sn.ID())
	}
	for _, sn := range f.kept {
		rtest.Assert(t, remaining.Has(*sn.ID()), "expected policy-kept snapshot %v to be untouched", sn.ID())
	}
	rtest.Equals(t, 1+len(f.kept), len(remaining))
}

// TestForgetSkipObjectLockedIntegration is the definitive, consolidated
// regression test for this feature slice (TASK-31): against one realistic
// mixed locked/unlocked/kept scenario (see
// forgetSkipObjectLockedIntegrationFixture above), it combines TASK-29's
// delete-and-classify behavior (which snapshot files survive/are removed)
// and TASK-30's reporting (the exact selected/removed/object-locked counts)
// into a single scenario, checked once via plain-text output and once via
// --json output.
func TestForgetSkipObjectLockedIntegration(t *testing.T) {
	t.Run("plain text output", func(t *testing.T) {
		f := setupForgetSkipObjectLockedIntegrationFixture(t)
		f.gopts.Verbosity = 1

		buf, err := withCaptureStdout(t, f.gopts, func(ctx context.Context, gopts global.Options) error {
			return runForget(ctx, f.opts, f.pruneOpts, gopts, gopts.Term, nil)
		})
		rtest.OK(t, err)

		f.assertOutcome(context.Background(), t)

		output := buf.String()
		rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("selected for removal: 4\n")),
			"expected \"selected for removal: 4\" in output, got:\n%s", output)
		rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("removed: 3\n")),
			"expected \"removed: 3\" in output, got:\n%s", output)
		rtest.Assert(t, bytes.Contains(buf.Bytes(), []byte("object locked: 1\n")),
			"expected \"object locked: 1\" in output, got:\n%s", output)
	})

	t.Run("json output", func(t *testing.T) {
		f := setupForgetSkipObjectLockedIntegrationFixture(t)
		f.gopts.JSON = true

		buf, err := withCaptureStdout(t, f.gopts, func(ctx context.Context, gopts global.Options) error {
			return runForget(ctx, f.opts, f.pruneOpts, gopts, gopts.Term, nil)
		})
		rtest.OK(t, err)

		f.assertOutcome(context.Background(), t)

		var result ForgetJSONResult
		rtest.OK(t, json.Unmarshal(buf.Bytes(), &result))

		rtest.Equals(t, 4, result.SelectedForRemoval)
		rtest.Equals(t, 3, result.Removed)
		rtest.Equals(t, 1, result.ObjectLocked)
		rtest.Assert(t, len(result.Groups) == 1, "expected 1 snapshot group, got %v", len(result.Groups))
	})
}

// TestForgetSkipObjectLockedRequiresObjectLockerBackend confirms TASK-29's
// upfront capability check: `forget --skip-object-locked` against a backend
// that doesn't implement backend.ObjectLocker at all (a plain local
// filesystem repository, standing in for "any non-s3 backend") fails
// clearly before attempting any removal, rather than silently ignoring the
// flag -- mirroring TestProtectIntegrationPreflightFailsOnUnsupportedBackend's
// approach for `protect`.
func TestForgetSkipObjectLockedRequiresObjectLockerBackend(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env.gopts.BackendTestHook = nil

	testRunInit(t, env.gopts)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	opts := ForgetOptions{
		PolicySelectionOptions: PolicySelectionOptions{Last: 0},
		GroupBy:                data.SnapshotGroupByOptions{Host: true, Path: true},
		SkipObjectLocked:       true,
	}

	err := testRunForgetMayFail(t, env.gopts, opts)
	rtest.Assert(t, err != nil, "expected an error against a backend without Object Lock support")
	rtest.Equals(t,
		"Fatal: the `forget` command requires a backend with S3 Object Lock support (currently only the `s3` backend); this repository uses the \"local\" backend, which does not support it",
		err.Error())
}
