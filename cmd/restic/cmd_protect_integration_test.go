package main

// TASK-23: a basic integration test proving `restic protect`'s object-set
// logic (runProtectApply) against a real local MinIO Object-Lock-enabled
// bucket, with a real restic repository initialized on top of it (not just
// raw objects) -- reusing the same real-MinIO technique the TASK-2/TASK-3
// harness in internal/backend/s3 established (spawn a local `minio server`
// process, create a bucket with MakeBucketOptions{ObjectLocking: true}).
// That harness itself lives in the s3_test package (internal test helpers,
// see TASK-2's technical notes), which _test.go files in another package
// cannot import, so the minimal process-spawning/bucket-fixture pieces are
// reproduced here rather than duplicating the whole suite. TASK-25 expands
// this into the full "protect locks exactly the expected object set" test.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/all"
	"github.com/restic/restic/internal/backend/layout"
	"github.com/restic/restic/internal/backend/s3"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/options"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

// protectMinioServer holds the connection details of a local minio server
// started by protectRunMinio, mirroring internal/backend/s3/s3_test.go's
// minioServer/runMinio.
type protectMinioServer struct {
	Endpoint string
	KeyID    string
	Secret   string
}

// protectRunMinio starts a local `minio server` process rooted at a fresh
// temporary directory below dir, waits until its TCP port is reachable, and
// returns a cleanup function the caller must invoke (e.g. via defer).
func protectRunMinio(ctx context.Context, t testing.TB, dir, key, secret string) (protectMinioServer, func()) {
	t.Helper()

	for _, sub := range []string{"config", "root"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0700); err != nil {
			t.Fatal(err)
		}
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(ctx, "minio",
		"server",
		"--address", addr,
		"--config-dir", filepath.Join(dir, "config"),
		filepath.Join(dir, "root"))
	cmd.Env = append(os.Environ(),
		"MINIO_ACCESS_KEY="+key,
		"MINIO_SECRET_KEY="+secret,
	)
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	var success bool
	for i := 0; i < 100; i++ {
		time.Sleep(200 * time.Millisecond)
		c, err := net.Dial("tcp", addr)
		if err == nil {
			success = true
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if !success {
		t.Fatal("unable to connect to minio server")
	}

	cleanup := func() {
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		_ = cmd.Wait()
	}

	return protectMinioServer{Endpoint: addr, KeyID: key, Secret: secret}, cleanup
}

func protectRandomCredentials(t testing.TB) (key, secret string) {
	t.Helper()
	buf := make([]byte, 10)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		t.Fatal(err)
	}
	key = hex.EncodeToString(buf)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		t.Fatal(err)
	}
	secret = hex.EncodeToString(buf)
	return key, secret
}

func protectNewMinioClient(t testing.TB, srv protectMinioServer) *minio.Client {
	t.Helper()
	client, err := minio.New(srv.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(srv.KeyID, srv.Secret, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// protectCreateLockedBucket creates a bucket with Object Lock enabled at
// creation time, matching TASK-3's fixture.
func protectCreateLockedBucket(ctx context.Context, t testing.TB, client *minio.Client) string {
	t.Helper()
	buf := make([]byte, 6)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		t.Fatal(err)
	}
	bucket := fmt.Sprintf("protect-locked-%s", hex.EncodeToString(buf))
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{ObjectLocking: true}); err != nil {
		t.Fatal(err)
	}
	return bucket
}

// TestProtectIntegrationAppliesRetentionAndExcludesLocks initializes a real
// restic repository on top of a real, locally-run MinIO Object-Lock-enabled
// bucket, creates two snapshots that only partially share data, runs
// runProtectApply on just one of them (simulating a keep-* policy that would
// discard the other), and confirms via the raw minio-go client that exactly
// the expected object set - config, keys/*, index/*, the protected
// snapshot's file, and its live packs - has Object Lock retention set to the
// requested date, and that a real lock file created via repository.LockRepo
// never receives any retention at all.
func TestProtectIntegrationAppliesRetentionAndExcludesLocks(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := protectRandomCredentials(t)
	srv, cleanup := protectRunMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := protectNewMinioClient(t, srv)
	bucket := protectCreateLockedBucket(ctx, t, client)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	cfg := s3.NewConfig()
	cfg.Endpoint = srv.Endpoint
	cfg.Bucket = bucket
	cfg.Prefix = fmt.Sprintf("protect-test-%d", time.Now().UnixNano())
	cfg.UseHTTP = true
	cfg.KeyID = srv.KeyID
	cfg.Secret = options.NewSecretString(srv.Secret)

	be, err := s3.Open(ctx, cfg, nil, nil)
	rtest.OK(t, err)

	repo, _ := repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})

	// Two independently-seeded snapshots, so they only partially share packs
	// (same technique as internal/repository/live_packs_test.go).
	older := data.TestCreateSnapshot(t, repo, time.Unix(1500000000, 0), 3)
	newer := data.TestCreateSnapshot(t, repo, time.Unix(1600000000, 0), 3)

	// A real lock file, written exactly as `restic protect`'s own
	// append-lock (openWithAppendLock) would create one.
	unlock, lockCtx, err := repository.LockRepo(ctx, repo, false, 0, func(string) {}, func(string, ...interface{}) {})
	rtest.OK(t, err)
	defer unlock.Unlock()

	printer := progress.NewNoopPrinter()
	retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)

	// Only "newer" is protected, simulating a policy that would keep it and
	// discard "older".
	objects, err := runProtectApply(lockCtx, repo, printer, data.Snapshots{newer}, retainUntil, false)
	rtest.OK(t, err)

	rtest.Equals(t, 1, len(objects.Config))
	rtest.Assert(t, len(objects.Keys) > 0, "expected at least one key handle")
	rtest.Assert(t, len(objects.Index) > 0, "expected at least one index handle")
	rtest.Equals(t, 1, len(objects.Snapshots))
	rtest.Assert(t, len(objects.Packs) > 0, "expected at least one pack handle")

	l := layout.NewDefaultLayout(cfg.Prefix, path.Join)

	// Every object in the returned set genuinely has COMPLIANCE retention
	// set to retainUntil, verified through the raw minio-go client -- not
	// just through the backend's own (potentially self-consistent-but-wrong)
	// reporting.
	for _, h := range objects.all() {
		objKey := l.Filename(h)
		mode, gotRetainUntil, err := client.GetObjectRetention(ctx, bucket, objKey, "")
		rtest.OK(t, err)
		rtest.Assert(t, mode != nil && *mode == minio.Compliance, "expected COMPLIANCE retention on %v, got %v", h, mode)
		rtest.Assert(t, gotRetainUntil != nil && gotRetainUntil.Truncate(time.Second).Equal(retainUntil),
			"expected retainUntil %v on %v, got %v", retainUntil, h, gotRetainUntil)
	}

	// older's exclusive pack(s) must never have received retention.
	olderPacks, err := repository.LivePacksForSnapshots(ctx, repo, restic.NewIDSet(*older.ID()), printer)
	rtest.OK(t, err)
	newerPacks, err := repository.LivePacksForSnapshots(ctx, repo, restic.NewIDSet(*newer.ID()), printer)
	rtest.OK(t, err)
	exclusiveToOlder := false
	for id := range olderPacks {
		if newerPacks.Has(id) {
			continue
		}
		exclusiveToOlder = true
		h := backend.Handle{Type: restic.PackFile, Name: id.String()}
		objKey := l.Filename(h)
		_, _, err := client.GetObjectRetention(ctx, bucket, objKey, "")
		rtest.Assert(t, err != nil, "expected no retention configuration on older's exclusive pack %v", id)
	}
	rtest.Assert(t, exclusiveToOlder, "expected the older snapshot to need at least one pack the newer one does not")

	// The lock file created above by LockRepo must never have received
	// retention either.
	var lockKey string
	rtest.OK(t, repo.List(ctx, restic.LockFile, func(id restic.ID, _ int64) error {
		lockKey = l.Filename(backend.Handle{Type: restic.LockFile, Name: id.String()})
		return nil
	}))
	rtest.Assert(t, lockKey != "", "expected a lock file to exist")
	_, _, err = client.GetObjectRetention(ctx, bucket, lockKey, "")
	rtest.Assert(t, err != nil, "expected no retention configuration on the lock file")
}

// protectSaveBlob saves a single data blob containing content in its own
// WithBlobUploader session (so its pack is flushed to the backend
// immediately) and returns its blob ID. TASK-25 uses this instead of
// data.TestCreateSnapshot's probabilistic fakeFileSystem content overlap
// (see TestProtectIntegrationAppliesRetentionAndExcludesLocks's "only
// partially share packs" comment above): calling this once and then
// referencing the returned ID directly from multiple snapshots' trees (see
// protectSaveSnapshot) guarantees, rather than merely hopes, that a given
// pack is shared across exactly the snapshots the test intends.
func protectSaveBlob(ctx context.Context, t testing.TB, repo *repository.Repository, content []byte) restic.ID {
	t.Helper()
	var id restic.ID
	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		newID, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, content, restic.ID{}, false)
		id = newID
		return err
	}))
	return id
}

// protectSaveSnapshot builds and saves a snapshot at time at whose tree
// contains one file node per (name, blob IDs) entry in files, referencing
// existing blob IDs directly rather than re-uploading them - so a blob ID
// already returned by protectSaveBlob can be shared verbatim, byte-for-byte,
// pack-for-pack, across multiple snapshots' trees. All snapshots built this
// way share the same paths/hostname, so protectDefaultGroupBy groups them
// together for policy purposes.
func protectSaveSnapshot(ctx context.Context, t testing.TB, repo *repository.Repository, at time.Time, files map[string]restic.IDs) *data.Snapshot {
	t.Helper()

	var treeID restic.ID
	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		var nodes []*data.Node
		for name, blobs := range files {
			nodes = append(nodes, &data.Node{
				Name:    name,
				Type:    data.NodeTypeFile,
				Mode:    0644,
				Content: blobs,
			})
		}
		treeID = data.TestSaveNodes(t, ctx, uploader, nodes)
		return nil
	}))

	snapshot, err := data.NewSnapshot([]string{"protect-exact-test"}, []string{"test"}, "protect-exact-host", at)
	rtest.OK(t, err)
	snapshot.Tree = &treeID

	id, err := data.SaveSnapshot(ctx, repo, snapshot)
	rtest.OK(t, err)
	data.TestSetSnapshotID(t, snapshot, id)

	return snapshot
}

// TestProtectExactObjectSet is TASK-25's dedicated, thorough test: it
// directly reproduces BASE.md's motivating example - a pack created long
// before the policy window, but still needed by a currently-kept snapshot,
// must be protected based on current liveness, not object creation date -
// on a realistic multi-snapshot, deduplicated repository against a real
// local MinIO Object-Lock-enabled bucket.
//
// Three snapshots are engineered, via protectSaveBlob/protectSaveSnapshot's
// direct blob/tree construction (a deterministic guarantee, not a
// probabilistic hope), so that: all three reference one common data blob,
// saved exactly once - so its pack is created alongside the oldest snapshot
// and predates the other two, exactly like BASE.md's "pack created long
// ago" - and each snapshot also has one blob unique to itself. A
// "--keep-last 2"-equivalent policy discards the oldest snapshot and keeps
// the other two. After protect runs on the kept set:
//   - the shared blob's pack must be protected: it is "old" in exactly the
//     same sense as the discarded snapshot's own exclusive pack, but it is
//     still needed by the two kept snapshots - this is BASE.md's scenario,
//     verified.
//   - the discarded snapshot's own exclusive pack must NOT be protected.
//   - the two kept snapshots' own exclusive packs must be protected.
//   - config/keys/index must be protected; locks/* must not.
func TestProtectExactObjectSet(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := protectRandomCredentials(t)
	srv, cleanup := protectRunMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := protectNewMinioClient(t, srv)
	bucket := protectCreateLockedBucket(ctx, t, client)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	cfg := s3.NewConfig()
	cfg.Endpoint = srv.Endpoint
	cfg.Bucket = bucket
	cfg.Prefix = fmt.Sprintf("protect-exact-%d", time.Now().UnixNano())
	cfg.UseHTTP = true
	cfg.KeyID = srv.KeyID
	cfg.Secret = options.NewSecretString(srv.Secret)

	be, err := s3.Open(ctx, cfg, nil, nil)
	rtest.OK(t, err)

	repo, _ := repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})

	// One shared blob, saved once and referenced by ID (no re-upload) from
	// all three snapshots' trees below.
	sharedBlob := protectSaveBlob(ctx, t, repo, []byte("shared across all three snapshots"))
	oldestUnique := protectSaveBlob(ctx, t, repo, []byte("unique to the oldest, discarded snapshot"))
	middleUnique := protectSaveBlob(ctx, t, repo, []byte("unique to the middle, kept snapshot"))
	newestUnique := protectSaveBlob(ctx, t, repo, []byte("unique to the newest, kept snapshot"))

	oldest := protectSaveSnapshot(ctx, t, repo, time.Unix(1500000000, 0), map[string]restic.IDs{
		"shared": {sharedBlob},
		"unique": {oldestUnique},
	})
	middle := protectSaveSnapshot(ctx, t, repo, time.Unix(1550000000, 0), map[string]restic.IDs{
		"shared": {sharedBlob},
		"unique": {middleUnique},
	})
	newest := protectSaveSnapshot(ctx, t, repo, time.Unix(1600000000, 0), map[string]restic.IDs{
		"shared": {sharedBlob},
		"unique": {newestUnique},
	})

	printer := progress.NewNoopPrinter()

	// Record, before protecting, exactly which packs each individual
	// snapshot needs (TASK-19's function, called once per snapshot) so the
	// assertions below have an authoritative per-snapshot reference map.
	packsOf := func(sn *data.Snapshot) restic.IDSet {
		packs, err := repository.LivePacksForSnapshots(ctx, repo, restic.NewIDSet(*sn.ID()), printer)
		rtest.OK(t, err)
		return packs
	}
	oldestPacks := packsOf(oldest)
	middlePacks := packsOf(middle)
	newestPacks := packsOf(newest)

	// Sanity: the shared blob's pack really is needed by all three - if this
	// ever failed, the test below would be checking nothing interesting.
	sharedPackFound := false
	for id := range oldestPacks {
		if middlePacks.Has(id) && newestPacks.Has(id) {
			sharedPackFound = true
			break
		}
	}
	rtest.Assert(t, sharedPackFound, "expected a pack shared by all three snapshots")

	// A policy keeping only the last 2 (by time) of these three snapshots -
	// all in the same protectDefaultGroupBy group, since they share
	// hostname/paths - discards exactly the oldest and keeps the other two.
	selections, err := selectSnapshotsForPolicy(data.Snapshots{oldest, middle, newest}, protectDefaultGroupBy, data.ExpirePolicy{Last: 2})
	rtest.OK(t, err)
	rtest.Equals(t, 1, len(selections))
	rtest.Equals(t, 2, len(selections[0].Keep))
	rtest.Equals(t, 1, len(selections[0].Remove))
	rtest.Equals(t, *oldest.ID(), *selections[0].Remove[0].ID())

	retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	objects, err := runProtectApply(ctx, repo, printer, selections[0].Keep, retainUntil, false)
	rtest.OK(t, err)

	l := layout.NewDefaultLayout(cfg.Prefix, path.Join)
	assertRetained := func(h backend.Handle, want bool) {
		t.Helper()
		objKey := l.Filename(h)
		mode, gotRetainUntil, err := client.GetObjectRetention(ctx, bucket, objKey, "")
		if want {
			rtest.OK(t, err)
			rtest.Assert(t, mode != nil && *mode == minio.Compliance, "expected COMPLIANCE retention on %v, got %v", h, mode)
			rtest.Assert(t, gotRetainUntil != nil && gotRetainUntil.Truncate(time.Second).Equal(retainUntil),
				"expected retainUntil %v on %v, got %v", retainUntil, h, gotRetainUntil)
		} else {
			rtest.Assert(t, err != nil, "expected no retention configuration on %v", h)
		}
	}

	// Every pack needed by either kept snapshot must be protected - this
	// includes the shared pack, which predates the policy's --for window and
	// was created alongside the very snapshot the policy discards: proof
	// that protection tracks current liveness, not object creation date.
	for id := range middlePacks {
		assertRetained(backend.Handle{Type: restic.PackFile, Name: id.String()}, true)
	}
	for id := range newestPacks {
		assertRetained(backend.Handle{Type: restic.PackFile, Name: id.String()}, true)
	}

	// Any pack needed ONLY by the discarded (oldest) snapshot - not by
	// either kept snapshot - must NOT be protected, even though it is "old"
	// in exactly the same sense as the shared pack above.
	exclusiveToOldest := false
	for id := range oldestPacks {
		if middlePacks.Has(id) || newestPacks.Has(id) {
			continue
		}
		exclusiveToOldest = true
		assertRetained(backend.Handle{Type: restic.PackFile, Name: id.String()}, false)
	}
	rtest.Assert(t, exclusiveToOldest, "expected the oldest snapshot to need at least one pack neither kept snapshot needs")

	// config/keys/index protected.
	rtest.Equals(t, 1, len(objects.Config))
	rtest.Assert(t, len(objects.Keys) > 0, "expected at least one key handle")
	rtest.Assert(t, len(objects.Index) > 0, "expected at least one index handle")
	for _, h := range objects.Config {
		assertRetained(h, true)
	}
	for _, h := range objects.Keys {
		assertRetained(h, true)
	}
	for _, h := range objects.Index {
		assertRetained(h, true)
	}

	// locks/* never protected: create a real lock exactly as protect's own
	// append-lock would, and confirm it received no retention.
	unlock, _, err := repository.LockRepo(ctx, repo, false, 0, func(string) {}, func(string, ...interface{}) {})
	rtest.OK(t, err)
	defer unlock.Unlock()
	var lockKey string
	rtest.OK(t, repo.List(ctx, restic.LockFile, func(id restic.ID, _ int64) error {
		lockKey = l.Filename(backend.Handle{Type: restic.LockFile, Name: id.String()})
		return nil
	}))
	rtest.Assert(t, lockKey != "", "expected a lock file to exist")
	_, _, err = client.GetObjectRetention(ctx, bucket, lockKey, "")
	rtest.Assert(t, err != nil, "expected no retention configuration on the lock file")
}

// protectCreateUnlockedBucket creates a bucket WITHOUT Object Lock enabled -
// the mirror image of protectCreateLockedBucket - for TASK-24's "bucket
// exists but wasn't created with Object Lock enabled" pre-flight check test.
func protectCreateUnlockedBucket(ctx context.Context, t testing.TB, client *minio.Client) string {
	t.Helper()
	buf := make([]byte, 6)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		t.Fatal(err)
	}
	bucket := fmt.Sprintf("protect-unlocked-%s", hex.EncodeToString(buf))
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	return bucket
}

// TestProtectIntegrationPreflightFailsOnNonLockedBucket confirms TASK-24's
// pre-flight check end to end, through the actual `restic protect` command
// entry point (runProtect, not just runProtectApply): run against a real
// restic repository already initialized on a real, locally-run MinIO bucket
// that does NOT have Object Lock enabled, it must exit with TASK-6's clear
// error before any object gains retention.
//
// The repository is initialized directly via s3.Open with static test
// credentials, exactly as TestProtectIntegrationAppliesRetentionAndExcludesLocks
// above does - global.OpenRepository has no supported way to pass static S3
// credentials for a local MinIO server (s3.Config's KeyID/Secret fields are
// "for testing only", not settable through a repository location or `-o`
// option) - but runProtect itself is then driven the way the real CLI would
// be, through gopts, with credentials supplied via the AWS_* environment
// variables the s3 backend's credential chain reads (s3.getCredentials's
// EnvAWS provider), exactly as a real user would set them.
func TestProtectIntegrationPreflightFailsOnNonLockedBucket(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := protectRandomCredentials(t)
	srv, cleanup := protectRunMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := protectNewMinioClient(t, srv)
	bucket := protectCreateUnlockedBucket(ctx, t, client)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	prefix := fmt.Sprintf("protect-test-%d", time.Now().UnixNano())

	cfg := s3.NewConfig()
	cfg.Endpoint = srv.Endpoint
	cfg.Bucket = bucket
	cfg.Prefix = prefix
	cfg.UseHTTP = true
	cfg.KeyID = srv.KeyID
	cfg.Secret = options.NewSecretString(srv.Secret)

	be, err := s3.Open(ctx, cfg, nil, nil)
	rtest.OK(t, err)
	repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})

	t.Setenv("AWS_ACCESS_KEY_ID", srv.KeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", srv.Secret)

	var opts ProtectOptions
	opts.For = data.ParseDurationOrPanic("30d")

	gopts := global.Options{
		Repo:     fmt.Sprintf("s3:http://%s/%s/%s", srv.Endpoint, bucket, prefix),
		Password: rtest.TestPassword,
		CacheDir: filepath.Join(tempdir, "cache"),
		Extended: make(options.Options),
		Backends: all.Backends(),
	}

	err = withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runProtect(ctx, opts, gopts, gopts.Term)
	})
	rtest.Assert(t, err != nil, "expected an error against a bucket without Object Lock enabled")
	rtest.Assert(t, errors.Is(err, s3.ErrObjectLockNotEnabled),
		"expected the TASK-6 ObjectLockNotEnabledError, got %v", err)

	// Confirm no object anywhere under prefix gained retention: with Object
	// Lock never enabled on this bucket, GetObjectRetention must report "no
	// retention configuration" for every object protect would have touched
	// had it gotten past the pre-flight check - i.e. SetRetention was never
	// called.
	for objInfo := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		rtest.OK(t, objInfo.Err)
		_, _, err := client.GetObjectRetention(ctx, bucket, objInfo.Key, "")
		rtest.Assert(t, err != nil, "expected no retention configuration on %v, protect must not have called SetRetention", objInfo.Key)
	}
}

// TestProtectIntegrationPreflightFailsOnUnsupportedBackend confirms TASK-24's
// pre-flight check rejects, before doing anything else, a backend that
// doesn't implement backend.ObjectLocker at all - using a real local
// filesystem repository (internal/backend/local) as a convenient stand-in
// for "any non-s3 backend", exactly as this task's spec suggests.
//
// The default BackendTestHook withTestEnvironment sets up (see
// TestListOnce) wraps the backend in a test-only listOnceBackend that embeds
// backend.Backend as a plain interface field and so does not itself
// implement backend.Unwrapper; that would hide the real backend type from
// backendTypeName's Unwrap walk and this test's exact-message assertion
// below, so it is disabled here to exercise the real, unwrapped backend
// chain (retry/cache/... down to *local.Local) instead.
func TestProtectIntegrationPreflightFailsOnUnsupportedBackend(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env.gopts.BackendTestHook = nil

	testRunInit(t, env.gopts)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	var opts ProtectOptions
	opts.For = data.ParseDurationOrPanic("30d")

	err := withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		return runProtect(ctx, opts, gopts, gopts.Term)
	})
	rtest.Assert(t, err != nil, "expected an error against a backend without Object Lock support")
	rtest.Equals(t,
		"Fatal: the `protect` command requires a backend with S3 Object Lock support (currently only the `s3` backend); this repository uses the \"local\" backend, which does not support it",
		err.Error())
}

// TestProtectRetentionMonotonic is TASK-26's dedicated test: it proves the
// "repeated protect only extends retention, never shortens" guarantee holds
// across multiple real runProtectApply invocations against a real, locally
// run MinIO Object-Lock-enabled bucket - i.e. that TASK-15/22's
// server-enforced mechanism (SetRetention's attempt-and-classify
// PutObjectRetention call, relying on MinIO/S3 rejecting an attempt to
// shorten an active COMPLIANCE retention, per TASK-8's spike) actually holds
// end to end, not merely at protectRetainUntil's pure-function unit level.
// runProtectApply is the same "real protect command" wiring level
// TestProtectIntegrationAppliesRetentionAndExcludesLocks/
// TestProtectExactObjectSet above already treat as such (runProtect itself
// is a thin selection/reporting wrapper around it, see cmd_protect.go).
//
// One snapshot is protected three times in a row with different --for
// durations - 30d, then a SHORTER 10d, then a LONGER 90d - and every object
// protect touches is re-read after each run via the raw minio-go client
// (not the backend's own reporting): unchanged after the shorter run,
// advanced after the longer one.
func TestProtectRetentionMonotonic(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := protectRandomCredentials(t)
	srv, cleanup := protectRunMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := protectNewMinioClient(t, srv)
	bucket := protectCreateLockedBucket(ctx, t, client)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	cfg := s3.NewConfig()
	cfg.Endpoint = srv.Endpoint
	cfg.Bucket = bucket
	cfg.Prefix = fmt.Sprintf("protect-monotonic-%d", time.Now().UnixNano())
	cfg.UseHTTP = true
	cfg.KeyID = srv.KeyID
	cfg.Secret = options.NewSecretString(srv.Secret)

	be, err := s3.Open(ctx, cfg, nil, nil)
	rtest.OK(t, err)

	repo, _ := repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})

	blob := protectSaveBlob(ctx, t, repo, []byte("protect retention monotonicity fixture"))
	snap := protectSaveSnapshot(ctx, t, repo, time.Now(), map[string]restic.IDs{
		"file": {blob},
	})

	printer := progress.NewNoopPrinter()
	l := layout.NewDefaultLayout(cfg.Prefix, path.Join)

	// sample reads back, via the raw client, the current retainUntil date
	// for every object protect touched, keyed by its object key - a sample
	// covering config/keys/index/the snapshot/its packs, per this task's
	// acceptance criteria.
	sample := func(objects protectObjectSet) map[string]time.Time {
		got := make(map[string]time.Time)
		for _, h := range objects.all() {
			objKey := l.Filename(h)
			mode, retainUntil, err := client.GetObjectRetention(ctx, bucket, objKey, "")
			rtest.OK(t, err)
			rtest.Assert(t, mode != nil && *mode == minio.Compliance, "expected COMPLIANCE retention on %v, got %v", h, mode)
			rtest.Assert(t, retainUntil != nil, "expected a retainUntil date on %v", h)
			got[objKey] = retainUntil.Truncate(time.Second)
		}
		return got
	}

	// Step 1: protect once with --for 30d, record the resulting dates.
	retainUntil30 := protectRetainUntil(time.Now(), data.ParseDurationOrPanic("30d"))
	objects, err := runProtectApply(ctx, repo, printer, data.Snapshots{snap}, retainUntil30, false)
	rtest.OK(t, err)
	rtest.Assert(t, len(objects.all()) > 0, "expected at least one protected object")
	firstRun := sample(objects)
	for objKey, got := range firstRun {
		rtest.Assert(t, got.Equal(retainUntil30), "expected %v retained until %v, got %v", objKey, retainUntil30, got)
	}

	// Step 2: re-run protect with a SHORTER --for 10d. The server, not
	// client-side comparison logic, must reject the shortening attempt, so
	// every date must be UNCHANGED from step 1.
	retainUntil10 := protectRetainUntil(time.Now(), data.ParseDurationOrPanic("10d"))
	rtest.Assert(t, retainUntil10.Before(retainUntil30), "test invariant broken: --for 10d must compute an earlier date than --for 30d")
	objects2, err := runProtectApply(ctx, repo, printer, data.Snapshots{snap}, retainUntil10, false)
	rtest.OK(t, err)
	secondRun := sample(objects2)
	for objKey, got := range secondRun {
		want := firstRun[objKey]
		rtest.Assert(t, got.Equal(want), "expected retention on %v to remain unchanged at %v after a shorter --for run, got %v", objKey, want, got)
	}

	// Step 3: re-run protect with a LONGER --for 90d. This time retention
	// must genuinely advance to the new, later date.
	retainUntil90 := protectRetainUntil(time.Now(), data.ParseDurationOrPanic("90d"))
	rtest.Assert(t, retainUntil90.After(retainUntil30), "test invariant broken: --for 90d must compute a later date than --for 30d")
	objects3, err := runProtectApply(ctx, repo, printer, data.Snapshots{snap}, retainUntil90, false)
	rtest.OK(t, err)
	thirdRun := sample(objects3)
	for objKey, got := range thirdRun {
		rtest.Assert(t, got.Equal(retainUntil90), "expected retention on %v to advance to %v after a longer --for run, got %v", objKey, retainUntil90, got)
	}
}

// protectTestGopts builds the global.Options a `restic protect` integration
// test needs to reopen (via runProtect, not runProtectApply) the repository
// TestRepositoryWithBackend already initialized on the given real MinIO s3
// backend - the same construction TestProtectIntegrationPreflightFailsOnNonLockedBucket
// established.
func protectTestGopts(srv protectMinioServer, bucket, prefix, cacheDir string) global.Options {
	return global.Options{
		Repo:     fmt.Sprintf("s3:http://%s/%s/%s", srv.Endpoint, bucket, prefix),
		Password: rtest.TestPassword,
		CacheDir: cacheDir,
		Extended: make(options.Options),
		Backends: all.Backends(),
	}
}

// TestProtectIntegrationDryRunMakesNoWrites is TASK-27's dry-run acceptance
// test: with a real snapshot that a "--keep-last 1" policy would protect,
// running `restic protect --dry-run` against a real, locally-run MinIO
// Object-Lock-enabled bucket must make zero SetRetention calls - verified
// via the raw minio-go client, not the backend's own reporting, seeing no
// retention configuration on any object under the repository's prefix
// afterwards.
func TestProtectIntegrationDryRunMakesNoWrites(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := protectRandomCredentials(t)
	srv, cleanup := protectRunMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := protectNewMinioClient(t, srv)
	bucket := protectCreateLockedBucket(ctx, t, client)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	prefix := fmt.Sprintf("protect-dryrun-%d", time.Now().UnixNano())

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
	data.TestCreateSnapshot(t, repo, time.Now(), 2)

	t.Setenv("AWS_ACCESS_KEY_ID", srv.KeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", srv.Secret)

	gopts := protectTestGopts(srv, bucket, prefix, filepath.Join(tempdir, "cache"))

	var opts ProtectOptions
	opts.For = data.ParseDurationOrPanic("30d")
	opts.Last = 1
	opts.DryRun = true

	err = withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runProtect(ctx, opts, gopts, gopts.Term)
	})
	rtest.OK(t, err)

	sawObject := false
	for objInfo := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		rtest.OK(t, objInfo.Err)
		sawObject = true
		_, _, err := client.GetObjectRetention(ctx, bucket, objInfo.Key, "")
		rtest.Assert(t, err != nil, "expected no retention configuration on %v after a dry run, protect must not have called SetRetention", objInfo.Key)
	}
	rtest.Assert(t, sawObject, "expected at least one object in the bucket, otherwise this test proves nothing")
}

// TestProtectIntegrationJSONOutput confirms `restic protect --json` (a real,
// non-dry run) writes a single well-formed JSON object to stdout with, at
// minimum, the protected snapshot list, the protected pack count, and the
// computed retain_until date - per this task's acceptance criteria.
func TestProtectIntegrationJSONOutput(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := protectRandomCredentials(t)
	srv, cleanup := protectRunMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := protectNewMinioClient(t, srv)
	bucket := protectCreateLockedBucket(ctx, t, client)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	prefix := fmt.Sprintf("protect-json-%d", time.Now().UnixNano())

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
	sn := data.TestCreateSnapshot(t, repo, time.Now(), 2)

	t.Setenv("AWS_ACCESS_KEY_ID", srv.KeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", srv.Secret)

	gopts := protectTestGopts(srv, bucket, prefix, filepath.Join(tempdir, "cache"))
	gopts.JSON = true

	var opts ProtectOptions
	opts.For = data.ParseDurationOrPanic("30d")
	opts.Last = 1

	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runProtect(ctx, opts, gopts, gopts.Term)
	})
	rtest.OK(t, err)

	var result ProtectResult
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &result))

	rtest.Assert(t, !result.DryRun, "expected dry_run false for a real run")
	rtest.Equals(t, 1, len(result.Snapshots))
	rtest.Equals(t, sn.ID().String(), result.Snapshots[0].ID.String())
	rtest.Assert(t, result.Packs > 0, "expected at least one protected pack in the JSON output")
	rtest.Assert(t, result.RetainUntil.After(time.Now()), "expected retain_until in the JSON output to be in the future")
}

// TestProtectIntegrationJSONZeroSnapshots confirms `restic protect --json`
// against a repository with nothing currently kept by the policy produces a
// well-formed, empty-ish JSON object rather than an error - per this task's
// acceptance criteria.
func TestProtectIntegrationJSONZeroSnapshots(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := protectRandomCredentials(t)
	srv, cleanup := protectRunMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := protectNewMinioClient(t, srv)
	bucket := protectCreateLockedBucket(ctx, t, client)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	prefix := fmt.Sprintf("protect-json-empty-%d", time.Now().UnixNano())

	cfg := s3.NewConfig()
	cfg.Endpoint = srv.Endpoint
	cfg.Bucket = bucket
	cfg.Prefix = prefix
	cfg.UseHTTP = true
	cfg.KeyID = srv.KeyID
	cfg.Secret = options.NewSecretString(srv.Secret)

	be, err := s3.Open(ctx, cfg, nil, nil)
	rtest.OK(t, err)

	// No snapshots at all: repository is initialized, but nothing is ever
	// created on top of it.
	repository.TestRepositoryWithBackend(t, be, 2, repository.Options{})

	t.Setenv("AWS_ACCESS_KEY_ID", srv.KeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", srv.Secret)

	gopts := protectTestGopts(srv, bucket, prefix, filepath.Join(tempdir, "cache"))
	gopts.JSON = true

	var opts ProtectOptions
	opts.For = data.ParseDurationOrPanic("30d")
	opts.Last = 1

	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runProtect(ctx, opts, gopts, gopts.Term)
	})
	rtest.OK(t, err)

	var result ProtectResult
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &result))

	rtest.Equals(t, 0, len(result.Snapshots))
	rtest.Equals(t, 0, result.Packs)
	rtest.Assert(t, !result.RetainUntil.IsZero(), "expected retain_until to still be computed even with nothing to protect")
}

// TestProtectIntegrationDryRunJSONCombo confirms "--dry-run --json" compose:
// the emitted JSON is well-formed, marked "dry_run": true, and still reports
// what WOULD have been protected, while genuinely making zero SetRetention
// calls (verified via the raw minio-go client) - per this task's acceptance
// criteria.
func TestProtectIntegrationDryRunJSONCombo(t *testing.T) {
	if _, err := exec.LookPath("minio"); err != nil {
		t.Skip(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := protectRandomCredentials(t)
	srv, cleanup := protectRunMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := protectNewMinioClient(t, srv)
	bucket := protectCreateLockedBucket(ctx, t, client)

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	prefix := fmt.Sprintf("protect-dryrun-json-%d", time.Now().UnixNano())

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
	sn := data.TestCreateSnapshot(t, repo, time.Now(), 2)

	t.Setenv("AWS_ACCESS_KEY_ID", srv.KeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", srv.Secret)

	gopts := protectTestGopts(srv, bucket, prefix, filepath.Join(tempdir, "cache"))
	gopts.JSON = true

	var opts ProtectOptions
	opts.For = data.ParseDurationOrPanic("30d")
	opts.Last = 1
	opts.DryRun = true

	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runProtect(ctx, opts, gopts, gopts.Term)
	})
	rtest.OK(t, err)

	var result ProtectResult
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &result))

	rtest.Assert(t, result.DryRun, "expected dry_run true in the JSON output")
	rtest.Equals(t, 1, len(result.Snapshots))
	rtest.Equals(t, sn.ID().String(), result.Snapshots[0].ID.String())
	rtest.Assert(t, result.Packs > 0, "expected the would-be protected pack count to still be reported")

	for objInfo := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		rtest.OK(t, objInfo.Err)
		_, _, err := client.GetObjectRetention(ctx, bucket, objInfo.Key, "")
		rtest.Assert(t, err != nil, "expected no retention configuration on %v after a dry run, protect must not have called SetRetention", objInfo.Key)
	}
}
