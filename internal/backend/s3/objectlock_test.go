package s3_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/s3"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/options"
	rtest "github.com/restic/restic/internal/test"
)

// minioServer holds the connection details of a local minio server started
// by runObjectLockMinio.
type minioServer struct {
	Endpoint string
	KeyID    string
	Secret   string
}

// runObjectLockMinio starts a local `minio server` process rooted at a fresh
// temporary directory below dir, using the given access key ID and secret,
// and waits until the server's TCP port is reachable. It allocates a free
// local port for each invocation, so this suite's MinIO instance can run
// concurrently with the one s3_test.go's runMinio starts on its hard-coded
// port. It returns the resulting minioServer (endpoint, access key, secret)
// so callers can construct an s3/minio-go client against it, together with a
// cleanup function that terminates the server process. Callers are
// responsible for invoking the returned cleanup function (e.g. via
// t.Cleanup or defer).
//
// This is a deliberate near-duplicate of s3_test.go's runMinio, kept
// separate so the Object Lock suite's need for a second, concurrently
// running MinIO instance doesn't change shared test infra other S3 tests
// depend on.
func runObjectLockMinio(ctx context.Context, t testing.TB, dir, key, secret string) (minioServer, func()) {
	mkdir(t, filepath.Join(dir, "config"))
	mkdir(t, filepath.Join(dir, "root"))

	addr := freeLocalAddr(t)

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

	err := cmd.Start()
	if err != nil {
		t.Fatal(err)
	}

	// wait until the TCP port is reachable
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
		return minioServer{}, nil
	}

	cleanup := func() {
		err = cmd.Process.Kill()
		if err != nil {
			t.Fatal(err)
		}

		// ignore errors, we've killed the process
		_ = cmd.Wait()
	}

	return minioServer{Endpoint: addr, KeyID: key, Secret: secret}, cleanup
}

// freeLocalAddr returns a local "host:port" address with a currently-unused
// TCP port, suitable for passing to `minio server --address`. The port is
// freed again before returning, so there is a (small, accepted-in-tests) race
// until the caller binds it.
func freeLocalAddr(t testing.TB) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// newMinioClient builds a minio-go client for the given local minio server,
// so Object Lock spike/fixture tests do not each repeat client construction.
func newMinioClient(t testing.TB, srv minioServer) *minio.Client {
	client, err := minio.New(srv.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(srv.KeyID, srv.Secret, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// randomBucketName returns a bucket name suffixed with a random hex string,
// so concurrent/sequential test runs against the same minio instance do not
// collide.
func randomBucketName(t testing.TB, prefix string) string {
	buf := make([]byte, 6)
	_, err := io.ReadFull(rand.Reader, buf)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(buf))
}

// createLockedBucket creates a bucket with Object Lock enabled at creation
// time and returns its name.
func createLockedBucket(ctx context.Context, t testing.TB, client *minio.Client) string {
	bucket := randomBucketName(t, "locked")
	err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{ObjectLocking: true})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

// createPlainBucket creates a bucket without Object Lock enabled, against
// the same minio instance/client as createLockedBucket, and returns its
// name.
func createPlainBucket(ctx context.Context, t testing.TB, client *minio.Client) string {
	bucket := randomBucketName(t, "plain")
	err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

// openObjectLocker opens an s3 backend against srv/bucket (which must
// already exist -- s3.Open never creates a bucket) and returns it through
// the backend.ObjectLocker interface, so TASK-14's tests can exercise
// IsObjectLockEnabled without reaching into the unexported s3 struct.
func openObjectLocker(ctx context.Context, t testing.TB, srv minioServer, bucket string) backend.ObjectLocker {
	cfg := s3.NewConfig()
	cfg.Endpoint = srv.Endpoint
	cfg.Bucket = bucket
	cfg.Prefix = fmt.Sprintf("test-%d", time.Now().UnixNano())
	cfg.UseHTTP = true
	cfg.KeyID = srv.KeyID
	cfg.Secret = options.NewSecretString(srv.Secret)

	be, err := s3.Open(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("s3.Open failed: %v", err)
	}

	locker, ok := be.(backend.ObjectLocker)
	if !ok {
		t.Fatal("s3 backend does not implement backend.ObjectLocker")
	}
	return locker
}

// lockableBackend is satisfied by the s3 backend: the full backend.Backend
// surface (so TestSetRetention can Save an object before locking it) plus
// backend.ObjectLocker (so it can exercise SetRetention). Declared here,
// rather than reused from openObjectLocker's narrower return type, because
// this black-box (s3_test) package cannot embed both onto *s3 itself.
type lockableBackend interface {
	backend.Backend
	backend.ObjectLocker
}

// openLockableBackend is openObjectLocker with two additions TestSetRetention
// needs and the IsObjectLockEnabled tests above do not: a caller-chosen,
// fixed prefix (rather than a random one), so the test can independently
// reconstruct the exact S3 object key SetRetention acts on, and an optional
// custom RoundTripper, so it can count the underlying PutObjectRetention
// calls.
func openLockableBackend(ctx context.Context, t testing.TB, srv minioServer, bucket, prefix string, rt http.RoundTripper) lockableBackend {
	cfg := s3.NewConfig()
	cfg.Endpoint = srv.Endpoint
	cfg.Bucket = bucket
	cfg.Prefix = prefix
	cfg.UseHTTP = true
	cfg.KeyID = srv.KeyID
	cfg.Secret = options.NewSecretString(srv.Secret)

	be, err := s3.Open(ctx, cfg, rt, nil)
	if err != nil {
		t.Fatalf("s3.Open failed: %v", err)
	}

	lockable, ok := be.(lockableBackend)
	if !ok {
		t.Fatal("s3 backend does not implement backend.Backend and backend.ObjectLocker")
	}
	return lockable
}

// objectKeyFor mirrors internal/backend/layout.DefaultLayout.Filename's
// key-construction logic (path.Join(prefix, <type dir>, name)) closely
// enough for this black-box test to locate the exact S3 object key a given
// handle maps to, for verification via the raw minio-go client -- duplicated
// here rather than imported, matching this file's existing
// objectLockDisabledCodeFromBucketCheck convention. Handles used by
// TestSetRetention are never PackFile handles, so the pack-specific two
// character subdirectory splitting is intentionally not reproduced here.
func objectKeyFor(prefix string, h backend.Handle) string {
	dirs := map[backend.FileType]string{
		backend.PackFile:     "data",
		backend.SnapshotFile: "snapshots",
		backend.IndexFile:    "index",
		backend.LockFile:     "locks",
		backend.KeyFile:      "keys",
	}
	return path.Join(prefix, dirs[h.Type], h.Name)
}

// putObjectRetentionCounter is an http.RoundTripper wrapper that counts PUT
// requests carrying the "retention" query parameter -- the exact request
// shape minio-go's PutObjectRetention issues (see
// api-object-retention.go's PutObjectRetention) -- so TestSetRetention can
// assert SetRetention makes exactly one such call per invocation, with no
// pre-read (no GetObjectRetention call) in any of the first-time-set,
// extend, or shorten-rejected-as-no-op cases.
type putObjectRetentionCounter struct {
	count int
}

func (c *putObjectRetentionCounter) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPut && req.URL.Query().Has("retention") {
		c.count++
	}
	return http.DefaultTransport.RoundTrip(req)
}

// TestIsObjectLockEnabled exercises TASK-14's IsObjectLockEnabled on the
// real s3 backend against the TASK-3 fixtures, covering the three cases the
// acceptance criteria call out: feature flag disabled, flag enabled against
// a lock-enabled bucket, and flag enabled against a plain bucket.
func TestIsObjectLockEnabled(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)

	t.Run("flag disabled", func(t *testing.T) {
		// Deliberately do not enable feature.ObjectLock: the flag defaults to
		// disabled (see internal/feature/registry_test.go), so no
		// TestSetFlag call is needed here to prove that.
		locker := openObjectLocker(ctx, t, srv, createLockedBucket(ctx, t, client))

		enabled, err := locker.IsObjectLockEnabled(ctx)
		if err == nil {
			t.Fatal("expected an error when the object-lock feature flag is disabled")
		}
		if enabled {
			t.Fatal("expected enabled == false alongside the flag-disabled error")
		}
		if !strings.Contains(err.Error(), "object-lock") {
			t.Fatalf("expected the error to name the `object-lock` feature flag, got %q", err.Error())
		}
	})

	t.Run("flag enabled, lock-enabled bucket", func(t *testing.T) {
		defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

		locker := openObjectLocker(ctx, t, srv, createLockedBucket(ctx, t, client))

		enabled, err := locker.IsObjectLockEnabled(ctx)
		if err != nil {
			t.Fatalf("expected no error for a lock-enabled bucket, got %v", err)
		}
		if !enabled {
			t.Fatal("expected enabled == true for a lock-enabled bucket")
		}
	})

	t.Run("flag enabled, plain bucket", func(t *testing.T) {
		defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

		bucket := createPlainBucket(ctx, t, client)
		locker := openObjectLocker(ctx, t, srv, bucket)

		enabled, err := locker.IsObjectLockEnabled(ctx)
		if err == nil {
			t.Fatal("expected an error for a plain (non-Object-Lock) bucket")
		}
		if enabled {
			t.Fatal("expected enabled == false alongside the not-enabled error")
		}
		if !errors.Is(err, s3.ErrObjectLockNotEnabled) {
			t.Fatalf("expected errors.Is(err, s3.ErrObjectLockNotEnabled) to hold, got %v", err)
		}
		if !strings.Contains(err.Error(), bucket) {
			t.Fatalf("expected the error to name the bucket %q, got %q", bucket, err.Error())
		}
	})
}

// TestObjectLockBucketFixtures is the baseline for the Object Lock spike
// tests (TASK-4 onward): it confirms bucket creation itself succeeds for
// both the lock-enabled and lock-disabled fixtures, so later spikes don't
// misattribute a bucket-creation failure to lock-detection behavior.
func TestObjectLockBucketFixtures(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)

	lockedBucket := createLockedBucket(ctx, t, client)
	if lockedBucket == "" {
		t.Fatal("expected a non-empty locked bucket name")
	}

	plainBucket := createPlainBucket(ctx, t, client)
	if plainBucket == "" {
		t.Fatal("expected a non-empty plain bucket name")
	}

	if lockedBucket == plainBucket {
		t.Fatalf("expected unique bucket names, got %q for both", lockedBucket)
	}

	for _, bucket := range []string{lockedBucket, plainBucket} {
		exists, err := client.BucketExists(ctx, bucket)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("bucket %q was not created", bucket)
		}
	}
}

// TestObjectLockEnabledDetection proves that GetBucketObjectLockConfig
// reliably confirms Object Lock is active on a bucket created with
// ObjectLocking: true (the TASK-3 lock-enabled fixture), and documents the
// exact shape it returns for such a bucket.
func TestObjectLockEnabledDetection(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)

	mode, validity, unit, err := client.GetBucketObjectLockConfig(ctx, bucket)
	if err != nil {
		t.Fatalf("GetBucketObjectLockConfig returned an error for a lock-enabled bucket: %v", err)
	}

	t.Logf("GetBucketObjectLockConfig(%q): mode=%v validity=%v unit=%v", bucket, mode, validity, unit)

	// Documented, real-MinIO behavior (not assumed from docs) for a bucket
	// created with ObjectLocking: true and no default retention configured:
	// GetBucketObjectLockConfig succeeds (err == nil) -- that alone is what
	// distinguishes "Object Lock enabled" from "Object Lock disabled" (see
	// TASK-5, where the same call against a plain bucket returns an error).
	// mode/validity/unit stay nil because no *default* bucket retention rule
	// was set at creation time; a nil triple here does NOT mean Object Lock
	// is off, it means no default retention is configured. TASK-6's
	// detection logic must key off `err`, not off mode/validity/unit being
	// non-nil.
	if mode != nil {
		t.Fatalf("expected nil mode (no default retention configured), got %v", *mode)
	}
	if validity != nil {
		t.Fatalf("expected nil validity (no default retention configured), got %v", *validity)
	}
	if unit != nil {
		t.Fatalf("expected nil unit (no default retention configured), got %v", *unit)
	}
}

// TestObjectLockDisabledDetection proves that GetBucketObjectLockConfig
// returns a non-nil error with a specific, documented error code for a
// bucket that was never created with Object Lock enabled (the TASK-3 plain
// bucket fixture), and that this code differs from the one returned for a
// bucket that does not exist at all -- so TASK-6's detection logic can tell
// "not enabled" apart from other failure modes.
func TestObjectLockDisabledDetection(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createPlainBucket(ctx, t, client)

	_, _, _, err = client.GetBucketObjectLockConfig(ctx, bucket)
	if err == nil {
		t.Fatal("expected a non-nil error for a bucket created without Object Lock enabled")
	}

	errResp := minio.ToErrorResponse(err)
	t.Logf("GetBucketObjectLockConfig(%q) [plain bucket]: code=%q message=%q", bucket, errResp.Code, errResp.Message)

	// Documented, real-MinIO behavior (not assumed from docs): a bucket
	// created without ObjectLocking: true responds to
	// GetBucketObjectLockConfig with this specific error code. TASK-6's
	// detection logic keys off this code, not the error message text, since
	// the message can vary across S3-compatible providers while the code is
	// the stable, documented contract.
	const wantDisabledCode = "ObjectLockConfigurationNotFoundError"
	if errResp.Code != wantDisabledCode {
		t.Fatalf("expected error code %q for a lock-disabled bucket, got %q", wantDisabledCode, errResp.Code)
	}

	// Distinguish from a bucket that doesn't exist at all, so the two
	// failure modes are never conflated by the caller.
	missingBucket := randomBucketName(t, "missing")
	_, _, _, err = client.GetBucketObjectLockConfig(ctx, missingBucket)
	if err == nil {
		t.Fatal("expected a non-nil error for a bucket that does not exist")
	}

	missingErrResp := minio.ToErrorResponse(err)
	t.Logf("GetBucketObjectLockConfig(%q) [nonexistent bucket]: code=%q message=%q", missingBucket, missingErrResp.Code, missingErrResp.Message)

	if missingErrResp.Code == wantDisabledCode {
		t.Fatalf("expected a different error code for a nonexistent bucket than %q, got the same", wantDisabledCode)
	}
	const wantMissingCode = "NoSuchBucket"
	if missingErrResp.Code != wantMissingCode {
		t.Fatalf("expected error code %q for a nonexistent bucket, got %q", wantMissingCode, missingErrResp.Code)
	}
}

// TestCheckObjectLockEnabled exercises s3.CheckObjectLockEnabled's three
// documented outcomes -- enabled, not enabled, and any other error -- against
// the real local MinIO fixtures from TASK-3/TASK-4/TASK-5, so callers
// (TASK-14, TASK-24) can rely on the chosen (bool, error) shape without ever
// seeing a raw MinIO error code or XML dump.
func TestCheckObjectLockEnabled(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)

	t.Run("enabled", func(t *testing.T) {
		bucket := createLockedBucket(ctx, t, client)

		enabled, err := s3.CheckObjectLockEnabled(ctx, client, bucket)
		if err != nil {
			t.Fatalf("expected no error for a lock-enabled bucket, got %v", err)
		}
		if !enabled {
			t.Fatal("expected enabled == true for a lock-enabled bucket")
		}
	})

	t.Run("not enabled", func(t *testing.T) {
		bucket := createPlainBucket(ctx, t, client)

		enabled, err := s3.CheckObjectLockEnabled(ctx, client, bucket)
		if err != nil {
			t.Fatalf("expected no error for a plain bucket, got %v", err)
		}
		if enabled {
			t.Fatal("expected enabled == false for a plain bucket")
		}
	})

	t.Run("other error", func(t *testing.T) {
		bucket := randomBucketName(t, "missing")

		enabled, err := s3.CheckObjectLockEnabled(ctx, client, bucket)
		if err == nil {
			t.Fatal("expected an error for a bucket that does not exist")
		}
		if enabled {
			t.Fatal("expected enabled == false alongside a non-nil error")
		}

		// The "other error" case must be distinguishable from "not enabled":
		// it carries a non-nil error, and must not claim to be the
		// not-enabled sentinel.
		if errors.Is(err, s3.ErrObjectLockNotEnabled) {
			t.Fatalf("error for a nonexistent bucket must not match ErrObjectLockNotEnabled, got %v", err)
		}

		// The underlying cause must still be reachable for debugging via
		// errors.As, since CheckObjectLockEnabled wraps it with %w.
		var errResp minio.ErrorResponse
		if !errors.As(err, &errResp) {
			t.Fatalf("expected the wrapped error to preserve the underlying minio.ErrorResponse, got %v", err)
		}
		if errResp.Code != "NoSuchBucket" {
			t.Fatalf("expected the wrapped error to preserve the underlying MinIO error, got code %q from %v", errResp.Code, err)
		}
	})
}

// TestObjectLockNotEnabledError checks that the error returned for the
// "not enabled" case is a clear, single-sentence, restic-style message that
// names the bucket and explains Object Lock must be enabled at
// bucket-creation time -- never a raw MinIO error code or XML dump -- and
// that it is recognizable programmatically via errors.Is.
func TestObjectLockNotEnabledError(t *testing.T) {
	err := s3.ObjectLockNotEnabledError("my-bucket")

	if err == nil {
		t.Fatal("expected a non-nil error")
	}

	if !errors.Is(err, s3.ErrObjectLockNotEnabled) {
		t.Fatalf("expected errors.Is(err, s3.ErrObjectLockNotEnabled) to hold, got %v", err)
	}

	msg := err.Error()
	if !strings.Contains(msg, "my-bucket") {
		t.Fatalf("expected the error message to name the bucket, got %q", msg)
	}
	if !strings.Contains(msg, "created") {
		t.Fatalf("expected the error message to explain Object Lock is set at bucket-creation time, got %q", msg)
	}

	// Never dump raw MinIO error codes/XML at the user.
	for _, forbidden := range []string{"ObjectLockConfigurationNotFoundError", "<?xml", "<Error>"} {
		if strings.Contains(msg, forbidden) {
			t.Fatalf("expected the error message not to contain raw MinIO output %q, got %q", forbidden, msg)
		}
	}
}

// TestObjectRetentionOnUnlockedBucket covers the second, reactive reading of
// "identify the error when trying to activate it" (TASK-7): rather than only
// passively checking the bucket's config (TASK-5's
// TestObjectLockDisabledDetection), this uploads an object to the TASK-3
// plain-bucket fixture and actively attempts PutObjectRetention on it, the
// call a naive protect implementation could hit directly if the TASK-6
// pre-flight check were ever skipped or raced.
func TestObjectRetentionOnUnlockedBucket(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createPlainBucket(ctx, t, client)

	const object = "retention-test-object"
	_, err = client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject into the plain bucket fixture failed: %v", err)
	}

	future := time.Now().Add(24 * time.Hour)
	compliance := minio.Compliance
	err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &future,
		Mode:            &compliance,
	})
	if err == nil {
		t.Fatal("expected a non-nil error setting retention on an object in a bucket without Object Lock enabled")
	}

	errResp := minio.ToErrorResponse(err)
	t.Logf("PutObjectRetention(%q, %q) [plain bucket]: code=%q message=%q", bucket, object, errResp.Code, errResp.Message)

	// Documented, real-MinIO behavior (not assumed from docs): attempting to
	// set object-level retention against a bucket that was never created with
	// Object Lock enabled fails with this code and the message "Bucket is
	// missing ObjectLockConfiguration" -- unrelated to the
	// ObjectLockConfigurationNotFoundError code TASK-5 observed from
	// GetBucketObjectLockConfig against the identical fixture. So the two
	// call sites (bucket-config read vs. object-retention write) surface
	// different codes for what is conceptually the same root cause: TASK-6's
	// CheckObjectLockEnabled classification (keyed on
	// ObjectLockConfigurationNotFoundError) does NOT cover this reactive
	// path, and a caller that reaches PutObjectRetention directly (bypassing
	// the pre-flight check) needs its own classification if it ever wants to
	// turn this into the same user-facing ObjectLockNotEnabledError -- it
	// cannot reuse TASK-6's code string as-is.
	const wantCode = "InvalidRequest"
	if errResp.Code != wantCode {
		t.Fatalf("expected error code %q for PutObjectRetention against a lock-disabled bucket, got %q", wantCode, errResp.Code)
	}
	if errResp.Code == objectLockDisabledCodeFromBucketCheck {
		t.Fatalf("expected the object-level retention error code to differ from the bucket-level %q code, got the same", objectLockDisabledCodeFromBucketCheck)
	}
}

// TestObjectRetentionGovernanceBlocksDelete proves GOVERNANCE mode, like
// COMPLIANCE (TestObjectRetentionBlocksDelete), rejects a normal
// version-specific RemoveObject while retention is active: a GOVERNANCE lock
// is not simply "advisory" against an ordinary delete call, it requires the
// same explicit bypass opt-in as documented for AWS S3. This is the
// TASK-10 counterpart to TASK-9's COMPLIANCE spike, validated for the PRD's
// "both modes tested with equal weight" product decision (section 8).
func TestObjectRetentionGovernanceBlocksDelete(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)

	const object = "governance-blocks-delete-object"
	info, err := client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject into the lock-enabled bucket fixture failed: %v", err)
	}
	if info.VersionID == "" {
		t.Fatal("expected a non-empty VersionID from PutObject against a versioned (lock-enabled) bucket")
	}

	governance := minio.Governance
	retainUntil := time.Now().Add(24 * time.Hour)
	err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &retainUntil,
		Mode:            &governance,
		VersionID:       info.VersionID,
	})
	if err != nil {
		t.Fatalf("PutObjectRetention (GOVERNANCE) on a lock-enabled bucket failed: %v", err)
	}

	// A normal, non-bypass delete of the locked version must fail, same as
	// the COMPLIANCE case in TestObjectRetentionBlocksDelete.
	deleteErr := client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{VersionID: info.VersionID})
	if deleteErr == nil {
		t.Fatal("expected RemoveObject to fail against a GOVERNANCE-locked object version with active retention and no bypass")
	}

	errResp := minio.ToErrorResponse(deleteErr)
	t.Logf("RemoveObject(%q, %q, version=%q) [GOVERNANCE-locked, no bypass]: code=%q message=%q", bucket, object, info.VersionID, errResp.Code, errResp.Message)

	const wantLockedCode = "InvalidRequest"
	if errResp.Code != wantLockedCode {
		t.Fatalf("expected error code %q for RemoveObject against a GOVERNANCE-locked object version, got %q", wantLockedCode, errResp.Code)
	}

	_, err = client.StatObject(ctx, bucket, object, minio.StatObjectOptions{VersionID: info.VersionID})
	if err != nil {
		t.Fatalf("expected the object version to still exist after a failed locked-delete, StatObject failed: %v", err)
	}
}

// TestObjectRetentionGovernanceBypass documents the MinIO/S3 admin bypass
// primitive for GOVERNANCE mode: RemoveObjectOptions{GovernanceBypass: true}
// succeeds against an object that TestObjectRetentionGovernanceBlocksDelete
// just proved a normal delete cannot touch.
//
// SCOPE BOUNDARY (binding, per .agent/prd/PRD.md section 8 and section 10
// "Governance bypass is out of scope for command-level exposure", and the
// Non-goals list in section 3): this test validates a MinIO/S3 client-library
// primitive ONLY, for future-story documentation purposes. It is explicitly
// NOT wired into any restic command (protect/forget/prune) in this phase --
// no flag, no CLI surface, no caller in cmd/restic or internal/backend/s3's
// production code exercises GovernanceBypass anywhere outside this test. A
// future "admin early deletion" story would need its own deliberate,
// separately-reviewed design (credential/authorization model in particular)
// before this primitive is ever exposed to a restic user.
func TestObjectRetentionGovernanceBypass(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)

	const object = "governance-bypass-object"
	info, err := client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject into the lock-enabled bucket fixture failed: %v", err)
	}
	if info.VersionID == "" {
		t.Fatal("expected a non-empty VersionID from PutObject against a versioned (lock-enabled) bucket")
	}

	governance := minio.Governance
	retainUntil := time.Now().Add(24 * time.Hour)
	err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &retainUntil,
		Mode:            &governance,
		VersionID:       info.VersionID,
	})
	if err != nil {
		t.Fatalf("PutObjectRetention (GOVERNANCE) on a lock-enabled bucket failed: %v", err)
	}

	// Sanity check: confirm the normal path is still blocked before proving
	// the bypass path works, so a passing bypass assertion can't be masked by
	// e.g. retention having failed to apply.
	deleteErr := client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{VersionID: info.VersionID})
	if deleteErr == nil {
		t.Fatal("expected a non-bypass RemoveObject to fail against a GOVERNANCE-locked object version")
	}

	// The bypass path: RemoveObjectOptions{GovernanceBypass: true} deletes
	// the still-locked version successfully.
	bypassErr := client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{
		VersionID:        info.VersionID,
		GovernanceBypass: true,
	})
	if bypassErr != nil {
		t.Fatalf("expected RemoveObject with GovernanceBypass: true to succeed against a GOVERNANCE-locked object version, got error: %v", bypassErr)
	}

	_, err = client.StatObject(ctx, bucket, object, minio.StatObjectOptions{VersionID: info.VersionID})
	if err == nil {
		t.Fatal("expected StatObject to fail after the object version was successfully deleted via GovernanceBypass")
	}
}

// TestLockDetectionStrategy confirms and exercises the delete-and-classify
// lock-detection strategy (TASK-11), the decision binding for TASK-13/
// TASK-15/TASK-16: forget/prune's --skip-object-locked path attempts the
// delete it already needs to make and classifies the result, instead of
// pre-checking every candidate with GetObjectRetention before deleting.
//
// CALL-COUNT ARGUMENT (binding, the reason delete-and-classify is chosen):
// on a healthy repository the overwhelming majority of forget/prune
// candidates are NOT locked -- only the small, recently-`protect`ed subset
// is. Compare the two strategies per candidate:
//
//   - pre-check (GetObjectRetention, then RemoveObject if unlocked): a flat
//     2 S3 calls per candidate, REGARDLESS of outcome -- the common,
//     unlocked case pays for a lookup it never needed.
//   - delete-and-classify (RemoveObject, then GetObjectRetention only if
//     that failed and was classified as a lock error): 1 call for the
//     common unlocked case (delete succeeds, done); 2 calls for the rare
//     locked case (delete fails, then one lookup -- for user-facing
//     reporting only, not for the skip decision itself, which the
//     classified delete failure already answered).
//
// So delete-and-classify never costs more than pre-check, and costs strictly
// less in the common case, which is why it is chosen over pre-checking.
//
// ACCEPTED TRADE-OFF: because the lazy lookup happens strictly after the
// failed delete, there is a small race window where retention could expire
// between the two calls, and the reported retainUntil could theoretically
// come back nil/expired for an object that was still locked a moment
// earlier. Retention windows for this feature are days to months, so this
// race is immaterial in practice. This is an accepted trade-off of the
// chosen strategy, not a defect to fix.
func TestLockDetectionStrategy(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)

	// Step 1: the unlocked candidate costs exactly 1 call -- the delete
	// itself succeeds and nothing else is ever called in this branch. This
	// is asserted by construction: the code below for this subtest simply
	// never calls GetObjectRetention, which is the entire point of the
	// strategy for the common case.
	t.Run("unlocked candidate costs one call", func(t *testing.T) {
		const object = "strategy-unlocked-object"
		info, err := client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
		if err != nil {
			t.Fatalf("PutObject into the lock-enabled bucket fixture failed: %v", err)
		}
		if info.VersionID == "" {
			t.Fatal("expected a non-empty VersionID from PutObject against a versioned (lock-enabled) bucket")
		}

		// No retention was ever set on this object/version: it is the
		// unlocked case. Call #1 (and the only call): RemoveObject.
		err = client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{VersionID: info.VersionID})
		if err != nil {
			t.Fatalf("expected RemoveObject to succeed against an unlocked object version, got error: %v", err)
		}

		_, err = client.StatObject(ctx, bucket, object, minio.StatObjectOptions{VersionID: info.VersionID})
		if err == nil {
			t.Fatal("expected StatObject to fail after the unlocked object version was deleted")
		}
	})

	// Step 2: the locked candidate costs 2 calls -- the failed delete
	// (which is also what classifies it as locked, per TASK-9's documented
	// error code), then, only after that classification, a lazy
	// GetObjectRetention lookup used solely to report the retainUntil date
	// to the user.
	t.Run("locked candidate classifies then looks up lazily", func(t *testing.T) {
		const object = "strategy-locked-object"
		info, err := client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
		if err != nil {
			t.Fatalf("PutObject into the lock-enabled bucket fixture failed: %v", err)
		}
		if info.VersionID == "" {
			t.Fatal("expected a non-empty VersionID from PutObject against a versioned (lock-enabled) bucket")
		}

		compliance := minio.Compliance
		retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)
		err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
			RetainUntilDate: &retainUntil,
			Mode:            &compliance,
			VersionID:       info.VersionID,
		})
		if err != nil {
			t.Fatalf("PutObjectRetention on a lock-enabled bucket failed: %v", err)
		}

		// Call #1: attempt the delete the code already needs to make. This
		// is both the skip decision AND the lock-detection signal -- no
		// separate pre-check call is used to decide anything here.
		deleteErr := client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{VersionID: info.VersionID})
		if deleteErr == nil {
			t.Fatal("expected RemoveObject to fail against a COMPLIANCE-locked object version with active retention")
		}

		// Classify using the exact error code TASK-9 documented
		// (TestObjectRetentionBlocksDelete): "InvalidRequest".
		errResp := minio.ToErrorResponse(deleteErr)
		t.Logf("RemoveObject(%q, %q, version=%q) [locked]: code=%q message=%q", bucket, object, info.VersionID, errResp.Code, errResp.Message)
		const wantLockedCode = "InvalidRequest"
		if errResp.Code != wantLockedCode {
			t.Fatalf("expected error code %q classifying a locked-delete failure, got %q", wantLockedCode, errResp.Code)
		}

		// Call #2: only now, after classification confirmed a lock, look up
		// the retention -- lazily, and only for reporting the date to the
		// user, not for the skip decision (already made above).
		mode, gotRetainUntil, err := client.GetObjectRetention(ctx, bucket, object, info.VersionID)
		if err != nil {
			t.Fatalf("lazy GetObjectRetention after a classified locked-delete failure returned an error: %v", err)
		}
		if mode == nil || *mode != minio.Compliance {
			t.Fatalf("expected mode %v from the lazy lookup, got %v", minio.Compliance, mode)
		}
		if gotRetainUntil == nil {
			t.Fatal("expected a non-nil retainUntilDate from the lazy lookup")
		}
		if !gotRetainUntil.Truncate(time.Second).Equal(retainUntil) {
			t.Fatalf("expected the lazy lookup to report retainUntilDate %v, got %v", retainUntil, gotRetainUntil)
		}

		// The object version must still be present: the classified delete
		// never actually removed it.
		_, err = client.StatObject(ctx, bucket, object, minio.StatObjectOptions{VersionID: info.VersionID})
		if err != nil {
			t.Fatalf("expected the locked object version to still exist after the classified delete failure, StatObject failed: %v", err)
		}
	})
}

// objectLockDisabledCodeFromBucketCheck mirrors the unexported
// objectLockNotEnabledCode constant in objectlock.go (TASK-6), duplicated
// here rather than imported so this black-box test documents the comparison
// value inline without reaching into the package's internals.
const objectLockDisabledCodeFromBucketCheck = "ObjectLockConfigurationNotFoundError"

// TestObjectRetentionRoundTrip proves the basic write/read retention
// primitive works end-to-end against the TASK-3 lock-enabled bucket fixture:
// upload an object, set a COMPLIANCE retention with a specific
// retainUntilDate, read it back, and confirm the values match.
func TestObjectRetentionRoundTrip(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)

	const object = "round-trip-object"
	_, err = client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject into the lock-enabled bucket fixture failed: %v", err)
	}

	// Retention dates are second-precision on S3/MinIO; truncate before
	// comparing to avoid flaky sub-second mismatches.
	retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	compliance := minio.Compliance
	err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &retainUntil,
		Mode:            &compliance,
	})
	if err != nil {
		t.Fatalf("PutObjectRetention on a lock-enabled bucket failed: %v", err)
	}

	mode, gotRetainUntil, err := client.GetObjectRetention(ctx, bucket, object, "")
	if err != nil {
		t.Fatalf("GetObjectRetention failed: %v", err)
	}
	if mode == nil || *mode != minio.Compliance {
		t.Fatalf("expected mode %v, got %v", minio.Compliance, mode)
	}
	if gotRetainUntil == nil {
		t.Fatal("expected a non-nil retainUntilDate")
	}
	if !gotRetainUntil.Truncate(time.Second).Equal(retainUntil) {
		t.Fatalf("expected retainUntilDate %v, got %v", retainUntil, gotRetainUntil)
	}
}

// TestObjectRetentionShorten sets a COMPLIANCE retention, then attempts to
// shorten it, and records exactly what MinIO does: reject with a specific
// error code, silently ignore it (retention unchanged), or actually shorten
// it. This is the write-side counterpart to TASK-11's delete-and-classify
// read-side decision: TASK-15/TASK-22 need this answer before they can rely
// on "attempt the target date, treat rejection as a benign no-op" instead of
// reading each object's current retention before every write.
//
// FINDING (binding for TASK-15/TASK-22): MinIO DOES reject an attempt to
// shorten an active COMPLIANCE retention, matching AWS S3's documented
// behavior. The observed error code is "InvalidRequest" (message: "Object is
// WORM protected and cannot be overwritten"), confirmed against a real local
// MinIO server below, not assumed from docs. GetObjectRetention afterward
// shows the original (longer) date is unchanged. This means the
// write-and-classify strategy (attempt PutObjectRetention with the target
// date directly, treat this specific rejection as a no-op) IS safe for
// TASK-15/TASK-22 to use -- no pre-read is required before every write.
func TestObjectRetentionShorten(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)

	const object = "shorten-object"
	_, err = client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject into the lock-enabled bucket fixture failed: %v", err)
	}

	compliance := minio.Compliance
	original := time.Now().Add(48 * time.Hour).Truncate(time.Second)
	err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &original,
		Mode:            &compliance,
	})
	if err != nil {
		t.Fatalf("initial PutObjectRetention failed: %v", err)
	}

	earlier := original.Add(-24 * time.Hour)
	shortenErr := client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &earlier,
		Mode:            &compliance,
	})

	errResp := minio.ToErrorResponse(shortenErr)
	t.Logf("PutObjectRetention shorten attempt: err=%v code=%q message=%q", shortenErr, errResp.Code, errResp.Message)

	_, afterShorten, err := client.GetObjectRetention(ctx, bucket, object, "")
	if err != nil {
		t.Fatalf("GetObjectRetention after the shorten attempt failed: %v", err)
	}
	if afterShorten == nil {
		t.Fatal("expected a non-nil retainUntilDate after the shorten attempt")
	}

	if shortenErr == nil {
		// No error at all: either silently ignored (date unchanged) or
		// actually shortened. Either way this is a MinIO behavior gap from
		// documented AWS S3 semantics and must be flagged loudly, since it
		// breaks the write-and-classify design TASK-15/TASK-22 rely on.
		if afterShorten.Truncate(time.Second).Equal(earlier) {
			t.Fatal("MinIO BEHAVIOR GAP: PutObjectRetention silently SHORTENED an active " +
				"COMPLIANCE retention with no error -- this contradicts documented AWS S3 " +
				"behavior and means TASK-15/TASK-22 CANNOT use write-and-classify; they must " +
				"read the current retention before every write instead")
		}
		t.Logf("MinIO silently ignored the shorten attempt (no error, date unchanged at %v) -- "+
			"still safe for write-and-classify since the retention was not weakened, but the "+
			"expected AWS-matching behavior is a loud rejection, not silence", afterShorten)
	} else {
		// Rejected with an error, matching documented AWS S3 COMPLIANCE
		// behavior. Confirm the stored date truly did not change.
		if !afterShorten.Truncate(time.Second).Equal(original) {
			t.Fatalf("expected retention to remain at %v after a rejected shorten attempt, got %v", original, afterShorten)
		}
	}
}

// TestObjectRetentionExtend confirms that setting retention to a LATER date
// than the current one succeeds normally -- the everyday "extend" case
// `restic protect` relies on -- proving the shorten case (TestObjectRetentionShorten)
// is specifically what gets rejected/no-op'd, not writes in general.
func TestObjectRetentionExtend(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)

	const object = "extend-object"
	_, err = client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject into the lock-enabled bucket fixture failed: %v", err)
	}

	compliance := minio.Compliance
	original := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &original,
		Mode:            &compliance,
	})
	if err != nil {
		t.Fatalf("initial PutObjectRetention failed: %v", err)
	}

	later := original.Add(24 * time.Hour)
	err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &later,
		Mode:            &compliance,
	})
	if err != nil {
		t.Fatalf("expected extending retention to a later date to succeed, got error: %v", err)
	}

	_, gotRetainUntil, err := client.GetObjectRetention(ctx, bucket, object, "")
	if err != nil {
		t.Fatalf("GetObjectRetention after the extend attempt failed: %v", err)
	}
	if gotRetainUntil == nil {
		t.Fatal("expected a non-nil retainUntilDate after extending retention")
	}
	if !gotRetainUntil.Truncate(time.Second).Equal(later) {
		t.Fatalf("expected retainUntilDate to advance to %v after extending, got %v", later, gotRetainUntil)
	}
}

// TestSetRetention exercises TASK-15's s3.SetRetention (backend.ObjectLocker)
// against the TASK-3 lock-enabled bucket fixture, proving the
// attempt-and-classify write design TASK-8's finding (see
// TestObjectRetentionShorten) licensed: every case -- first-time set,
// extension, and a rejected shorten -- makes exactly one PutObjectRetention
// call, with no GetObjectRetention pre-read, and a rejected shorten is
// reported to the caller as success (a benign no-op), not an error.
func TestSetRetention(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)
	const prefix = "set-retention-test"

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	counter := &putObjectRetentionCounter{}
	be := openLockableBackend(ctx, t, srv, bucket, prefix, counter)

	saveObject := func(t *testing.T, name string) backend.Handle {
		h := backend.Handle{Type: backend.IndexFile, Name: name}
		if err := be.Save(ctx, h, backend.NewByteReader([]byte("object lock spike payload"), nil)); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
		return h
	}

	getRetention := func(t *testing.T, h backend.Handle) *time.Time {
		key := objectKeyFor(prefix, h)
		_, retainUntil, err := client.GetObjectRetention(ctx, bucket, key, "")
		if err != nil {
			t.Fatalf("GetObjectRetention(%q) failed: %v", key, err)
		}
		return retainUntil
	}

	t.Run("first-time set", func(t *testing.T) {
		h := saveObject(t, "first-time-set-object")

		counter.count = 0
		retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)
		if err := be.SetRetention(ctx, h, retainUntil); err != nil {
			t.Fatalf("SetRetention failed: %v", err)
		}
		if counter.count != 1 {
			t.Fatalf("expected exactly one PutObjectRetention call, got %d", counter.count)
		}

		got := getRetention(t, h)
		if got == nil || !got.Truncate(time.Second).Equal(retainUntil) {
			t.Fatalf("expected retainUntil %v, got %v", retainUntil, got)
		}
	})

	t.Run("extend advances retention with one call", func(t *testing.T) {
		h := saveObject(t, "extend-object")

		counter.count = 0
		original := time.Now().Add(24 * time.Hour).Truncate(time.Second)
		if err := be.SetRetention(ctx, h, original); err != nil {
			t.Fatalf("initial SetRetention failed: %v", err)
		}
		if counter.count != 1 {
			t.Fatalf("expected exactly one PutObjectRetention call for the initial set, got %d", counter.count)
		}

		counter.count = 0
		later := original.Add(24 * time.Hour)
		if err := be.SetRetention(ctx, h, later); err != nil {
			t.Fatalf("extending SetRetention failed: %v", err)
		}
		if counter.count != 1 {
			t.Fatalf("expected exactly one PutObjectRetention call for the extension, got %d", counter.count)
		}

		got := getRetention(t, h)
		if got == nil || !got.Truncate(time.Second).Equal(later) {
			t.Fatalf("expected retention to advance to %v, got %v", later, got)
		}
	})

	t.Run("shorten is a no-op, not an error", func(t *testing.T) {
		h := saveObject(t, "shorten-object")

		counter.count = 0
		original := time.Now().Add(48 * time.Hour).Truncate(time.Second)
		if err := be.SetRetention(ctx, h, original); err != nil {
			t.Fatalf("initial SetRetention failed: %v", err)
		}
		if counter.count != 1 {
			t.Fatalf("expected exactly one PutObjectRetention call for the initial set, got %d", counter.count)
		}

		counter.count = 0
		earlier := original.Add(-24 * time.Hour)
		if err := be.SetRetention(ctx, h, earlier); err != nil {
			t.Fatalf("expected a shortening SetRetention call to be treated as a benign no-op, got error: %v", err)
		}
		if counter.count != 1 {
			t.Fatalf("expected exactly one PutObjectRetention call for the rejected shorten attempt, got %d", counter.count)
		}

		got := getRetention(t, h)
		if got == nil || !got.Truncate(time.Second).Equal(original) {
			t.Fatalf("expected retention to remain unchanged at %v after a shorten attempt, got %v", original, got)
		}
	})

	t.Run("flag disabled", func(t *testing.T) {
		defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, false)()

		h := saveObject(t, "flag-disabled-object")

		counter.count = 0
		err := be.SetRetention(ctx, h, time.Now().Add(24*time.Hour))
		if err == nil {
			t.Fatal("expected an error when the object-lock feature flag is disabled")
		}
		if !strings.Contains(err.Error(), "object-lock") {
			t.Fatalf("expected the error to name the `object-lock` feature flag, got %q", err.Error())
		}
		if counter.count != 0 {
			t.Fatalf("expected no PutObjectRetention call when the feature flag is disabled, got %d", counter.count)
		}
	})
}

// TestObjectRetentionBlocksDelete proves the actual enforcement half of
// Object Lock (TASK-8 only proved the retention metadata round-trips): a
// COMPLIANCE-locked object version must reject RemoveObject while retention
// is active, must remain fully present after the rejected attempt, and must
// become deletable again once retainUntilDate has passed. This gives
// TASK-11's delete-and-classify decision and TASK-16's Remove/ErrObjectLocked
// wrapping a real, observed error code to match against instead of one
// assumed from AWS documentation.
//
// FINDING #1 (binding for TASK-11/TASK-16): a real local MinIO server DOES
// accept a retainUntilDate only a few seconds in the future -- unlike AWS
// S3's documented 1-day minimum retention *period* for COMPLIANCE mode, MinIO
// does not enforce a minimum granularity on retainUntilDate itself, so this
// test observes real-time expiry directly rather than substituting a
// GetObjectRetention-only check.
//
// FINDING #2 (critical, binding for TASK-11/TASK-16, see also
// TestUnversionedDeleteBypassesLock below): the delete this test asserts
// fails is a *version-specific* RemoveObject (VersionOptions.VersionID set
// to the locked version). Object Lock requires bucket versioning -- MinIO
// auto-enables it for a bucket created with ObjectLocking: true (verified:
// GetBucketVersioning reports Status "Enabled" on the TASK-3 locked-bucket
// fixture with no explicit versioning call ever made) -- and on a versioned
// bucket, RemoveObject WITHOUT a VersionID does not touch the locked version
// at all: it succeeds unconditionally by writing a new delete marker on top,
// which only *hides* the object from unversioned reads/lists. The literal
// call restic's current s3 backend makes today (client.RemoveObject(ctx,
// bucket, obj, minio.RemoveObjectOptions{}), no VersionID) would therefore
// NOT observe a locked-delete error at all against a real Object-Lock bucket.
// TASK-16 needs the object's VersionID at delete time (e.g. from a preceding
// Stat/Put) for the "attempt delete, classify the error" strategy to ever see
// this error path in practice.
func TestObjectRetentionBlocksDelete(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)

	const object = "blocks-delete-object"
	info, err := client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject into the lock-enabled bucket fixture failed: %v", err)
	}
	if info.VersionID == "" {
		t.Fatal("expected a non-empty VersionID from PutObject against a versioned (lock-enabled) bucket")
	}

	// Keep this short: long enough to reliably assert the delete fails
	// before it expires, short enough to keep the test fast.
	const retentionWindow = 3 * time.Second
	compliance := minio.Compliance
	retainUntil := time.Now().Add(retentionWindow)
	err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &retainUntil,
		Mode:            &compliance,
		VersionID:       info.VersionID,
	})
	if err != nil {
		t.Fatalf("PutObjectRetention on a lock-enabled bucket failed: %v", err)
	}

	// Step 2: attempt deletion of the locked version while still locked, and
	// assert failure. See FINDING #2: this must target the version
	// explicitly, or the bucket's (mandatory, for Object Lock) versioning
	// turns the delete into a no-error delete-marker write instead.
	deleteErr := client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{VersionID: info.VersionID})
	if deleteErr == nil {
		t.Fatal("expected RemoveObject to fail against a COMPLIANCE-locked object version with active retention")
	}

	errResp := minio.ToErrorResponse(deleteErr)
	t.Logf("RemoveObject(%q, %q, version=%q) [locked, active retention]: code=%q message=%q", bucket, object, info.VersionID, errResp.Code, errResp.Message)

	// Documented, real-MinIO behavior (not assumed from docs): deleting a
	// COMPLIANCE-locked object version under active retention fails with this
	// code and message -- the same "InvalidRequest" / "Object is WORM
	// protected and cannot be overwritten" pair TestObjectRetentionShorten
	// observed for a rejected retention-shortening PutObjectRetention. Both
	// call sites hit MinIO's same WORM guard. TASK-16's Remove/ErrObjectLocked
	// wrapping keys off this code, not the message text, matching the pattern
	// established in TASK-6/TASK-7.
	const wantLockedCode = "InvalidRequest"
	if errResp.Code != wantLockedCode {
		t.Fatalf("expected error code %q for RemoveObject against a COMPLIANCE-locked object version, got %q", wantLockedCode, errResp.Code)
	}

	// Step 3: confirm the object version still exists after the failed
	// delete.
	_, err = client.StatObject(ctx, bucket, object, minio.StatObjectOptions{VersionID: info.VersionID})
	if err != nil {
		t.Fatalf("expected the object version to still exist after a failed locked-delete, StatObject failed: %v", err)
	}

	// Step 4: wait out the retention window, then confirm deletion succeeds.
	time.Sleep(retentionWindow + time.Second)

	err = client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{VersionID: info.VersionID})
	if err != nil {
		t.Fatalf("expected RemoveObject to succeed once retention had expired, got error: %v", err)
	}

	_, err = client.StatObject(ctx, bucket, object, minio.StatObjectOptions{VersionID: info.VersionID})
	if err == nil {
		t.Fatal("expected StatObject to fail after the object version was successfully deleted post-expiry")
	}
}

// TestUnversionedDeleteBypassesLock documents FINDING #2 from
// TestObjectRetentionBlocksDelete in isolation: it proves that the exact,
// version-less RemoveObject call restic's s3 backend makes today (see
// internal/backend/s3/s3.go's Remove) does NOT fail against a
// COMPLIANCE-locked object, even while retention is fully active. This is
// not a bug in this spike -- it is mandatory S3/MinIO versioned-bucket
// semantics (Object Lock requires versioning) -- but it is a real,
// load-bearing constraint TASK-11/TASK-16 must design around, not assume
// away: an unversioned Remove() alone can never be the signal that detects a
// locked object.
func TestUnversionedDeleteBypassesLock(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)

	const object = "unversioned-delete-object"
	info, err := client.PutObject(ctx, bucket, object, strings.NewReader("object lock spike payload"), -1, minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject into the lock-enabled bucket fixture failed: %v", err)
	}

	// A long-lived retention: this test never waits for expiry, it only
	// proves the version-less delete is a no-op with respect to the lock
	// while retention is unquestionably active.
	compliance := minio.Compliance
	retainUntil := time.Now().Add(24 * time.Hour)
	err = client.PutObjectRetention(ctx, bucket, object, minio.PutObjectRetentionOptions{
		RetainUntilDate: &retainUntil,
		Mode:            &compliance,
		VersionID:       info.VersionID,
	})
	if err != nil {
		t.Fatalf("PutObjectRetention on a lock-enabled bucket failed: %v", err)
	}

	// The version-less delete restic's current Remove() issues: it succeeds
	// with no error, because on a versioned bucket it writes a delete marker
	// instead of touching the locked version.
	err = client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{})
	if err != nil {
		t.Fatalf("expected the version-less RemoveObject to succeed (delete-marker write) even against a locked object, got error: %v", err)
	}

	// From an unversioned caller's point of view (exactly what restic uses
	// today), the object now looks deleted.
	_, err = client.StatObject(ctx, bucket, object, minio.StatObjectOptions{})
	if err == nil {
		t.Fatal("expected unversioned StatObject to report the object as gone after the delete-marker write")
	}

	// But the locked version is still physically present and still WORM
	// protected: GetObjectRetention on that specific version still succeeds
	// and still reports the original retention, proving no data was actually
	// destroyed.
	mode, gotRetainUntil, err := client.GetObjectRetention(ctx, bucket, object, info.VersionID)
	if err != nil {
		t.Fatalf("expected the locked version's retention to still be readable after the version-less delete, got error: %v", err)
	}
	if mode == nil || *mode != minio.Compliance {
		t.Fatalf("expected the locked version to still report COMPLIANCE mode, got %v", mode)
	}
	if gotRetainUntil == nil || !gotRetainUntil.Truncate(time.Second).Equal(retainUntil.Truncate(time.Second)) {
		t.Fatalf("expected the locked version's retainUntilDate to be unchanged at %v, got %v", retainUntil, gotRetainUntil)
	}

	// And a version-specific delete against that same version still fails,
	// exactly as TestObjectRetentionBlocksDelete observed -- the delete
	// marker did not weaken the lock.
	versionDeleteErr := client.RemoveObject(ctx, bucket, object, minio.RemoveObjectOptions{VersionID: info.VersionID})
	if versionDeleteErr == nil {
		t.Fatal("expected a version-specific delete of the locked version to still fail after the version-less delete-marker write")
	}
	errResp := minio.ToErrorResponse(versionDeleteErr)
	t.Logf("RemoveObject(%q, %q, version=%q) after delete-marker write: code=%q message=%q", bucket, object, info.VersionID, errResp.Code, errResp.Message)
	const wantLockedCode = "InvalidRequest"
	if errResp.Code != wantLockedCode {
		t.Fatalf("expected error code %q, got %q", wantLockedCode, errResp.Code)
	}
}

// TestRemoveObjectLockAware exercises TASK-16's Remove/ErrObjectLocked
// wrapping end to end, through the s3 backend's public Remove method
// (rather than the raw minio-go client, as the earlier spikes above do),
// against a real local MinIO server: a locked object's delete fails and is
// classified with backend.ErrObjectLocked; an unlocked object's delete
// succeeds exactly as before, with the feature flag on or off; and deleting
// a nonexistent object remains a normal no-op.
func TestRemoveObjectLockAware(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)
	const prefix = "remove-object-lock-test"

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	be := openLockableBackend(ctx, t, srv, bucket, prefix, nil)

	saveObject := func(t *testing.T, name string) backend.Handle {
		h := backend.Handle{Type: backend.IndexFile, Name: name}
		if err := be.Save(ctx, h, backend.NewByteReader([]byte("object lock remove-test payload"), nil)); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
		return h
	}

	t.Run("locked object: Remove fails and is classified", func(t *testing.T) {
		h := saveObject(t, "locked-remove-object")
		objKey := objectKeyFor(prefix, h)

		compliance := minio.Compliance
		retainUntil := time.Now().Add(24 * time.Hour)
		if err := client.PutObjectRetention(ctx, bucket, objKey, minio.PutObjectRetentionOptions{
			RetainUntilDate: &retainUntil,
			Mode:            &compliance,
		}); err != nil {
			t.Fatalf("PutObjectRetention failed: %v", err)
		}

		err := be.Remove(ctx, h)
		if err == nil {
			t.Fatal("expected Remove to fail against a locked object")
		}
		if !errors.Is(err, backend.ErrObjectLocked) {
			t.Fatalf("expected errors.Is(err, backend.ErrObjectLocked) to hold, got %v", err)
		}

		// Confirm the object is genuinely still present, not merely hidden
		// behind a delete marker (see TestUnversionedDeleteBypassesLock).
		if _, err := client.StatObject(ctx, bucket, objKey, minio.StatObjectOptions{}); err != nil {
			t.Fatalf("expected the locked object to still exist after the failed Remove, StatObject failed: %v", err)
		}
	})

	t.Run("unlocked object: Remove succeeds, flag enabled", func(t *testing.T) {
		h := saveObject(t, "unlocked-remove-object-flag-on")
		objKey := objectKeyFor(prefix, h)

		if err := be.Remove(ctx, h); err != nil {
			t.Fatalf("expected Remove to succeed against an unlocked object, got: %v", err)
		}
		if _, err := client.StatObject(ctx, bucket, objKey, minio.StatObjectOptions{}); err == nil {
			t.Fatal("expected the object to be gone after Remove succeeded")
		}
	})

	t.Run("unlocked object: Remove succeeds, flag disabled", func(t *testing.T) {
		defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, false)()

		h := saveObject(t, "unlocked-remove-object-flag-off")
		objKey := objectKeyFor(prefix, h)

		if err := be.Remove(ctx, h); err != nil {
			t.Fatalf("expected Remove to succeed against an unlocked object with the flag disabled, got: %v", err)
		}
		if _, err := client.StatObject(ctx, bucket, objKey, minio.StatObjectOptions{}); err == nil {
			t.Fatal("expected the object to be gone after Remove succeeded")
		}
	})

	t.Run("nonexistent object: Remove is a normal no-op, unchanged", func(t *testing.T) {
		h := backend.Handle{Type: backend.IndexFile, Name: "never-existed-remove-object"}
		if err := be.Remove(ctx, h); err != nil {
			t.Fatalf("expected Remove on a nonexistent object to succeed as a no-op, got: %v", err)
		}
	})
}

// TestRetainedUntil exercises TASK-16's ObjectLocker.RetainedUntil against a
// real local MinIO server: a locked object reports the correct retainUntil
// date with locked=true; an object with no retention ever set, and one with
// an expired retention, both report locked=false with no error; and the
// feature flag gates the method exactly like SetRetention/IsObjectLockEnabled.
func TestRetainedUntil(t *testing.T) {
	// try to find a minio binary
	_, err := exec.LookPath("minio")
	if err != nil {
		t.Skip(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempdir := rtest.TempDir(t)
	key, secret := newRandomCredentials(t)
	srv, cleanup := runObjectLockMinio(ctx, t, tempdir, key, secret)
	defer cleanup()

	client := newMinioClient(t, srv)
	bucket := createLockedBucket(ctx, t, client)
	const prefix = "retained-until-test"

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	be := openLockableBackend(ctx, t, srv, bucket, prefix, nil)

	saveObject := func(t *testing.T, name string) backend.Handle {
		h := backend.Handle{Type: backend.IndexFile, Name: name}
		if err := be.Save(ctx, h, backend.NewByteReader([]byte("retained-until payload"), nil)); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
		return h
	}

	t.Run("locked object reports the correct date", func(t *testing.T) {
		h := saveObject(t, "locked-object")
		objKey := objectKeyFor(prefix, h)

		compliance := minio.Compliance
		want := time.Now().Add(24 * time.Hour).Truncate(time.Second)
		if err := client.PutObjectRetention(ctx, bucket, objKey, minio.PutObjectRetentionOptions{
			RetainUntilDate: &want,
			Mode:            &compliance,
		}); err != nil {
			t.Fatalf("PutObjectRetention failed: %v", err)
		}

		got, locked, err := be.RetainedUntil(ctx, h)
		if err != nil {
			t.Fatalf("RetainedUntil failed: %v", err)
		}
		if !locked {
			t.Fatal("expected locked=true for an object under active retention")
		}
		if !got.Truncate(time.Second).Equal(want) {
			t.Fatalf("expected retainUntil %v, got %v", want, got)
		}
	})

	t.Run("object with no retention reports locked=false", func(t *testing.T) {
		h := saveObject(t, "no-retention-object")

		got, locked, err := be.RetainedUntil(ctx, h)
		if err != nil {
			t.Fatalf("RetainedUntil failed: %v", err)
		}
		if locked {
			t.Fatalf("expected locked=false, got true with retainUntil %v", got)
		}
	})

	t.Run("object with expired retention reports locked=false", func(t *testing.T) {
		h := saveObject(t, "expired-retention-object")
		objKey := objectKeyFor(prefix, h)

		compliance := minio.Compliance
		retainUntil := time.Now().Add(3 * time.Second)
		if err := client.PutObjectRetention(ctx, bucket, objKey, minio.PutObjectRetentionOptions{
			RetainUntilDate: &retainUntil,
			Mode:            &compliance,
		}); err != nil {
			t.Fatalf("PutObjectRetention failed: %v", err)
		}

		time.Sleep(4 * time.Second)

		got, locked, err := be.RetainedUntil(ctx, h)
		if err != nil {
			t.Fatalf("RetainedUntil failed: %v", err)
		}
		if locked {
			t.Fatalf("expected locked=false for expired retention, got true with retainUntil %v", got)
		}
	})

	t.Run("flag disabled returns an error naming the flag", func(t *testing.T) {
		defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, false)()

		h := saveObject(t, "flag-disabled-object")
		_, _, err := be.RetainedUntil(ctx, h)
		if err == nil {
			t.Fatal("expected an error when the object-lock feature flag is disabled")
		}
		if !strings.Contains(err.Error(), "object-lock") {
			t.Fatalf("expected the error to name the `object-lock` feature flag, got %q", err.Error())
		}
	})
}

// TestIsPermanentErrorClassifiesObjectLocked guards against a locked-delete
// failure being retried by internal/backend/retry: without this
// classification, a caller going through the retry-wrapped backend (as
// forget/prune's --skip-object-locked do in normal operation) would see the
// same locked-delete error retried for the backend's full MaxElapsedTime
// (minutes) on every single locked object, instead of getting it back
// immediately to classify -- retention windows run days to years, so no
// backoff could ever turn the retry into a success. s3.Open does no network
// I/O, so this is a pure unit test and needs no local MinIO server.
func TestIsPermanentErrorClassifiesObjectLocked(t *testing.T) {
	cfg := s3.NewConfig()
	cfg.Endpoint = "127.0.0.1:1"
	cfg.Bucket = "is-permanent-error-test"
	cfg.UseHTTP = true
	cfg.KeyID = "key"
	cfg.Secret = options.NewSecretString("secret")

	be, err := s3.Open(context.Background(), cfg, nil, nil)
	if err != nil {
		t.Fatalf("s3.Open failed: %v", err)
	}

	lockedErr := fmt.Errorf("%w: simulated locked-delete failure", backend.ErrObjectLocked)
	if !be.IsPermanentError(lockedErr) {
		t.Fatal("expected IsPermanentError to classify a wrapped backend.ErrObjectLocked as permanent")
	}

	if be.IsPermanentError(errors.New("some unrelated transient error")) {
		t.Fatal("expected IsPermanentError to not misclassify an unrelated error as permanent")
	}
}
