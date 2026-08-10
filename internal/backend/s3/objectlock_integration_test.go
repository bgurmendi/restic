package s3_test

// TASK-17: a consolidated integration test suite that exercises the full
// ObjectLocker implementation (TASK-14/15/16) plus Remove's ErrObjectLocked
// wrapping as a whole, through a real restic s3 backend instance (via
// s3.Open, the same helpers TASK-14/15/16's per-method tests already use),
// rather than a raw minio.Client or a bare *s3 struct. The per-method tests
// in objectlock_test.go already cover each piece in isolation; this file's
// job is to catch any gap between the raw MinIO primitives those tests
// documented and how restic's backend wraps them end to end: config parsing,
// Layout/Filename key construction, feature flag plumbing, and error
// wrapping, all exercised together on one object across its whole lifecycle.

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/feature"
	rtest "github.com/restic/restic/internal/test"
)

// TestObjectLockerFullLifecycleCompliance drives every ObjectLocker method
// plus Remove, in the order a real `restic protect` / `forget
// --skip-object-locked` run would: confirm the bucket is lock-enabled, save
// a file, extend its retention, fail to delete it while locked (classified
// via errors.Is), confirm the reported retention date, then wait out the
// window and confirm the same file deletes normally once it expires. This is
// the one test in the suite that strings the whole COMPLIANCE lifecycle
// together on a single object; the per-method tests in objectlock_test.go
// each cover a slice of this in isolation, with separate fixture objects.
func TestObjectLockerFullLifecycleCompliance(t *testing.T) {
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
	const prefix = "full-lifecycle-compliance-test"

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	be := openLockableBackend(ctx, t, srv, bucket, prefix, nil)

	// Step 1: the bucket-lock check succeeds through the backend's public
	// interface, same as TestIsObjectLockEnabled but as the first step of
	// this end-to-end flow rather than standalone.
	enabled, err := be.IsObjectLockEnabled(ctx)
	if err != nil {
		t.Fatalf("IsObjectLockEnabled failed against a lock-enabled bucket: %v", err)
	}
	if !enabled {
		t.Fatal("expected IsObjectLockEnabled to report true for a lock-enabled bucket")
	}

	// Step 2: save a real file through the backend's public Save, exactly as
	// restic's writer path would.
	h := backend.Handle{Type: backend.IndexFile, Name: "full-lifecycle-object"}
	if err := be.Save(ctx, h, backend.NewByteReader([]byte("full lifecycle payload"), nil)); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Step 3: SetRetention on that file's handle, attempt-and-classify per
	// TASK-15. Keep the window short: long enough to reliably observe the
	// blocked delete below before it expires, short enough to keep the test
	// fast once it waits the window out in step 6.
	const retentionWindow = 3 * time.Second
	retainUntil := time.Now().Add(retentionWindow)
	if err := be.SetRetention(ctx, h, retainUntil); err != nil {
		t.Fatalf("SetRetention failed: %v", err)
	}

	// Step 4: a real Remove call on that file fails, classified via the
	// sentinel -- no separate lock-check call, the classification rides on
	// this same failed delete (TASK-11's delete-and-classify decision).
	if err := be.Remove(ctx, h); err == nil {
		t.Fatal("expected Remove to fail against a file under active COMPLIANCE retention")
	} else if !errors.Is(err, backend.ErrObjectLocked) {
		t.Fatalf("expected errors.Is(err, backend.ErrObjectLocked) to hold, got %v", err)
	}

	// Step 5: RetainedUntil, the lazy reporting-only lookup, reports the
	// correct date.
	gotRetainUntil, locked, err := be.RetainedUntil(ctx, h)
	if err != nil {
		t.Fatalf("RetainedUntil failed: %v", err)
	}
	if !locked {
		t.Fatal("expected locked == true immediately after a classified locked-delete failure")
	}
	if !gotRetainUntil.Truncate(time.Second).Equal(retainUntil.Truncate(time.Second)) {
		t.Fatalf("expected RetainedUntil to report %v, got %v", retainUntil, gotRetainUntil)
	}

	// Step 6: once the window has passed, the identical Remove call on the
	// identical handle now succeeds -- proving the earlier failure really was
	// the active lock, not some unrelated, permanent problem with this file.
	time.Sleep(retentionWindow + time.Second)

	if err := be.Remove(ctx, h); err != nil {
		t.Fatalf("expected Remove to succeed once retention had expired, got: %v", err)
	}
}

// TestObjectLockerGovernanceReadPath is the GOVERNANCE-mode counterpart to
// TestObjectLockerFullLifecycleCompliance, restricted to the read paths
// (RetainedUntil, Remove's error wrapping): SetRetention is hardcoded to
// COMPLIANCE mode (TASK-15), so the GOVERNANCE retention here is set
// directly via the raw minio-go client, exactly as the acceptance criteria
// prescribe. It confirms Remove/RetainedUntil are mode-agnostic -- they
// classify on the S3 error code / retention metadata, never on the mode
// itself -- by exercising them through the real backend against an object
// locked in the mode the backend itself can never write.
func TestObjectLockerGovernanceReadPath(t *testing.T) {
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
	const prefix = "governance-read-path-test"

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	be := openLockableBackend(ctx, t, srv, bucket, prefix, nil)

	h := backend.Handle{Type: backend.IndexFile, Name: "governance-object"}
	if err := be.Save(ctx, h, backend.NewByteReader([]byte("governance payload"), nil)); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Set GOVERNANCE retention directly via the raw client -- bypassing
	// SetRetention, which is hardcoded to COMPLIANCE -- against the exact S3
	// key the backend wrote to.
	objKey := objectKeyFor(prefix, h)
	governance := minio.Governance
	retainUntil := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	if err := client.PutObjectRetention(ctx, bucket, objKey, minio.PutObjectRetentionOptions{
		RetainUntilDate: &retainUntil,
		Mode:            &governance,
	}); err != nil {
		t.Fatalf("PutObjectRetention (GOVERNANCE) via the raw client failed: %v", err)
	}

	// The backend's RetainedUntil correctly reports the GOVERNANCE retention,
	// even though it was never set through SetRetention.
	gotRetainUntil, locked, err := be.RetainedUntil(ctx, h)
	if err != nil {
		t.Fatalf("RetainedUntil failed: %v", err)
	}
	if !locked {
		t.Fatal("expected locked == true for a GOVERNANCE-locked object")
	}
	if !gotRetainUntil.Truncate(time.Second).Equal(retainUntil) {
		t.Fatalf("expected RetainedUntil to report %v, got %v", retainUntil, gotRetainUntil)
	}

	// The backend's Remove correctly classifies the GOVERNANCE-locked delete
	// failure with the same sentinel as COMPLIANCE.
	if err := be.Remove(ctx, h); err == nil {
		t.Fatal("expected Remove to fail against a GOVERNANCE-locked file with active retention")
	} else if !errors.Is(err, backend.ErrObjectLocked) {
		t.Fatalf("expected errors.Is(err, backend.ErrObjectLocked) to hold for a GOVERNANCE lock, got %v", err)
	}

	// The object must still be genuinely present -- the classified delete
	// never actually removed it.
	if _, err := client.StatObject(ctx, bucket, objKey, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("expected the GOVERNANCE-locked object to still exist after the failed Remove, StatObject failed: %v", err)
	}
}

// TestObjectLockerInertWhenFlagDisabled confirms that, through the real
// backend rather than a bare struct, every ObjectLocker method refuses to
// make any S3 call and returns the flag-not-enabled error when
// feature.ObjectLock is disabled, and that Remove's behavior for a locked
// object is byte-for-byte what it was before this feature existed: a
// version-less delete that "succeeds" by writing a delete marker over the
// still-locked version (see TestUnversionedDeleteBypassesLock), never
// ErrObjectLocked.
func TestObjectLockerInertWhenFlagDisabled(t *testing.T) {
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
	const prefix = "flag-disabled-inert-test"

	// Deliberately do not enable feature.ObjectLock: it defaults to disabled
	// (see internal/feature/registry_test.go).
	be := openLockableBackend(ctx, t, srv, bucket, prefix, nil)

	h := backend.Handle{Type: backend.IndexFile, Name: "flag-disabled-object"}
	if err := be.Save(ctx, h, backend.NewByteReader([]byte("flag disabled payload"), nil)); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Lock the object out-of-band via the raw client, so the flag being off
	// is the only reason Remove below could possibly succeed. Capture the
	// locked version's VersionID now, while it is still the current version:
	// once Remove below writes a delete marker on top, an unversioned lookup
	// (GetObjectRetention/StatObject with no VersionID) targets the marker,
	// not the version underneath, so this is the only point that version ID
	// is still reachable without one.
	objKey := objectKeyFor(prefix, h)
	info, err := client.StatObject(ctx, bucket, objKey, minio.StatObjectOptions{})
	if err != nil {
		t.Fatalf("StatObject before locking failed: %v", err)
	}
	compliance := minio.Compliance
	retainUntil := time.Now().Add(24 * time.Hour)
	if err := client.PutObjectRetention(ctx, bucket, objKey, minio.PutObjectRetentionOptions{
		RetainUntilDate: &retainUntil,
		Mode:            &compliance,
		VersionID:       info.VersionID,
	}); err != nil {
		t.Fatalf("PutObjectRetention failed: %v", err)
	}

	t.Run("ObjectLocker methods return the flag-not-enabled error", func(t *testing.T) {
		if _, err := be.IsObjectLockEnabled(ctx); err == nil || !strings.Contains(err.Error(), "object-lock") {
			t.Fatalf("expected IsObjectLockEnabled to fail naming `object-lock`, got %v", err)
		}
		if err := be.SetRetention(ctx, h, time.Now().Add(time.Hour)); err == nil || !strings.Contains(err.Error(), "object-lock") {
			t.Fatalf("expected SetRetention to fail naming `object-lock`, got %v", err)
		}
		if _, _, err := be.RetainedUntil(ctx, h); err == nil || !strings.Contains(err.Error(), "object-lock") {
			t.Fatalf("expected RetainedUntil to fail naming `object-lock`, got %v", err)
		}
	})

	t.Run("Remove behaves exactly as before this feature", func(t *testing.T) {
		// The pre-feature behavior: a version-less RemoveObject call, which
		// "succeeds" by writing a delete marker over the current (locked)
		// version, never observing the lock at all.
		if err := be.Remove(ctx, h); err != nil {
			t.Fatalf("expected Remove to succeed (delete-marker write) with the flag disabled, got: %v", err)
		}

		// From an unversioned read, the object now looks gone.
		if _, err := client.StatObject(ctx, bucket, objKey, minio.StatObjectOptions{}); err == nil {
			t.Fatal("expected unversioned StatObject to report the object as gone after the delete-marker write")
		}

		// But nothing was actually destroyed: the underlying locked version
		// is still present, still WORM-protected.
		mode, gotRetainUntil, err := client.GetObjectRetention(ctx, bucket, objKey, info.VersionID)
		if err != nil {
			t.Fatalf("expected the locked version's retention to still be readable, got error: %v", err)
		}
		if mode == nil || *mode != minio.Compliance {
			t.Fatalf("expected the locked version to still report COMPLIANCE mode, got %v", mode)
		}
		if gotRetainUntil == nil {
			t.Fatal("expected a non-nil retainUntilDate for the still-present locked version")
		}
	})
}

// TestObjectLockerRemoveUnrelatedFailureNotMisclassified is the regression
// guard for TASK-16's over-matching risk (see the technical notes on
// TASK-17): it confirms that Remove's ErrObjectLocked classification is
// never accidentally triggered by an unrelated failure, using the example
// the acceptance criteria call out directly -- deleting an
// already-deleted/nonexistent file -- through the real backend with the
// feature flag enabled.
func TestObjectLockerRemoveUnrelatedFailureNotMisclassified(t *testing.T) {
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
	const prefix = "unrelated-failure-test"

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	be := openLockableBackend(ctx, t, srv, bucket, prefix, nil)

	h := backend.Handle{Type: backend.IndexFile, Name: "never-saved-object"}

	// Remove on a file that was never saved: with the flag enabled,
	// removeObjectLockAware's leading StatObject fails with not-exist, which
	// must be treated as a normal, error-free no-op -- never as a lock.
	err = be.Remove(ctx, h)
	if err != nil {
		t.Fatalf("expected Remove on a nonexistent file to succeed as a no-op, got: %v", err)
	}
	if errors.Is(err, backend.ErrObjectLocked) {
		t.Fatal("a nonexistent-file failure must never be misclassified as backend.ErrObjectLocked")
	}

	// Same check for a file that existed and was already deleted: delete it
	// twice, and confirm the second Remove is still a clean no-op, not a
	// locked-delete misclassification.
	h2 := backend.Handle{Type: backend.IndexFile, Name: "delete-twice-object"}
	if err := be.Save(ctx, h2, backend.NewByteReader([]byte("delete twice payload"), nil)); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if err := be.Remove(ctx, h2); err != nil {
		t.Fatalf("first Remove failed: %v", err)
	}

	err = be.Remove(ctx, h2)
	if err != nil {
		t.Fatalf("expected the second Remove of an already-deleted file to succeed as a no-op, got: %v", err)
	}
	if errors.Is(err, backend.ErrObjectLocked) {
		t.Fatal("an already-deleted file's second Remove must never be misclassified as backend.ErrObjectLocked")
	}
}
