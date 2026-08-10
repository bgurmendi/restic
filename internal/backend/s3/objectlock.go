package s3

import (
	"context"
	"fmt"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/feature"

	"github.com/minio/minio-go/v7"
)

// make sure that *s3 implements backend.ObjectLocker
var _ backend.ObjectLocker = &s3{}

// objectLockNotEnabledCode is the S3 error code minio-go's
// GetBucketObjectLockConfig returns when a bucket exists but was not created
// with Object Lock enabled. Confirmed against a real local MinIO server (see
// TestObjectLockDisabledDetection); CheckObjectLockEnabled keys off this
// stable code rather than the associated error message, which can vary
// across S3-compatible providers.
const objectLockNotEnabledCode = "ObjectLockConfigurationNotFoundError"

// ErrObjectLockNotEnabled is wrapped by the error ObjectLockNotEnabledError
// returns, so callers can detect the "Object Lock is not enabled on this
// bucket" case programmatically with errors.Is, instead of parsing error
// strings.
var ErrObjectLockNotEnabled = errors.New("object lock is not enabled on this bucket; it can only be enabled when the bucket is created, not afterwards")

// ObjectLockNotEnabledError returns a clear, actionable error explaining
// that bucket does not have S3 Object Lock enabled. Object Lock cannot be
// turned on for an existing bucket, only at bucket-creation time, so the
// message points the caller at re-creating the bucket rather than at fixing
// it in place. Callers that need to fail once CheckObjectLockEnabled reports
// enabled == false should return this error rather than composing their own.
//
// The returned error wraps ErrObjectLockNotEnabled, so it can be recognized
// with errors.Is(err, ErrObjectLockNotEnabled).
func ObjectLockNotEnabledError(bucket string) error {
	return fmt.Errorf("bucket %q: %w", bucket, ErrObjectLockNotEnabled)
}

// CheckObjectLockEnabled reports whether bucket has S3 Object Lock enabled.
//
// It returns (true, nil) if Object Lock is enabled. It returns (false, nil)
// if the bucket exists but was created without Object Lock -- a valid,
// expected state to report to the caller, not itself an error. Callers that
// need to fail on that case can do so with ObjectLockNotEnabledError(bucket).
// For any other failure (the bucket does not exist, a network/auth error,
// ...) it returns (false, err), with err wrapping the underlying cause so
// errors.Is/errors.As inspection of it still works.
//
// CheckObjectLockEnabled never attempts to enable Object Lock itself: it
// cannot be turned on for an existing bucket, only at bucket-creation time.
func CheckObjectLockEnabled(ctx context.Context, client *minio.Client, bucket string) (bool, error) {
	_, _, _, err := client.GetBucketObjectLockConfig(ctx, bucket)
	if err == nil {
		return true, nil
	}

	if minio.ToErrorResponse(err).Code == objectLockNotEnabledCode {
		return false, nil
	}

	return false, fmt.Errorf("checking object lock status for bucket %q: %w", bucket, err)
}

// IsObjectLockEnabled implements backend.ObjectLocker. It reports whether
// the backend's configured bucket has S3 Object Lock enabled, by delegating
// to CheckObjectLockEnabled and converting a "not enabled" result into the
// clear, actionable ObjectLockNotEnabledError.
//
// The whole capability is gated behind the `object-lock` feature flag: when
// the flag is disabled, this method returns immediately without making any
// S3 call, matching the `s3-restore` flag-gating pattern used by open() for
// EnableRestore.
func (be *s3) IsObjectLockEnabled(ctx context.Context) (bool, error) {
	if !feature.Flag.Enabled(feature.ObjectLock) {
		return false, fmt.Errorf("feature flag `object-lock` is required to use S3 Object Lock support")
	}

	enabled, err := CheckObjectLockEnabled(ctx, be.client, be.cfg.Bucket)
	if err != nil {
		return false, err
	}
	if !enabled {
		return false, ObjectLockNotEnabledError(be.cfg.Bucket)
	}
	return true, nil
}

// objectLockNoRetentionCode is the S3 error code minio-go's
// GetObjectRetention returns when the object (or object version) it targets
// has no retention configuration at all -- i.e. it was never locked, as
// opposed to having a retention that has since expired (which returns the
// retention normally, with a past retainUntilDate, no error). Confirmed
// against a real local MinIO server. RetainedUntil below treats this code
// as "not locked", the same non-error outcome as an expired retention.
const objectLockNoRetentionCode = "NoSuchObjectLockConfiguration"

// RetainedUntil implements backend.ObjectLocker. It reports the S3 Object
// Lock retention expiry for the file described by h, calling
// GetObjectRetention exactly once.
//
// Per the ObjectLocker interface contract, this is a lazy, reporting-only
// lookup: callers must only invoke it after a Remove call for h has already
// failed with ErrObjectLocked, purely to obtain a user-facing retention
// date. It is never used to decide whether to skip a delete.
func (be *s3) RetainedUntil(ctx context.Context, h backend.Handle) (time.Time, bool, error) {
	if !feature.Flag.Enabled(feature.ObjectLock) {
		return time.Time{}, false, fmt.Errorf("feature flag `object-lock` is required to use S3 Object Lock support")
	}

	objName := be.Filename(h)
	_, retainUntilDate, err := be.client.GetObjectRetention(ctx, be.cfg.Bucket, objName, "")
	if err != nil {
		if minio.ToErrorResponse(err).Code == objectLockNoRetentionCode {
			// No retention was ever set on this object: not locked.
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("getting object lock retention for %v: %w", h, err)
	}

	if retainUntilDate == nil || !retainUntilDate.After(time.Now()) {
		// A retention exists but has already expired: not locked.
		return time.Time{}, false, nil
	}

	return *retainUntilDate, true, nil
}

// objectLockRestrictedCode is the S3 error code minio-go returns when an
// operation is blocked by an active Object Lock retention. Confirmed against
// a real local MinIO server: both a PutObjectRetention call that would
// shorten an active COMPLIANCE retention (see TestObjectRetentionShorten)
// and a RemoveObject call against a locked object version (see
// TestObjectRetentionBlocksDelete) fail with this same code, paired with the
// message "Object is WORM protected and cannot be overwritten". SetRetention
// below keys off this code, not the message, matching the
// objectLockNotEnabledCode pattern above.
const objectLockRestrictedCode = "InvalidRequest"

// SetRetention implements backend.ObjectLocker. It extends the S3 Object
// Lock COMPLIANCE retention on the file described by h until retainUntil.
//
// Per TASK-8's finding, MinIO rejects a PutObjectRetention call that would
// shorten an active COMPLIANCE retention with objectLockRestrictedCode, the
// same way documented AWS S3 behavior does. This lets SetRetention always
// attempt retainUntil directly -- no pre-read (GetObjectRetention) before
// every write: a first-time retention or a genuine extension succeeds
// normally, and a rejected shorten attempt is treated as a benign no-op (the
// object is already sufficiently protected), not as an error. Exactly one
// PutObjectRetention call is made per SetRetention call in every case. This
// mirrors TASK-11's delete-and-classify decision on the read side.
//
// SetRetention must never be called for locks/* files (see the PRD's
// "Objects protected" section); enforcing that exclusion is the caller's
// responsibility, not this method's.
func (be *s3) SetRetention(ctx context.Context, h backend.Handle, retainUntil time.Time) error {
	if !feature.Flag.Enabled(feature.ObjectLock) {
		return fmt.Errorf("feature flag `object-lock` is required to use S3 Object Lock support")
	}

	objName := be.Filename(h)
	compliance := minio.Compliance
	err := be.client.PutObjectRetention(ctx, be.cfg.Bucket, objName, minio.PutObjectRetentionOptions{
		RetainUntilDate: &retainUntil,
		Mode:            &compliance,
	})
	if err == nil {
		return nil
	}

	if minio.ToErrorResponse(err).Code == objectLockRestrictedCode {
		// Benign no-op: the object's current retention already covers at
		// least retainUntil, so there is nothing to extend.
		return nil
	}

	return fmt.Errorf("setting object lock retention on %v: %w", h, err)
}

// removeObjectLockAware implements Remove's behavior when the `object-lock`
// feature flag is enabled (see s3.go's Remove, which delegates here).
//
// TASK-9's spike proved (binding finding, real local MinIO server): Object
// Lock requires bucket versioning, and on a versioned bucket a version-less
// RemoveObject call -- the one Remove makes when this feature is disabled --
// never observes a lock at all. It always "succeeds" by writing a new
// delete marker on top of the current version, which only hides the object
// from unversioned reads/lists; the version underneath, locked or not, is
// untouched and its storage is never reclaimed. So once this feature is on,
// a delete must target the object's actual current version explicitly,
// which means first finding that version's ID.
//
// This costs one extra StatObject call before every delete, in both the
// locked and unlocked case, versus the feature-disabled path -- there is no
// way to avoid it: without a VersionID, RemoveObject cannot fail on a lock,
// so its result could not be classified either way, and delete-and-classify
// depends on a delete call that can actually fail. This is a distinct cost
// from -- not a reintroduction of -- the pre-check (GetObjectRetention)
// strategy TASK-11 rejected: a pre-check would still need this same Stat
// call to find the version to delete, PLUS a separate retention check on
// every candidate; delete-and-classify still avoids that second,
// classification-only call in the common (unlocked) case, per TASK-11.
func (be *s3) removeObjectLockAware(ctx context.Context, objName string) error {
	info, err := be.client.StatObject(ctx, be.cfg.Bucket, objName, minio.StatObjectOptions{})
	if err != nil {
		if be.IsNotExist(err) {
			return nil
		}
		return errors.Wrap(err, "client.StatObject")
	}

	err = be.client.RemoveObject(ctx, be.cfg.Bucket, objName, minio.RemoveObjectOptions{VersionID: info.VersionID})
	if err == nil {
		return nil
	}

	if be.IsNotExist(err) {
		return nil
	}

	if minio.ToErrorResponse(err).Code == objectLockRestrictedCode {
		// The delete call restic already had to make failed because the
		// object is under an active retention lock: wrap it with the
		// sentinel so callers can detect it with errors.Is, no separate
		// call needed -- the classification rides on this same failure.
		return fmt.Errorf("%w: %s", backend.ErrObjectLocked, err)
	}

	return errors.Wrap(err, "client.RemoveObject")
}
