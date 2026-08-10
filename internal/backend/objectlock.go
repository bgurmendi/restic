package backend

import (
	"context"
	"time"

	"github.com/restic/restic/internal/errors"
)

// ErrObjectLocked is returned wrapped from Remove when the backend refuses
// to delete a file because it is under an active retention lock (e.g. S3
// Object Lock in COMPLIANCE or GOVERNANCE mode). Callers must check for it
// with errors.Is(err, backend.ErrObjectLocked); do not string-match the
// underlying error.
//
// This rides on the Remove call a caller already makes: detecting a lock is
// "free" for the common case (most files are not locked, so Remove just
// succeeds) and costs nothing extra beyond the delete attempt itself when a
// file *is* locked.
var ErrObjectLocked = errors.New("file is object-locked and cannot be removed")

// ObjectLocker is implemented by backends that support setting a
// retention-based delete lock on individual files (currently only the s3
// backend, via S3 Object Lock). It is deliberately small: the two read
// methods cover only the operations that genuinely require backend-specific
// I/O beyond a normal Remove call. Detecting whether a given file is
// currently locked is NOT one of them - that is done by attempting the
// ordinary Remove and checking errors.Is(err, ErrObjectLocked), never by
// calling a method on this interface first.
type ObjectLocker interface {
	// IsObjectLockEnabled reports whether the backend's underlying bucket
	// has Object Lock enabled. It is intended to be called once per command
	// run, as a pre-flight check (e.g. before `restic protect` attempts to
	// set any retention), not per-file.
	IsObjectLockEnabled(ctx context.Context) (bool, error)

	// RetainedUntil returns the retention expiry for the file described by
	// h, and whether it is currently locked. Call this only after a Remove
	// call for h has already failed with ErrObjectLocked; this method
	// exists purely to produce a user-facing retention date for reporting,
	// it is never used to decide whether to skip a delete - that decision
	// is always made from the error returned by Remove itself.
	RetainedUntil(ctx context.Context, h Handle) (retainUntil time.Time, locked bool, err error)

	// SetRetention extends the retention lock on the file described by h
	// until retainUntil. Repeated calls with an earlier retainUntil than
	// the file's current retention must not shorten it; the error-handling
	// contract for that case (attempt-and-treat-shorten-rejection-as-no-op,
	// vs. read-then-write) is finalized and implemented by the backend, not
	// by this interface.
	SetRetention(ctx context.Context, h Handle, retainUntil time.Time) error
}

// AsObjectLocker walks be's Unwrap() chain -- the same chain wrapping layers
// like retry/cache/logger/sema use -- looking for a backend that implements
// ObjectLocker (currently only the s3 backend). It returns (nil, false) if no
// layer in the chain implements it.
//
// This mirrors AsBackend above, but cannot reuse it directly: AsBackend's
// generic constraint requires the target type to itself satisfy the full
// Backend interface, which ObjectLocker deliberately does not -- it is a
// narrow, additional capability some backends implement on top of Backend,
// not a replacement for it.
func AsObjectLocker(be Backend) (ObjectLocker, bool) {
	for be != nil {
		if ol, ok := be.(ObjectLocker); ok {
			return ol, true
		}

		if uw, ok := be.(Unwrapper); ok {
			be = uw.Unwrap()
		} else {
			break
		}
	}
	return nil, false
}
