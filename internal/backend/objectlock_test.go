package backend

import (
	"context"
	"testing"
	"time"

	"github.com/restic/restic/internal/errors"
	rtest "github.com/restic/restic/internal/test"
)

// mockObjectLocker is a minimal ObjectLocker used purely to prove the
// interface is implementable with the expected method set/signatures. It is
// not wired into any real backend yet - that's TASK-14/15/16.
type mockObjectLocker struct {
	enabled bool
}

func (m *mockObjectLocker) IsObjectLockEnabled(_ context.Context) (bool, error) {
	return m.enabled, nil
}

func (m *mockObjectLocker) RetainedUntil(_ context.Context, _ Handle) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (m *mockObjectLocker) SetRetention(_ context.Context, _ Handle, _ time.Time) error {
	return nil
}

var _ ObjectLocker = &mockObjectLocker{}

func TestErrObjectLockedIsSentinel(t *testing.T) {
	// A locked-delete failure is expected to wrap ErrObjectLocked, so callers
	// can classify it with a plain errors.Is check - never string-matching.
	wrapped := errors.Wrap(ErrObjectLocked, "failed to remove file")
	rtest.Assert(t, errors.Is(wrapped, ErrObjectLocked),
		"expected errors.Is to find ErrObjectLocked through a wrapped error")

	unrelated := errors.New("some other failure")
	rtest.Assert(t, !errors.Is(unrelated, ErrObjectLocked),
		"expected an unrelated error to not match ErrObjectLocked")
}
