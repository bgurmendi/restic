package main

import (
	"testing"

	"github.com/restic/restic/internal/feature"
	rtest "github.com/restic/restic/internal/test"
)

// TestForgetSkipObjectLockedFeatureFlagGate confirms that `--skip-object-locked`
// is rejected with a clear error before the object-lock feature flag is
// enabled, and accepted (no error from this gate) once it is - matching
// runProtect's feature-flag-gate pattern in cmd_protect_test.go.
func TestForgetSkipObjectLockedFeatureFlagGate(t *testing.T) {
	rtest.Assert(t, !feature.Flag.Enabled(feature.ObjectLock), "expected object-lock feature to be disabled by default")

	opts := ForgetOptions{SkipObjectLocked: true}
	err := verifyForgetOptions(&opts)
	rtest.Assert(t, err != nil, "expected an error when object-lock feature flag is disabled")
	rtest.Equals(t, "Fatal: feature flag `object-lock` is required to use `--skip-object-locked`, enable it by setting RESTIC_FEATURES=object-lock", err.Error())

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()
	err = verifyForgetOptions(&opts)
	rtest.Assert(t, err == nil, "expected no error once the object-lock feature flag is enabled, got %v", err)
}

// TestForgetSkipObjectLockedDefaultUnaffected confirms --skip-object-locked
// defaults to false and, when left unset, never trips the feature-flag gate -
// i.e. forget's default behavior is unaffected by this task.
func TestForgetSkipObjectLockedDefaultUnaffected(t *testing.T) {
	var opts ForgetOptions
	rtest.Equals(t, false, opts.SkipObjectLocked)
	rtest.Assert(t, verifyForgetOptions(&opts) == nil, "expected no error for default options")
}
