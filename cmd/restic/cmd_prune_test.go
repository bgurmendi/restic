package main

import (
	"testing"

	"github.com/restic/restic/internal/feature"
	rtest "github.com/restic/restic/internal/test"
)

// TestPruneSkipObjectLockedFeatureFlagGate confirms that `--skip-object-locked`
// is rejected with a clear error before the object-lock feature flag is
// enabled, and accepted (no error from this gate) once it is - mirroring
// TestForgetSkipObjectLockedFeatureFlagGate in cmd_forget_test.go.
func TestPruneSkipObjectLockedFeatureFlagGate(t *testing.T) {
	rtest.Assert(t, !feature.Flag.Enabled(feature.ObjectLock), "expected object-lock feature to be disabled by default")

	opts := PruneOptions{SkipObjectLocked: true, MaxUnused: "5%"}
	err := verifyPruneOptions(&opts)
	rtest.Assert(t, err != nil, "expected an error when object-lock feature flag is disabled")
	rtest.Equals(t, "Fatal: feature flag `object-lock` is required to use `--skip-object-locked`, enable it by setting RESTIC_FEATURES=object-lock", err.Error())

	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()
	err = verifyPruneOptions(&opts)
	rtest.Assert(t, err == nil, "expected no error once the object-lock feature flag is enabled, got %v", err)
}

// TestPruneSkipObjectLockedDefaultUnaffected confirms --skip-object-locked
// defaults to false and, when left unset, never trips the feature-flag gate -
// i.e. prune's default behavior is unaffected by this task.
func TestPruneSkipObjectLockedDefaultUnaffected(t *testing.T) {
	opts := PruneOptions{MaxUnused: "5%"}
	rtest.Equals(t, false, opts.SkipObjectLocked)
	rtest.Assert(t, verifyPruneOptions(&opts) == nil, "expected no error for default options")
}
