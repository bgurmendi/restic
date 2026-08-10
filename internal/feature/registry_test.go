package feature_test

import (
	"testing"

	"github.com/restic/restic/internal/feature"
	rtest "github.com/restic/restic/internal/test"
)

// TestObjectLockFlagRegisteredAndDisabledByDefault confirms the `object-lock`
// Alpha flag is registered on the global feature.Flag set and, like other
// Alpha flags, defaults to disabled.
func TestObjectLockFlagRegisteredAndDisabledByDefault(t *testing.T) {
	rtest.Assert(t, !feature.Flag.Enabled(feature.ObjectLock), "expected object-lock feature to be disabled by default")
}
