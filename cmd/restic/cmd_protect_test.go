package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/all"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
	"github.com/spf13/pflag"
)

// fakeObjectLocker wraps a backend.Backend and implements backend.ObjectLocker
// by recording SetRetention calls in memory, so protect's object-set and
// retention-application logic can be tested without a real S3/MinIO backend.
// It embeds the wrapped backend so every other backend.Backend method passes
// straight through.
type fakeObjectLocker struct {
	backend.Backend

	mu      sync.Mutex
	applied map[backend.Handle]time.Time
}

var _ backend.ObjectLocker = &fakeObjectLocker{}

func newFakeObjectLocker(be backend.Backend) *fakeObjectLocker {
	return &fakeObjectLocker{Backend: be, applied: map[backend.Handle]time.Time{}}
}

func (f *fakeObjectLocker) IsObjectLockEnabled(_ context.Context) (bool, error) {
	return true, nil
}

func (f *fakeObjectLocker) RetainedUntil(_ context.Context, h backend.Handle) (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.applied[h]
	return t, ok, nil
}

func (f *fakeObjectLocker) SetRetention(_ context.Context, h backend.Handle, retainUntil time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied[h] = retainUntil
	return nil
}

func TestProtectForFlagParsing(t *testing.T) {
	testCases := []struct {
		input   string
		value   data.Duration
		wantErr bool
	}{
		{input: "120d", value: data.Duration{Days: 120}},
		{input: "1y2m3d", value: data.Duration{Years: 1, Months: 2, Days: 3}},
		{input: "9h", value: data.Duration{Hours: 9}},
		{input: "banana", wantErr: true},
		{input: "2w", wantErr: true}, // weeks aren't a supported unit, unlike days/months/years/hours
	}

	for _, testCase := range testCases {
		t.Run(testCase.input, func(t *testing.T) {
			set := pflag.NewFlagSet("protect", pflag.ContinueOnError)
			var opts ProtectOptions
			opts.AddFlags(set)

			err := set.Parse([]string{"--for", testCase.input})
			if testCase.wantErr {
				rtest.Assert(t, err != nil, "should have returned error for input %q", testCase.input)
			} else {
				rtest.OK(t, err)
				rtest.Equals(t, testCase.value, opts.For)
			}
		})
	}
}

func TestVerifyProtectOptions(t *testing.T) {
	const missingErrorMsg = "Fatal: --for is required"
	const negValErrorMsg = "Fatal: durations containing negative values are not allowed for --for"

	testCases := []struct {
		input    ProtectOptions
		errorMsg string
	}{
		{ProtectOptions{}, missingErrorMsg},
		{ProtectOptions{For: data.ParseDurationOrPanic("120d")}, ""},
		{ProtectOptions{For: data.ParseDurationOrPanic("1y2m3d3h")}, ""},
		{ProtectOptions{For: data.ParseDurationOrPanic("-1y2m3d3h")}, negValErrorMsg},
		{ProtectOptions{For: data.ParseDurationOrPanic("1y-2m3d3h")}, negValErrorMsg},
		{ProtectOptions{For: data.ParseDurationOrPanic("1y2m-3d3h")}, negValErrorMsg},
		{ProtectOptions{For: data.ParseDurationOrPanic("1y2m3d-3h")}, negValErrorMsg},
	}

	for _, testCase := range testCases {
		err := verifyProtectOptions(&testCase.input)
		if testCase.errorMsg != "" {
			rtest.Assert(t, err != nil, "should have returned error for input %+v", testCase.input)
			rtest.Equals(t, testCase.errorMsg, err.Error())
		} else {
			rtest.Assert(t, err == nil, "expected no error for input %+v, got %v", testCase.input, err)
		}
	}
}

// TestProtectFlagsIncludeForgetKeepFlags confirms `protect` shares (rather
// than redefines) forget's "--keep-*" policy flags, plus its own "--for"
// flag: registering both ForgetOptions' and ProtectOptions' flags on
// separate flag sets should yield the exact same "--keep-*"/"--within-*"
// flag names, and protect's set should additionally contain "--for".
func TestProtectFlagsIncludeForgetKeepFlags(t *testing.T) {
	forgetSet := pflag.NewFlagSet("forget", pflag.ContinueOnError)
	var forgetOpts ForgetOptions
	forgetOpts.AddFlags(forgetSet)

	protectSet := pflag.NewFlagSet("protect", pflag.ContinueOnError)
	var protectOpts ProtectOptions
	protectOpts.AddFlags(protectSet)

	forgetSet.VisitAll(func(f *pflag.Flag) {
		if len(f.Name) < 5 || f.Name[:5] != "keep-" {
			return
		}
		got := protectSet.Lookup(f.Name)
		rtest.Assert(t, got != nil, "expected protect to expose forget's %q flag too", f.Name)
	})

	rtest.Assert(t, protectSet.Lookup("for") != nil, "expected protect to expose a --for flag")
}

// TestProtectRetainUntil confirms the target-date arithmetic itself: given a
// fixed "now" (not time.Now(), so the test is deterministic) and a
// data.Duration, protectRetainUntil returns exactly now + duration,
// truncated to the second. It does not exercise SetRetention's own
// shorten-is-a-no-op behavior - that's TASK-15/TASK-26's job - this test
// only proves the plumbing that turns "--for" into a single target date.
func TestProtectRetainUntil(t *testing.T) {
	// Fixed with a non-zero, non-round sub-second component to make sure
	// truncation is actually exercised rather than accidentally true anyway.
	now := time.Date(2026, time.January, 15, 10, 30, 45, 123456789, time.UTC)

	testCases := []struct {
		name     string
		duration data.Duration
		want     time.Time
	}{
		{"zero duration", data.Duration{}, time.Date(2026, time.January, 15, 10, 30, 45, 0, time.UTC)},
		{"days only", data.Duration{Days: 120}, time.Date(2026, time.May, 15, 10, 30, 45, 0, time.UTC)},
		{"hours only", data.Duration{Hours: 9}, time.Date(2026, time.January, 15, 19, 30, 45, 0, time.UTC)},
		{"years, months, and days", data.Duration{Years: 1, Months: 2, Days: 3}, time.Date(2027, time.March, 18, 10, 30, 45, 0, time.UTC)},
		{"all units combined", data.Duration{Years: 1, Months: 1, Days: 1, Hours: 25}, time.Date(2027, time.February, 17, 11, 30, 45, 0, time.UTC)},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got := protectRetainUntil(now, testCase.duration)
			rtest.Assert(t, got.Equal(testCase.want), "expected retainUntil %v, got %v", testCase.want, got)
			rtest.Equals(t, 0, got.Nanosecond())
		})
	}
}

// TestProtectFeatureFlagGate confirms that running protect with the
// "object-lock" feature flag disabled (the default) fails immediately with a
// clear error, before the repository is ever opened: gopts here points at a
// repository path that does not exist, so any attempt to open it would fail
// with a different (repository-not-found) error instead.
func TestProtectFeatureFlagGate(t *testing.T) {
	rtest.Assert(t, !feature.Flag.Enabled(feature.ObjectLock), "expected object-lock feature to be disabled by default")

	var opts ProtectOptions
	opts.For = data.ParseDurationOrPanic("30d")
	opts.Daily = 7

	gopts := global.Options{Repo: t.TempDir() + "/does-not-exist", Backends: all.Backends()}

	err := runProtect(context.Background(), opts, gopts, nil)
	rtest.Assert(t, err != nil, "expected an error when object-lock feature flag is disabled")
	rtest.Equals(t, "Fatal: feature flag `object-lock` is required to use the `protect` command, enable it by setting RESTIC_FEATURES=object-lock", err.Error())
}

// TestProtectFeatureFlagGateEnabled confirms that with the feature flag
// enabled, valid keep-* + --for flags cause protect to proceed past the gate
// (i.e. it goes on to actually try opening the repository instead of
// returning the feature-flag error).
func TestProtectFeatureFlagGateEnabled(t *testing.T) {
	defer feature.TestSetFlag(t, feature.Flag, feature.ObjectLock, true)()

	var opts ProtectOptions
	opts.For = data.ParseDurationOrPanic("30d")
	opts.Daily = 7

	gopts := global.Options{Repo: t.TempDir() + "/does-not-exist", Backends: all.Backends()}

	err := runProtect(context.Background(), opts, gopts, nil)
	rtest.Assert(t, err != nil, "expected an error, repository does not exist")
	rtest.Assert(t, err.Error() != "Fatal: feature flag `object-lock` is required to use the `protect` command, enable it by setting RESTIC_FEATURES=object-lock",
		"expected the feature-flag gate to be passed, got the gate error again: %v", err)
}

// TestCollectProtectObjectSetExcludesLocks is the explicit runtime assertion
// TASK-23's acceptance criteria calls for: a repository with a real lock file
// present, checked against collectProtectObjectSet's actual returned handles
// (not just the absence of code that would add one) - so a future refactor
// that accidentally started listing locks/* would fail this test.
func TestCollectProtectObjectSetExcludesLocks(t *testing.T) {
	ctx := context.Background()
	printer := progress.NewNoopPrinter()

	repo, be := repository.TestRepositoryWithBackend(t, nil, 2, repository.Options{})
	sn := data.TestCreateSnapshot(t, repo, time.Unix(1600000000, 0), 2)

	// Write a real lock file directly, exactly as repository.LockRepo would,
	// so it is genuinely present in the backend under test.
	lockID := restic.NewRandomID()
	lockHandle := backend.Handle{Type: restic.LockFile, Name: lockID.String()}
	rtest.OK(t, be.Save(ctx, lockHandle, backend.NewByteReader([]byte("{}"), be.Hasher())))

	rtest.OK(t, repo.LoadIndex(ctx, printer))
	set, err := collectProtectObjectSet(ctx, repo, data.Snapshots{sn}, printer)
	rtest.OK(t, err)

	for _, h := range set.all() {
		rtest.Assert(t, h.Type != restic.LockFile, "expected no locks/* handle in the protected object set, got %v", h)
	}
	rtest.Equals(t, []backend.Handle{{Type: restic.ConfigFile}}, set.Config)
	rtest.Assert(t, len(set.Keys) > 0, "expected at least one key handle")
	rtest.Assert(t, len(set.Index) > 0, "expected at least one index handle")
	rtest.Equals(t, []backend.Handle{{Type: restic.SnapshotFile, Name: sn.ID().String()}}, set.Snapshots)
	rtest.Assert(t, len(set.Packs) > 0, "expected at least one pack handle")
}

// TestRunProtectApplyProtectsExpectedObjectSet drives runProtectApply against
// a repository backed by fakeObjectLocker (so no real S3/MinIO server is
// needed here) with two independently-seeded snapshots, protecting only one
// of them - simulating a policy that keeps the newer snapshot and would
// remove the older one. It asserts SetRetention was called exactly once per
// object in the returned set, with exactly the requested retainUntil, and
// that the older snapshot's exclusive pack(s) were never touched.
func TestRunProtectApplyProtectsExpectedObjectSet(t *testing.T) {
	ctx := context.Background()
	printer := progress.NewNoopPrinter()

	locker := newFakeObjectLocker(repository.TestBackend(t))
	repo, _ := repository.TestRepositoryWithBackend(t, locker, 2, repository.Options{})

	older := data.TestCreateSnapshot(t, repo, time.Unix(1500000000, 0), 2)
	newer := data.TestCreateSnapshot(t, repo, time.Unix(1600000000, 0), 2)

	toProtect := data.Snapshots{newer}
	retainUntil := time.Now().Add(30 * 24 * time.Hour).Truncate(time.Second)

	objects, err := runProtectApply(ctx, repo, printer, toProtect, retainUntil, false)
	rtest.OK(t, err)

	rtest.Equals(t, []backend.Handle{{Type: restic.SnapshotFile, Name: newer.ID().String()}}, objects.Snapshots)

	// Every handle in the returned set - and only those handles - must have
	// had SetRetention called on it with exactly retainUntil.
	all := objects.all()
	rtest.Equals(t, len(all), len(locker.applied))
	for _, h := range all {
		got, ok := locker.applied[h]
		rtest.Assert(t, ok, "expected SetRetention to have been called for %v", h)
		rtest.Assert(t, got.Equal(retainUntil), "expected retainUntil %v for %v, got %v", retainUntil, h, got)
	}

	// older's exclusive pack(s) must never have had SetRetention called on
	// them: compute older's own live packs (ground truth from the same
	// TASK-19 primitive) and confirm none of them made it into the applied
	// set unless newer also needs them.
	olderPacks, err := repository.LivePacksForSnapshots(ctx, repo, restic.NewIDSet(*older.ID()), printer)
	rtest.OK(t, err)
	newerPacks, err := repository.LivePacksForSnapshots(ctx, repo, restic.NewIDSet(*newer.ID()), printer)
	rtest.OK(t, err)
	exclusiveToOlder := false
	for id := range olderPacks {
		if !newerPacks.Has(id) {
			exclusiveToOlder = true
			h := backend.Handle{Type: restic.PackFile, Name: id.String()}
			_, ok := locker.applied[h]
			rtest.Assert(t, !ok, "expected pack %v (exclusive to the un-selected older snapshot) to not have retention applied", id)
		}
	}
	rtest.Assert(t, exclusiveToOlder, "expected the older snapshot to need at least one pack the newer one does not, otherwise this test proves nothing")

	// locks/* must never appear among the applied handles either.
	for h := range locker.applied {
		rtest.Assert(t, h.Type != restic.LockFile, "expected no locks/* handle to ever have retention applied, got %v", h)
	}
}

// TestRunProtectApplyRequiresObjectLocker confirms runProtectApply fails with
// a clear error - rather than a panic or a silent no-op - when the
// repository's backend does not implement backend.ObjectLocker (e.g. the
// in-memory test backend, standing in for any non-s3 backend).
func TestRunProtectApplyRequiresObjectLocker(t *testing.T) {
	ctx := context.Background()
	printer := progress.NewNoopPrinter()

	repo := repository.TestRepository(t)
	sn := data.TestCreateSnapshot(t, repo, time.Unix(1600000000, 0), 1)

	_, err := runProtectApply(ctx, repo, printer, data.Snapshots{sn}, time.Now().Add(24*time.Hour), false)
	rtest.Assert(t, err != nil, "expected an error for a backend without Object Lock support")
	rtest.Equals(t, "Fatal: the `protect` command requires a backend with S3 Object Lock support (currently only the `s3` backend); this repository uses the \"mem\" backend, which does not support it", err.Error())
}
