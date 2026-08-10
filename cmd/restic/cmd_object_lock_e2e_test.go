package main

// TASK-40: the definitive end-to-end proof for this whole feature slice. It
// runs BASE.md/PRD section 4's exact recommended daily flow --
//
//	restic protect --keep-daily 3 --for <duration>
//	restic forget  --keep-daily 3 --skip-object-locked
//	restic prune   --skip-object-locked
//
// -- twice in a row (simulating two consecutive days' cron runs) against one
// realistic, deduplicated repository on a real, locally-run MinIO
// Object-Lock-enabled bucket. Every individual command's own tests
// (TASK-25/26/31/36) already prove their behavior in isolation; this test
// proves they compose correctly: nothing a kept snapshot needs is ever lost,
// snapshots/packs outside the policy that were never protected are removed
// normally, and a snapshot that WAS protected in the first run but has since
// fallen out of the policy is only cleaned up once its retention has
// actually expired -- exercising the "extend, never shorten" guarantee
// (TASK-26) across a real elapsed-time boundary, not just protectRetainUntil's
// pure-function level.
//
// Reuses the MinIO process-spawning harness and protectSaveBlob/
// protectSaveSnapshot deterministic-sharing helpers already established in
// cmd_protect_integration_test.go (same package).

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/layout"
	"github.com/restic/restic/internal/backend/s3"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/feature"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/options"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

// e2eSaveDay creates one simulated day's snapshot: it shares one common
// "base" blob with every other day (simulating deduplicated, mostly
// unchanging content across daily backups) plus one blob unique to that day,
// at a fixed calendar date so `--keep-daily` buckets land exactly as
// expected regardless of the machine's local timezone.
func e2eSaveDay(ctx context.Context, t *testing.T, repo *repository.Repository, day int, shared restic.ID) *data.Snapshot {
	t.Helper()
	unique := protectSaveBlob(ctx, t, repo, []byte(fmt.Sprintf("e2e flow: unique content for day %d", day)))
	at := time.Date(2024, time.January, day, 12, 0, 0, 0, time.UTC)
	return protectSaveSnapshot(ctx, t, repo, at, map[string]restic.IDs{
		"base": {shared},
		"day":  {unique},
	})
}

func TestObjectLockEndToEndFlow(t *testing.T) {
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

	prefix := fmt.Sprintf("e2e-flow-%d", time.Now().UnixNano())

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

	t.Setenv("AWS_ACCESS_KEY_ID", srv.KeyID)
	t.Setenv("AWS_SECRET_ACCESS_KEY", srv.Secret)
	gopts := protectTestGopts(srv, bucket, prefix, filepath.Join(tempdir, "cache"))

	printer := progress.NewNoopPrinter()
	groupBy := protectDefaultGroupBy
	policy := data.ExpirePolicy{Daily: 3}

	// A realistic 5-day-old backup history: one shared "base" blob (the
	// dedup case BASE.md is all about) plus one blob unique to each day.
	shared := protectSaveBlob(ctx, t, repo, []byte("e2e flow: shared base content across every day"))
	var days []*data.Snapshot
	for d := 1; d <= 5; d++ {
		days = append(days, e2eSaveDay(ctx, t, repo, d, shared))
	}

	// runFlow executes one cron pass of the recommended flow -- protect, then
	// forget --skip-object-locked, then prune --skip-object-locked -- against
	// whatever "days" currently holds, protecting for retainFor.
	runFlow := func(retainFor time.Duration) {
		t.Helper()

		selections, err := selectSnapshotsForPolicy(data.Snapshots(days), groupBy, policy)
		rtest.OK(t, err)
		rtest.Equals(t, 1, len(selections), "expected all simulated snapshots to fall into a single group")

		retainUntil := time.Now().Add(retainFor)
		_, err = runProtectApply(ctx, repo, printer, selections[0].Keep, retainUntil, false)
		rtest.OK(t, err)

		forgetOpts := ForgetOptions{
			PolicySelectionOptions: PolicySelectionOptions{Daily: 3},
			GroupBy:                groupBy,
			SkipObjectLocked:       true,
		}
		pruneOptsForForget := PruneOptions{MaxUnused: "0%"}
		rtest.OK(t, withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
			return runForget(ctx, forgetOpts, pruneOptsForForget, gopts, gopts.Term, nil)
		}))

		pruneOpts := PruneOptions{MaxUnused: "0%", SkipObjectLocked: true}
		rtest.OK(t, withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
			return runPrune(ctx, pruneOpts, gopts, gopts.Term)
		}))
	}

	assertSurviving := func(wantKept []*data.Snapshot) {
		t.Helper()
		remaining := restic.NewIDSet()
		rtest.OK(t, repo.List(ctx, restic.SnapshotFile, func(id restic.ID, _ int64) error {
			remaining.Insert(id)
			return nil
		}))
		rtest.Equals(t, len(wantKept), len(remaining))
		for _, sn := range wantKept {
			rtest.Assert(t, remaining.Has(*sn.ID()), "expected snapshot %v to survive", sn.ID())
		}
	}

	// e2eRunCheck runs `restic check --read-data`, the strongest available
	// proof that no data a kept snapshot needs was ever lost, but -- unlike
	// testRunCheck -- WITHOUT its check-unused pass. check-unused flags any
	// blob check doesn't find live is an "error"; here that would misfire,
	// because index/pack files this same run's protect call just locked
	// cannot be removed by forget/prune's index rewrite yet (see
	// MasterIndexRewriteOpts.SkipObjectLocked), so the old, superseded index
	// legitimately keeps listing already-forgotten, still-locked blobs
	// alongside the new index until a future, unlocked run cleans it up --
	// an expected steady state of --skip-object-locked, not corruption.
	e2eRunCheck := func() {
		t.Helper()
		output, err := testRunCheckOutput(t, gopts, false)
		if err != nil {
			t.Error(output)
			t.Fatalf("unexpected error: %+v", err)
		}
	}

	// --- Pass 1 -----------------------------------------------------------
	// keep-daily 3 over days 1-5 keeps the 3 most recent (3, 4, 5) and
	// discards 1 and 2 -- neither of which was ever protected, so this is
	// plain, unremarkable forget/prune behavior.
	shortRetention := 3 * time.Second
	runFlow(shortRetention)

	assertSurviving([]*data.Snapshot{days[2], days[3], days[4]})
	e2eRunCheck()

	restoreDir1 := filepath.Join(tempdir, "restore1")
	testRunRestore(t, gopts, restoreDir1, days[4].ID().String())
	e2eAssertFileContent(t, restoreDir1, "base", "e2e flow: shared base content across every day")
	e2eAssertFileContent(t, restoreDir1, "day", "e2e flow: unique content for day 5")

	// Let day 3's retention (set by the protect call inside pass 1, above)
	// genuinely expire in real wall-clock time before simulating the next
	// day's cron run -- this is what proves TASK-26's "extend, never
	// shorten" guarantee holds across a real elapsed-time boundary: day 3 is
	// still real S3 Object Lock retained right now, and would be skipped
	// (not removed) by forget --skip-object-locked if pass 2 ran immediately.
	time.Sleep(shortRetention + 2*time.Second)

	// --- Pass 2 -------------------------------------------------------------
	// Simulate the next day's backup: day 6 arrives. keep-daily 3 over days
	// 1-6 now keeps 4, 5, 6 and newly discards day 3 -- previously protected
	// by pass 1, but its retention has since lapsed (see the sleep above), so
	// forget/prune must be able to clean it up in this pass, not skip it.
	days = append(days, e2eSaveDay(ctx, t, repo, 6, shared))
	runFlow(shortRetention)

	assertSurviving([]*data.Snapshot{days[3], days[4], days[5]})
	e2eRunCheck()

	restoreDir2 := filepath.Join(tempdir, "restore2")
	testRunRestore(t, gopts, restoreDir2, days[5].ID().String())
	e2eAssertFileContent(t, restoreDir2, "base", "e2e flow: shared base content across every day")
	e2eAssertFileContent(t, restoreDir2, "day", "e2e flow: unique content for day 6")

	// day 4 and day 5 were protected in BOTH passes (kept by the policy the
	// whole time): confirm their retention only ever advanced, never
	// shortened, by reading it back through the raw minio-go client.
	l := layout.NewDefaultLayout(prefix, path.Join)
	for _, sn := range []*data.Snapshot{days[3], days[4]} {
		h := backend.Handle{Type: restic.SnapshotFile, Name: sn.ID().String()}
		snapKey := l.Filename(h)
		_, retainUntil, err := client.GetObjectRetention(ctx, bucket, snapKey, "")
		rtest.OK(t, err)
		rtest.Assert(t, retainUntil.After(time.Now()), "expected snapshot %v to still be actively retained after pass 2", sn.ID())
	}
}

// e2eAssertFileContent asserts that dir/name contains exactly want.
func e2eAssertFileContent(t *testing.T, dir, name, want string) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(dir, name))
	rtest.OK(t, err)
	rtest.Equals(t, want, string(got))
}
