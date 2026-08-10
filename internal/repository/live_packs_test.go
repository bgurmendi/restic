package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

// TestLivePacksForSnapshots checks that LivePacksForSnapshots returns exactly the packs a
// given snapshot set needs -- not packs that are only referenced by snapshots excluded from
// that set.
func TestLivePacksForSnapshots(t *testing.T) {
	repo, _, _ := repository.TestRepositoryWithVersion(t, 2)
	ctx := context.TODO()
	printer := progress.NewNoopPrinter()

	// two snapshots with independently randomized data, so they don't fully share packs
	snA := data.TestCreateSnapshot(t, repo, time.Unix(1460289341, 0), 3)
	snB := data.TestCreateSnapshot(t, repo, time.Unix(1560289341, 0), 3)
	idA, idB := *snA.ID(), *snB.ID()

	packsA, err := repository.LivePacksForSnapshots(ctx, repo, restic.NewIDSet(idA), printer)
	rtest.OK(t, err)
	packsB, err := repository.LivePacksForSnapshots(ctx, repo, restic.NewIDSet(idB), printer)
	rtest.OK(t, err)
	packsBoth, err := repository.LivePacksForSnapshots(ctx, repo, restic.NewIDSet(idA, idB), printer)
	rtest.OK(t, err)

	rtest.Assert(t, len(packsA) > 0, "expected snapshot A to need at least one pack")
	rtest.Assert(t, len(packsB) > 0, "expected snapshot B to need at least one pack")

	// B must need at least one pack A does not -- otherwise the test below (that querying
	// with only A's ID excludes B's exclusive packs) would be vacuous.
	bOnly := false
	for id := range packsB {
		if !packsA.Has(id) {
			bOnly = true
			break
		}
	}
	rtest.Assert(t, bOnly, "expected snapshot B to need a pack not referenced by snapshot A")

	// querying with only A's snapshot ID must not pull in packs exclusively used by B, and
	// vice versa: the combined result must be exactly the union of the two individual ones.
	for id := range packsA {
		rtest.Assert(t, packsBoth.Has(id), "pack %v needed by A missing from combined set", id)
	}
	for id := range packsB {
		rtest.Assert(t, packsBoth.Has(id), "pack %v needed by B missing from combined set", id)
	}
	for id := range packsBoth {
		rtest.Assert(t, packsA.Has(id) || packsB.Has(id), "pack %v in combined set but needed by neither A nor B", id)
	}
}
