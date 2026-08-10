package repository

import (
	"context"
	"fmt"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository/index"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui/progress"
)

// LivePacksForSnapshots returns the set of packs required to restore exactly the given
// snapshots. It is the read-only counterpart of PlanPrune's live-pack computation: it calls
// the same underlying primitives PlanPrune uses -- data.FindUsedBlobs (which is what
// PlanPrune's own getUsedBlobs callback is built on) and packInfoFromIndex -- but stops at
// "which packs does this set of snapshots need". It performs no deletion planning, computes
// no "dead" packs, and does not touch PlanPrune's own deletion-planning state
// (packInfoWithID, decidePackAction, PruneStats bookkeeping, target-packsize logic), which a
// caller only interested in liveness (such as protect) has no use for.
//
// Unlike PlanPrune's getUsedBlobs callback -- which walks every snapshot in the repository
// except an explicit ignore set -- snapshotIDs here is an explicit inclusion set: only the
// given snapshots' trees are loaded and traversed.
func LivePacksForSnapshots(ctx context.Context, repo *Repository, snapshotIDs restic.IDSet, printer progress.Printer) (restic.IDSet, error) {
	snapshotTrees := make(restic.IDs, 0, len(snapshotIDs))
	for id := range snapshotIDs {
		sn, err := data.LoadSnapshot(ctx, repo, id)
		if err != nil {
			return nil, fmt.Errorf("failed loading snapshot %v: %w", id, err)
		}
		snapshotTrees = append(snapshotTrees, *sn.Tree)
	}

	usedBlobs := index.NewAssociatedSet[uint8](repo.idx)
	bar := printer.NewCounter("snapshots")
	bar.SetMax(uint64(len(snapshotTrees)))
	defer bar.Done()
	if err := data.FindUsedBlobs(ctx, repo, snapshotTrees, usedBlobs, bar); err != nil {
		return nil, fmt.Errorf("failed finding blobs: %w", err)
	}

	// packInfoFromIndex reports on every pack in the index, not just the ones the given
	// snapshots use -- the same as it does for PlanPrune. Only stats is discarded here;
	// PlanPrune's callers care about it, liveness callers don't.
	stats := PruneStats{}
	_, indexPack, err := packInfoFromIndex(ctx, repo, usedBlobs, &stats, printer)
	if err != nil {
		return nil, err
	}

	livePacks := restic.NewIDSet()
	for id, info := range indexPack {
		if info.usedBlobs > 0 {
			livePacks.Insert(id)
		}
	}
	return livePacks, nil
}
