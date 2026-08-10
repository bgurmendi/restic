package repository

import (
	"context"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/repository/index"
	"github.com/restic/restic/internal/repository/pack"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui/progress"
)

type RepairIndexOptions struct {
	ReadAllPacks bool
}

func RepairIndex(ctx context.Context, repo *Repository, opts RepairIndexOptions, printer progress.Printer) error {
	var obsoleteIndexes restic.IDs
	packSizeFromList := make(map[restic.ID]int64)
	packSizeFromIndex := make(map[restic.ID]int64)
	removePacks := restic.NewIDSet()

	if opts.ReadAllPacks {
		// get list of old index files but start with empty index
		err := repo.List(ctx, restic.IndexFile, func(id restic.ID, _ int64) error {
			obsoleteIndexes = append(obsoleteIndexes, id)
			return nil
		})
		if err != nil {
			return err
		}
		repo.clearIndex()

	} else {
		printer.P("loading indexes...\n")
		err := repo.loadIndexWithCallback(ctx, restic.NoopTerminalCounterFactory, func(id restic.ID, _ *index.Index, err error) error {
			if err != nil {
				printer.E("removing invalid index %v: %v\n", id, err)
				obsoleteIndexes = append(obsoleteIndexes, id)
				return nil
			}
			return nil
		})
		if err != nil {
			return err
		}

		packSizeFromIndex, err = pack.Size(ctx, repo, false)
		if err != nil {
			return err
		}
	}

	oldIndexes := repo.idx.IDs()

	printer.P("getting pack files to read...\n")
	err := repo.List(ctx, restic.PackFile, func(id restic.ID, packSize int64) error {
		size, ok := packSizeFromIndex[id]
		if !ok || size != packSize {
			// Pack was not referenced in index or size does not match
			packSizeFromList[id] = packSize
			removePacks.Insert(id)
		}
		if !ok {
			printer.E("adding pack file to index %v\n", id)
		} else if size != packSize {
			printer.E("reindexing pack file %v with unexpected size %v instead of %v\n", id, packSize, size)
		}
		delete(packSizeFromIndex, id)
		return nil
	})
	if err != nil {
		return err
	}
	for id := range packSizeFromIndex {
		// forget pack files that are referenced in the index but do not exist
		// when rebuilding the index
		removePacks.Insert(id)
		printer.E("removing not found pack file %v\n", id)
	}

	if len(packSizeFromList) > 0 {
		printer.P("reading pack files\n")
		bar := printer.NewCounter("packs")
		bar.SetMax(uint64(len(packSizeFromList)))
		invalidFiles, err := repo.createIndexFromPacks(ctx, packSizeFromList, bar)
		bar.Done()
		if err != nil {
			return err
		}

		for _, id := range invalidFiles {
			printer.V("skipped incomplete pack file: %v\n", id)
		}
	}

	// repair index has no --skip-object-locked concept of its own: any
	// locked-delete failure it hits stays fatal, exactly as before.
	_, err = rewriteIndexFiles(ctx, repo, removePacks, oldIndexes, obsoleteIndexes, false, printer)
	if err != nil {
		return err
	}

	// drop outdated in-memory index
	repo.clearIndex()
	return nil
}

// rewriteIndexFiles rebuilds the index without removePacks and deletes the
// now-obsolete old index files. When skipObjectLocked is true (prune's
// --skip-object-locked, see PrunePlan.Execute), a locked-delete failure on
// an old index file is tolerated rather than fatal: renewing Object Lock
// retention on index/* (restic protect) locks whatever index files exist at
// the time, so a routine, same-day rewrite triggered by forget removing a
// snapshot must not turn into a hard prune failure just because the
// superseded old index file is still under active retention. The returned
// bool reports whether that happened at least once; see
// index.MasterIndex.Rewrite's docstring for why the caller must then treat
// removePacks as not yet safe to physically delete.
func rewriteIndexFiles(ctx context.Context, repo *Repository, removePacks restic.IDSet, oldIndexes restic.IDSet, extraObsolete restic.IDs, skipObjectLocked bool, printer progress.Printer) (bool, error) {
	printer.P("rebuilding index\n")

	bar := printer.NewCounter("indexes processed")
	var skippedObjectLocked bool
	err := repo.idx.Rewrite(ctx, &internalRepository{repo}, removePacks, oldIndexes, extraObsolete, index.MasterIndexRewriteOpts{
		SaveProgress: bar,
		DeleteProgress: func() restic.Counter {
			return printer.NewCounter("old indexes deleted")
		},
		DeleteReport: func(id restic.ID, err error) {
			switch {
			case err == nil:
				printer.VV("removed index %v\n", id.String())
			case skipObjectLocked && errors.Is(err, backend.ErrObjectLocked):
				skippedObjectLocked = true
				printer.VV("index %v is still object-locked, skipping\n", id.String())
			default:
				printer.VV("failed to remove index %v: %v\n", id.String(), err)
			}
		},
		SkipObjectLocked: skipObjectLocked,
	})
	return skippedObjectLocked, err
}
