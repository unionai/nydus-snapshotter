/*
 * Copyright (c) 2020. Ant Group. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package snapshot

import (
	"context"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/core/snapshots/storage"
	"github.com/containerd/log"
	"github.com/containerd/nydus-snapshotter/pkg/errdefs"
	"github.com/pkg/errors"
)

type WalkFunc = func(id string, info snapshots.Info) bool

func GetSnapshotInfo(ctx context.Context, ms *storage.MetaStore, key string) (string, snapshots.Info, snapshots.Usage, error) {
	ctx, t, err := ms.TransactionContext(ctx, false)
	if err != nil {
		return "", snapshots.Info{}, snapshots.Usage{}, err
	}

	defer func() {
		if err := t.Rollback(); err != nil {
			log.L.WithError(err).Errorf("Rollback traction %s", key)
		}
	}()

	id, info, usage, err := storage.GetInfo(ctx, key)
	if err != nil {
		return "", snapshots.Info{}, snapshots.Usage{}, err
	}

	return id, info, usage, nil
}

func GetSnapshot(ctx context.Context, ms *storage.MetaStore, key string) (*storage.Snapshot, error) {
	ctx, t, err := ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := t.Rollback(); err != nil {
			log.L.WithError(err).Errorf("Rollback traction %s", key)
		}
	}()

	s, err := storage.GetSnapshot(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "get snapshot")
	}

	return &s, nil
}

// Iterate all the parents of a  snapshot specified by `key`
// Stop the iteration once callback `fn` is invoked successfully and return current iterated snapshot
func IterateParentSnapshots(ctx context.Context, ms *storage.MetaStore, key string, fn WalkFunc) (string, snapshots.Info, error) {
	ctx, t, err := ms.TransactionContext(ctx, false)
	if err != nil {
		return "", snapshots.Info{}, err
	}

	defer func() {
		if err := t.Rollback(); err != nil {
			log.L.WithError(err).Errorf("Rollback transaction %s", key)
		}
	}()

	iterationCount := 0
	log.L.WithFields(log.Fields{
		"call_site":    "IterateParentSnapshots",
		"snapshot_key": key,
	}).Debug("ITERATE_PARENTS_START")

	for cKey := key; cKey != ""; {
		iterationCount++
		id, info, _, err := storage.GetInfo(ctx, cKey)
		if err != nil {
			log.L.WithError(err).Warnf("failed to get snapshot info of %q", cKey)
			return "", snapshots.Info{}, err
		}

		log.L.WithFields(log.Fields{
			"call_site":      "IterateParentSnapshots",
			"snapshot_key":   key,
			"iteration":      iterationCount,
			"current_key":    cKey,
			"current_id":     id,
			"current_parent": info.Parent,
			"info_kind":      info.Kind.String(),
		}).Debug("ITERATE_PARENTS_VISITING")

		if fn(id, info) {
			log.L.WithFields(log.Fields{
				"call_site":        "IterateParentSnapshots",
				"snapshot_key":     key,
				"total_iterations": iterationCount,
				"found_id":         id,
			}).Debug("ITERATE_PARENTS_FOUND")
			return id, info, nil
		}

		cKey = info.Parent
	}

	log.L.WithFields(log.Fields{
		"call_site":        "IterateParentSnapshots",
		"snapshot_key":     key,
		"total_iterations": iterationCount,
	}).Debug("ITERATE_PARENTS_NOT_FOUND")

	return "", snapshots.Info{}, errdefs.ErrNotFound
}

func UpdateSnapshotInfo(ctx context.Context, ms *storage.MetaStore, info snapshots.Info, fieldPaths ...string) (snapshots.Info, error) {
	ctx, t, err := ms.TransactionContext(ctx, true)
	if err != nil {
		return snapshots.Info{}, err
	}
	info, err = storage.UpdateInfo(ctx, info, fieldPaths...)
	if err != nil {
		if rerr := t.Rollback(); rerr != nil {
			log.L.WithError(rerr).Errorf("update snapshot info")
		}
		return snapshots.Info{}, err
	}
	if err := t.Commit(); err != nil {
		return snapshots.Info{}, err
	}
	return info, nil
}
