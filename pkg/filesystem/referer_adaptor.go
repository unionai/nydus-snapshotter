/*
 * Copyright (c) 2023. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filesystem

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/containerd/log"

	snpkg "github.com/containerd/containerd/v2/pkg/snapshotters"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

func (fs *Filesystem) ReferrerDetectEnabled() bool {
	return fs.referrerMgr != nil
}

func (fs *Filesystem) CheckReferrer(ctx context.Context, labels map[string]string) bool {
	start := time.Now()
	logger := log.G(ctx).WithField("component", "referrer_detection")

	if !fs.ReferrerDetectEnabled() {
		return false
	}

	ref, ok := labels[snpkg.TargetRefLabel]
	if !ok {
		return false
	}

	manifestDigest := digest.Digest(labels[snpkg.TargetManifestDigestLabel])
	if manifestDigest.Validate() != nil {
		return false
	}

	logger = logger.WithFields(log.Fields{
		"ref":             ref,
		"manifest_digest": manifestDigest.String(),
	})
	logger.Debug("FS_CHECK_REFERRER_START")

	if _, err := fs.referrerMgr.CheckReferrer(ctx, ref, manifestDigest); err != nil {
		logger.WithFields(log.Fields{
			"duration_ms": time.Since(start).Milliseconds(),
			"result":      false,
		}).WithError(err).Debug("FS_CHECK_REFERRER_COMPLETE")
		return false
	}

	logger.WithFields(log.Fields{
		"duration_ms": time.Since(start).Milliseconds(),
		"result":      true,
	}).Info("FS_CHECK_REFERRER_COMPLETE")

	return true
}

func (fs *Filesystem) TryFetchMetadata(ctx context.Context, labels map[string]string, metadataPath string) error {
	start := time.Now()
	logger := log.G(ctx).WithFields(log.Fields{
		"component":     "referrer_detection",
		"phase":         "try_fetch_metadata",
		"metadata_path": metadataPath,
	})

	ref, ok := labels[snpkg.TargetRefLabel]
	if !ok {
		return fmt.Errorf("empty label %s", snpkg.TargetRefLabel)
	}

	manifestDigest := digest.Digest(labels[snpkg.TargetManifestDigestLabel])
	if err := manifestDigest.Validate(); err != nil {
		return fmt.Errorf("invalid label %s=%s", snpkg.TargetManifestDigestLabel, manifestDigest)
	}

	logger = logger.WithFields(log.Fields{
		"ref":             ref,
		"manifest_digest": manifestDigest.String(),
	})
	logger.Debug("FS_TRY_FETCH_METADATA_START")

	// Acquire a per-path mutex to serialize concurrent fetches to the same metadata file.
	// This prevents race conditions when multiple containers start simultaneously on the
	// same image with referrer detection enabled - they all find the same parent snapshot
	// and try to write to the same image.boot file.
	mutexAcquireStart := time.Now()
	logger.Debug("MUTEX_ACQUIRE_ATTEMPT")
	mu := fs.getMetadataMutex(metadataPath)
	mu.Lock()
	defer mu.Unlock()
	mutexWaitDuration := time.Since(mutexAcquireStart).Milliseconds()
	logger.WithFields(log.Fields{
		"mutex_wait_duration_ms": mutexWaitDuration,
	}).Info("MUTEX_ACQUIRED")

	// Check if metadata file already exists to avoid redundant fetches.
	// This is safe now because we hold the mutex.
	fileCheckStart := time.Now()
	if _, err := os.Stat(metadataPath); err == nil {
		logger.WithFields(log.Fields{
			"mutex_wait_duration_ms": mutexWaitDuration,
			"file_check_duration_ms": time.Since(fileCheckStart).Milliseconds(),
			"file_exists":            true,
			"total_duration_ms":      time.Since(start).Milliseconds(),
		}).Info("FS_TRY_FETCH_METADATA_SKIPPED_FILE_EXISTS")
		return nil
	}
	logger.WithFields(log.Fields{
		"file_check_duration_ms": time.Since(fileCheckStart).Milliseconds(),
		"file_exists":            false,
	}).Debug("METADATA_FILE_NOT_EXISTS_WILL_FETCH")

	fetchStart := time.Now()
	if err := fs.referrerMgr.TryFetchMetadata(ctx, ref, manifestDigest, metadataPath); err != nil {
		logger.WithFields(log.Fields{
			"mutex_wait_duration_ms": mutexWaitDuration,
			"fetch_duration_ms":      time.Since(fetchStart).Milliseconds(),
			"total_duration_ms":      time.Since(start).Milliseconds(),
		}).WithError(err).Error("FS_TRY_FETCH_METADATA_FAILED")
		return errors.Wrap(err, "try fetch metadata")
	}

	logger.WithFields(log.Fields{
		"mutex_wait_duration_ms": mutexWaitDuration,
		"fetch_duration_ms":      time.Since(fetchStart).Milliseconds(),
		"total_duration_ms":      time.Since(start).Milliseconds(),
	}).Info("FS_TRY_FETCH_METADATA_COMPLETE")

	return nil
}
