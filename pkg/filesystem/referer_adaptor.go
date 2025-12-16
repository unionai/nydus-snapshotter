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

	"github.com/containerd/log"

	snpkg "github.com/containerd/containerd/v2/pkg/snapshotters"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
)

func (fs *Filesystem) ReferrerDetectEnabled() bool {
	return fs.referrerMgr != nil
}

func (fs *Filesystem) CheckReferrer(ctx context.Context, labels map[string]string) bool {
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

	if _, err := fs.referrerMgr.CheckReferrer(ctx, ref, manifestDigest); err != nil {
		return false
	}

	return true
}

func (fs *Filesystem) TryFetchMetadata(ctx context.Context, labels map[string]string, metadataPath string) error {
	ref, ok := labels[snpkg.TargetRefLabel]
	if !ok {
		return fmt.Errorf("empty label %s", snpkg.TargetRefLabel)
	}

	manifestDigest := digest.Digest(labels[snpkg.TargetManifestDigestLabel])
	if err := manifestDigest.Validate(); err != nil {
		return fmt.Errorf("invalid label %s=%s", snpkg.TargetManifestDigestLabel, manifestDigest)
	}

	// Acquire a per-path mutex to serialize concurrent fetches to the same metadata file.
	// This prevents race conditions when multiple containers start simultaneously on the
	// same image with referrer detection enabled - they all find the same parent snapshot
	// and try to write to the same image.boot file.
	mu := fs.getMetadataMutex(metadataPath)
	mu.Lock()
	defer mu.Unlock()

	// Check if metadata file already exists to avoid redundant fetches.
	// This is safe now because we hold the mutex.
	if _, err := os.Stat(metadataPath); err == nil {
		log.L.Debugf("Metadata file %s already exists, skipping fetch", metadataPath)
		return nil
	}

	if err := fs.referrerMgr.TryFetchMetadata(ctx, ref, manifestDigest, metadataPath); err != nil {
		return errors.Wrap(err, "try fetch metadata")
	}

	return nil
}
