/*
 * Copyright (c) 2023. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package filesystem

func (fs *Filesystem) WithMetadataPathLock(metadataPath string, f func() error) error {
	// Acquire a per-path mutex to serialize concurrent fetches to the same metadata file.
	// This prevents race conditions when multiple containers start simultaneously on the
	// same image with referrer detection enabled - they all find the same parent snapshot
	// and try to write to the same image.boot file.
	mu := fs.getMetadataMutex(metadataPath)
	mu.Lock()
	defer mu.Unlock()

	return f()
}
