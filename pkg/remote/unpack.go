/*
 * Copyright (c) 2023. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package remote

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"golang.org/x/sys/unix"
)

func atomicWrite(target string, reader io.Reader) error {
	dir := filepath.Dir(target)

	// Create a temporary file in the same directory as target.
	// This ensures they're on the same filesystem for linkat to work.
	file, err := os.CreateTemp(dir, filepath.Base(target)+".tmp.")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	defer file.Close()

	// Get the temp file path before unlinking
	tmpPath := file.Name()

	// Immediately unlink the temp file.
	// This removes the directory entry, but the file remains accessible via the fd.
	// If the process is killed before linkat, the file is automatically cleaned up
	// by the kernel when the fd is closed (no orphaned temp files).
	if err := os.Remove(tmpPath); err != nil {
		return fmt.Errorf("unlink temp file: %w", err)
	}

	// Write content to the anonymous file (still accessible via fd)
	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("write to temp file: %w", err)
	}

	// Sync to ensure data is on disk before linking
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temp file: %w", err)
	}

	// Get the fd number for constructing /proc path
	fd := file.Fd()

	// Construct the /proc/self/fd/<fd> path for linkat.
	// This is the only way to reference an unlinked file for linking.
	procPath := fmt.Sprintf("/proc/self/fd/%d", fd)

	// Try to atomically link the anonymous file to the target path.
	// AT_SYMLINK_FOLLOW is required when linking via /proc/self/fd/.
	// linkat will fail with EEXIST if target already exists.
	err = unix.Linkat(unix.AT_FDCWD, procPath, unix.AT_FDCWD, target, unix.AT_SYMLINK_FOLLOW)
	if err == nil {
		// Success - file is now visible at target path
		return nil
	}

	if !errors.Is(err, unix.EEXIST) {
		// Unexpected error (permission denied, filesystem full, etc.)
		return fmt.Errorf("linkat to target: %w", err)
	}

	// Target already exists - another goroutine may have written it.
	// Fall back to staging + rename pattern to safely overwrite.
	defer os.Remove(tmpPath) // Best-effort cleanup (harmless if file was renamed)
	err = unix.Linkat(unix.AT_FDCWD, procPath, unix.AT_FDCWD, tmpPath, unix.AT_SYMLINK_FOLLOW)
	if err != nil {
		if errors.Is(err, unix.EEXIST) {
			// Staging file also exists - another goroutine is already handling this.
			// The target file either exists or will exist shortly. Treat as success.
			return nil
		}
		return fmt.Errorf("linkat to staging: %w", err)
	}

	// Atomically rename staging file to target (overwrites existing target)
	if err := os.Rename(tmpPath, target); err != nil {
		return fmt.Errorf("rename staging file to target: %w", err)
	}

	return nil
}

// Unpack unpacks the file named `source` in tar stream
// and write atomically into `target`
func Unpack(reader io.Reader, source, target string) error {
	// Decompress the stream (handles gzip, zstd, etc. or plain tar)
	rdr, err := compression.DecompressStream(reader)
	if err != nil {
		return err
	}
	defer rdr.Close()

	// Iterate through tar entries to find the source file
	found := false
	tr := tar.NewReader(rdr)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if hdr.Name == source {
			if err := atomicWrite(target, tr); err != nil {
				return err
			}
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("not found file %s in tar", source)
	}

	return nil
}
