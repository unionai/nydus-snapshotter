/*
 * Copyright (c) 2023. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package remote

import (
	"archive/tar"
	stderrors "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

func atomicWrite(target string, reader io.Reader) error {
	dir := filepath.Dir(target)

	// Create an anonymous file in the target directory.
	// O_TMPFILE creates an unnamed inode that's never visible in the filesystem
	// until we explicitly link it. If the process crashes, the kernel automatically
	// cleans up the inode (no orphaned temp files).
	fd, err := unix.Open(dir, unix.O_TMPFILE|unix.O_CLOEXEC|unix.O_RDWR, 0644)
	if err != nil {
		return errors.Wrapf(err, "open temp file in %s", dir)
	}

	file := os.NewFile(uintptr(fd), "")
	defer file.Close()

	// Write content to the anonymous file
	if _, err := io.Copy(file, reader); err != nil {
		return errors.Wrapf(err, "write to temp file for %s", target)
	}

	// Sync to ensure data is on disk before linking
	if err := file.Sync(); err != nil {
		return errors.Wrapf(err, "sync temp file for %s", target)
	}

	// Atomically link the anonymous file to the target path.
	// linkat will fail with EEXIST if target already exists.
	err = unix.Linkat(fd, "", unix.AT_FDCWD, target, unix.AT_EMPTY_PATH)
	if err == nil {
		return nil
	}

	if !stderrors.Is(err, unix.EEXIST) {
		return errors.Wrapf(err, "linkat to target %s", target)
	}

	// Target already exists
	// Fall back to staging to a unique temporary file + rename pattern to safely overwrite.
	tmpPath := fmt.Sprintf("%s.tmp.%d-%d-%d", target, os.Getpid(), fd, time.Now().UnixNano())
	defer os.Remove(tmpPath)

	// Write to a temporary file first
	if err := unix.Linkat(fd, "", unix.AT_FDCWD, tmpPath, unix.AT_EMPTY_PATH); err != nil {
		return errors.Wrapf(err, "linkat to staging file for %s", target)
	}

	// Atomically replace the target with the temporary file
	if err := os.Rename(tmpPath, target); err != nil {
		return errors.Wrapf(err, "rename staging file %s to target %s", tmpPath, target)
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
