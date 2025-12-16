/*
 * Copyright (c) 2023. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package remote

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/v2/pkg/archive/compression"
)

// atomicWrite writes data from reader to target using atomic write pattern.
// It writes to a temporary file first, then atomically renames it to target.
func atomicWrite(target string, reader io.Reader) (err error) {
	dir := filepath.Dir(target)
	file, err := os.CreateTemp(dir, filepath.Base(target)+".tmp.")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := file.Name()

	defer func() {
		if err != nil {
			os.Remove(tmpPath)
		}
	}()

	if _, err = io.Copy(file, reader); err != nil {
		file.Close()
		return fmt.Errorf("write to temp file: %w", err)
	}

	if err = file.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	if err = os.Rename(tmpPath, target); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}

	return nil
}

// Unpack unpacks the file named `source` in tar stream
// and write into `target` path using atomic write pattern.
func Unpack(reader io.Reader, source, target string) error {
	rdr, err := compression.DecompressStream(reader)
	if err != nil {
		return err
	}
	defer rdr.Close()

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
