/*
 * Copyright (c) 2023. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package referrer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/containerd/nydus-snapshotter/pkg/auth"
	"github.com/containerd/nydus-snapshotter/pkg/label"
	"github.com/containerd/nydus-snapshotter/pkg/remote"

	"github.com/containerd/nydus-snapshotter/pkg/remote/remotes"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// Containerd restricts the max size of manifest index to 8M, follow it.
const maxManifestIndexSize = 0x800000
const metadataNameInLayer = "image/image.boot"

type referrer struct {
	remote              *remote.Remote
	referrerTagSuffixes []string
}

func newReferrer(keyChain *auth.PassKeyChain, insecure bool, referrerTagSuffixes []string) *referrer {
	return &referrer{
		remote:              remote.New(keyChain, insecure),
		referrerTagSuffixes: referrerTagSuffixes,
	}
}

// checkReferrer fetches the referrers and parses out the nydus
// image by specified manifest digest.
// it's using distribution list referrers API with tag-based fallback.
func (r *referrer) checkReferrer(ctx context.Context, ref string, manifestDigest digest.Digest) (*ocispec.Descriptor, error) {
	handle := func() (*ocispec.Descriptor, error) {
		// Try standard referrer API first
		desc, err := r.checkReferrerStandard(ctx, ref, manifestDigest)
		if err == nil {
			return desc, nil
		}

		// Fallback to tag-based discovery for any registry
		return r.checkReferrerTagBased(ctx, ref, manifestDigest)
	}

	desc, err := handle()
	if err != nil && r.remote.RetryWithPlainHTTP(ref, err) {
		return handle()
	}

	return desc, err
}

// checkReferrerStandard uses the standard OCI referrer API
func (r *referrer) checkReferrerStandard(ctx context.Context, ref string, manifestDigest digest.Digest) (*ocispec.Descriptor, error) {
	// Create an new resolver to request.
	fetcher, err := r.remote.Fetcher(ctx, ref)
	if err != nil {
		return nil, errors.Wrap(err, "get fetcher")
	}

	// Fetch image referrers from remote registry.
	rc, _, err := fetcher.(remotes.ReferrersFetcher).FetchReferrers(ctx, manifestDigest)
	if err != nil {
		return nil, errors.Wrap(err, "fetch referrers")
	}
	defer rc.Close()

	// Parse image manifest list from referrers.
	var index ocispec.Index
	bytes, err := io.ReadAll(io.LimitReader(rc, maxManifestIndexSize))
	if err != nil {
		return nil, errors.Wrap(err, "read referrers")
	}
	if err := json.Unmarshal(bytes, &index); err != nil {
		return nil, errors.Wrap(err, "unmarshal referrers index")
	}
	if len(index.Manifests) == 0 {
		return nil, fmt.Errorf("empty referrer list")
	}

	// Prefer to fetch the last manifest and check if it is a nydus image.
	// TODO: should we search by matching ArtifactType?
	rc, err = fetcher.Fetch(ctx, index.Manifests[0])
	if err != nil {
		return nil, errors.Wrap(err, "fetch referrers")
	}
	defer rc.Close()

	var manifest ocispec.Manifest
	bytes, err = io.ReadAll(rc)
	if err != nil {
		return nil, errors.Wrap(err, "read manifest")
	}
	if err := json.Unmarshal(bytes, &manifest); err != nil {
		return nil, errors.Wrap(err, "unmarshal manifest")
	}
	if len(manifest.Layers) < 1 {
		return nil, fmt.Errorf("invalid manifest")
	}
	metaLayer := manifest.Layers[len(manifest.Layers)-1]
	if !label.IsNydusMetaLayer(metaLayer.Annotations) {
		return nil, fmt.Errorf("invalid nydus manifest")
	}

	return &metaLayer, nil
}

// checkReferrerTagBased implements tag-based referrer discovery for any registry
func (r *referrer) checkReferrerTagBased(ctx context.Context, ref string, manifestDigest digest.Digest) (*ocispec.Descriptor, error) {
	// Extract tag from reference
	parts := strings.Split(ref, ":")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid reference format")
	}

	baseTag := parts[len(parts)-1]
	baseRef := strings.Join(parts[:len(parts)-1], ":")

	// Try each configured tag suffix in priority order
	for _, suffix := range r.referrerTagSuffixes {
		candidateRef := baseRef + ":" + baseTag + suffix
		desc, err := r.validateTagBasedReferrer(ctx, candidateRef, manifestDigest)
		if err == nil && desc != nil {
			return desc, nil
		}
	}

	return nil, fmt.Errorf("no tag-based referrer found")
}

// validateTagBasedReferrer checks if a candidate reference is a valid nydus referrer
func (r *referrer) validateTagBasedReferrer(ctx context.Context, candidateRef string, expectedSubject digest.Digest) (*ocispec.Descriptor, error) {
	// Resolve the candidate reference
	resolver := r.remote.Resolve(ctx, candidateRef)
	_, desc, err := resolver.Resolve(ctx, candidateRef)
	if err != nil {
		return nil, errors.Wrap(err, "resolve reference")
	}

	// Fetch the manifest
	fetcher, err := resolver.Fetcher(ctx, candidateRef)
	if err != nil {
		return nil, errors.Wrap(err, "get fetcher")
	}

	rc, err := fetcher.Fetch(ctx, desc)
	if err != nil {
		return nil, errors.Wrap(err, "fetch manifest")
	}
	defer rc.Close()

	// Parse manifest
	var manifest ocispec.Manifest
	bytes, err := io.ReadAll(rc)
	if err != nil {
		return nil, errors.Wrap(err, "read manifest")
	}

	if err := json.Unmarshal(bytes, &manifest); err != nil {
		return nil, errors.Wrap(err, "unmarshal manifest")
	}

	// Check if this manifest references the expected subject
	if manifest.Subject == nil || manifest.Subject.Digest != expectedSubject {
		return nil, fmt.Errorf("not a referrer for expected subject")
	}

	// Check if it's a nydus manifest
	if len(manifest.Layers) < 1 {
		return nil, fmt.Errorf("invalid manifest")
	}

	metaLayer := manifest.Layers[len(manifest.Layers)-1]
	if !label.IsNydusMetaLayer(metaLayer.Annotations) {
		return nil, fmt.Errorf("not a nydus manifest")
	}

	return &metaLayer, nil
}

// fetchMetadata fetches and unpacks nydus metadata file to specified path.
func (r *referrer) fetchMetadata(ctx context.Context, ref string, desc ocispec.Descriptor, metadataPath string) error {
	handle := func() error {
		// Create an new resolver to request.
		resolver := r.remote.Resolve(ctx, ref)
		fetcher, err := resolver.Fetcher(ctx, ref)
		if err != nil {
			return errors.Wrap(err, "get fetcher")
		}

		// Unpack nydus metadata file to specified path.
		rc, err := fetcher.Fetch(ctx, desc)
		if err != nil {
			return errors.Wrap(err, "fetch nydus metadata")
		}
		defer rc.Close()

		if err := remote.Unpack(rc, metadataNameInLayer, metadataPath); err != nil {
			os.Remove(metadataPath)
			return errors.Wrap(err, "unpack metadata from layer")
		}

		return nil
	}

	// TODO: check metafile already exists
	err := handle()
	if err != nil && r.remote.RetryWithPlainHTTP(ref, err) {
		return handle()
	}

	return err
}
