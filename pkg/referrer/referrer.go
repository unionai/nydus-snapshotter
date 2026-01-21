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
	"time"

	"github.com/containerd/containerd/v2/pkg/reference"
	"github.com/containerd/log"
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
const digestDelimiter = "@"

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
	start := time.Now()
	logger := log.G(ctx).WithFields(log.Fields{
		"component":       "referrer_detection",
		"phase":           "check_referrer",
		"ref":             ref,
		"manifest_digest": manifestDigest.String(),
	})
	logger.Debug("CHECK_REFERRER_START")

	attempt := 0
	handle := func() (*ocispec.Descriptor, error) {
		attempt++
		handleStart := time.Now()
		logger.WithField("attempt", attempt).Debug("CHECK_REFERRER_ATTEMPT_START")

		// Try standard referrer API first
		standardStart := time.Now()
		desc, err := r.checkReferrerStandard(ctx, ref, manifestDigest)
		standardDuration := time.Since(standardStart).Milliseconds()

		if err == nil {
			logger.WithFields(log.Fields{
				"attempt":              attempt,
				"method":               "standard_api",
				"standard_duration_ms": standardDuration,
				"handle_duration_ms":   time.Since(handleStart).Milliseconds(),
				"meta_layer_digest":    desc.Digest.String(),
			}).Info("CHECK_REFERRER_STANDARD_API_SUCCESS")
			return desc, nil
		}

		logger.WithFields(log.Fields{
			"attempt":              attempt,
			"standard_duration_ms": standardDuration,
			"standard_error":       err.Error(),
		}).Debug("CHECK_REFERRER_STANDARD_API_FAILED_TRYING_TAG_BASED")

		// Fallback to tag-based discovery for any registry
		tagBasedStart := time.Now()
		desc, err = r.checkReferrerTagBased(ctx, ref, manifestDigest)
		tagBasedDuration := time.Since(tagBasedStart).Milliseconds()

		if err == nil {
			logger.WithFields(log.Fields{
				"attempt":               attempt,
				"method":                "tag_based",
				"standard_duration_ms":  standardDuration,
				"tag_based_duration_ms": tagBasedDuration,
				"handle_duration_ms":    time.Since(handleStart).Milliseconds(),
				"meta_layer_digest":     desc.Digest.String(),
			}).Info("CHECK_REFERRER_TAG_BASED_SUCCESS")
			return desc, nil
		}

		logger.WithFields(log.Fields{
			"attempt":               attempt,
			"standard_duration_ms":  standardDuration,
			"tag_based_duration_ms": tagBasedDuration,
			"handle_duration_ms":    time.Since(handleStart).Milliseconds(),
			"tag_based_error":       err.Error(),
		}).Debug("CHECK_REFERRER_TAG_BASED_FAILED")

		return nil, err
	}

	desc, err := handle()
	if err != nil && r.remote.RetryWithPlainHTTP(ref, err) {
		logger.WithFields(log.Fields{
			"reason": "retry_with_plain_http",
		}).Warn("CHECK_REFERRER_RETRYING_WITH_PLAIN_HTTP")
		desc, err = handle()
	}

	if err != nil {
		logger.WithFields(log.Fields{
			"total_duration_ms": time.Since(start).Milliseconds(),
			"total_attempts":    attempt,
		}).WithError(err).Debug("CHECK_REFERRER_COMPLETE_ERROR")
	} else {
		logger.WithFields(log.Fields{
			"total_duration_ms": time.Since(start).Milliseconds(),
			"total_attempts":    attempt,
		}).Debug("CHECK_REFERRER_COMPLETE")
	}

	return desc, err
}

// checkReferrerStandard uses the standard OCI referrer API
func (r *referrer) checkReferrerStandard(ctx context.Context, ref string, manifestDigest digest.Digest) (*ocispec.Descriptor, error) {
	start := time.Now()
	logger := log.G(ctx).WithFields(log.Fields{
		"component":       "referrer_detection",
		"phase":           "standard_api",
		"ref":             ref,
		"manifest_digest": manifestDigest.String(),
	})
	logger.Debug("STANDARD_API_START")

	// Create an new resolver to request.
	fetcherStart := time.Now()
	fetcher, err := r.remote.Fetcher(ctx, ref)
	fetcherDuration := time.Since(fetcherStart).Milliseconds()
	if err != nil {
		logger.WithFields(log.Fields{
			"fetcher_duration_ms": fetcherDuration,
		}).WithError(err).Debug("STANDARD_API_FETCHER_CREATE_FAILED")
		return nil, errors.Wrap(err, "get fetcher")
	}
	logger.WithFields(log.Fields{
		"fetcher_duration_ms": fetcherDuration,
	}).Debug("STANDARD_API_FETCHER_CREATED")

	// Fetch image referrers from remote registry.
	fetchReferrersStart := time.Now()
	rc, _, err := fetcher.(remotes.ReferrersFetcher).FetchReferrers(ctx, manifestDigest)
	fetchReferrersDuration := time.Since(fetchReferrersStart).Milliseconds()
	if err != nil {
		logger.WithFields(log.Fields{
			"fetcher_duration_ms":         fetcherDuration,
			"fetch_referrers_duration_ms": fetchReferrersDuration,
			"total_duration_ms":           time.Since(start).Milliseconds(),
		}).WithError(err).Debug("STANDARD_API_FETCH_REFERRERS_FAILED")
		return nil, errors.Wrap(err, "fetch referrers")
	}
	defer rc.Close()
	logger.WithFields(log.Fields{
		"fetch_referrers_duration_ms": fetchReferrersDuration,
	}).Debug("STANDARD_API_FETCH_REFERRERS_SUCCESS")

	// Parse image manifest list from referrers.
	readStart := time.Now()
	var index ocispec.Index
	bytes, err := io.ReadAll(io.LimitReader(rc, maxManifestIndexSize))
	readDuration := time.Since(readStart).Milliseconds()
	if err != nil {
		return nil, errors.Wrap(err, "read referrers")
	}
	if err := json.Unmarshal(bytes, &index); err != nil {
		return nil, errors.Wrap(err, "unmarshal referrers index")
	}
	logger.WithFields(log.Fields{
		"read_duration_ms": readDuration,
		"bytes_read":       len(bytes),
		"manifest_count":   len(index.Manifests),
	}).Debug("STANDARD_API_REFERRERS_INDEX_PARSED")

	if len(index.Manifests) == 0 {
		logger.WithFields(log.Fields{
			"total_duration_ms": time.Since(start).Milliseconds(),
		}).Debug("STANDARD_API_EMPTY_REFERRER_LIST")
		return nil, fmt.Errorf("empty referrer list")
	}

	// Prefer to fetch the last manifest and check if it is a nydus image.
	// TODO: should we search by matching ArtifactType?
	fetchManifestStart := time.Now()
	rc, err = fetcher.Fetch(ctx, index.Manifests[0])
	fetchManifestDuration := time.Since(fetchManifestStart).Milliseconds()
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
	logger.WithFields(log.Fields{
		"fetch_manifest_duration_ms": fetchManifestDuration,
		"manifest_layers":            len(manifest.Layers),
	}).Debug("STANDARD_API_MANIFEST_FETCHED")

	if len(manifest.Layers) < 1 {
		return nil, fmt.Errorf("invalid manifest")
	}
	metaLayer := manifest.Layers[len(manifest.Layers)-1]
	if !label.IsNydusMetaLayer(metaLayer.Annotations) {
		return nil, fmt.Errorf("invalid nydus manifest")
	}

	logger.WithFields(log.Fields{
		"fetcher_duration_ms":         fetcherDuration,
		"fetch_referrers_duration_ms": fetchReferrersDuration,
		"fetch_manifest_duration_ms":  fetchManifestDuration,
		"total_duration_ms":           time.Since(start).Milliseconds(),
		"meta_layer_digest":           metaLayer.Digest.String(),
		"meta_layer_size":             metaLayer.Size,
	}).Info("STANDARD_API_COMPLETE")

	return &metaLayer, nil
}

// checkReferrerTagBased implements tag-based referrer discovery for any registry
func (r *referrer) checkReferrerTagBased(ctx context.Context, ref string, manifestDigest digest.Digest) (*ocispec.Descriptor, error) {
	start := time.Now()
	logger := log.G(ctx).WithFields(log.Fields{
		"component":       "referrer_detection",
		"phase":           "tag_based",
		"ref":             ref,
		"manifest_digest": manifestDigest.String(),
	})
	logger.Debug("TAG_BASED_START")

	// Generate candidate references using robust parsing
	candidates, err := r.generateReferrerCandidates(ref, r.referrerTagSuffixes)
	if err != nil {
		logger.WithError(err).Debug("TAG_BASED_CANDIDATE_GENERATION_FAILED")
		return nil, fmt.Errorf("failed to generate referrer candidates: %w", err)
	}
	logger.WithFields(log.Fields{
		"candidate_count": len(candidates),
		"candidates":      candidates,
	}).Debug("TAG_BASED_CANDIDATES_GENERATED")

	// Try each candidate in priority order
	for i, candidateRef := range candidates {
		candidateStart := time.Now()
		logger.WithFields(log.Fields{
			"candidate_index": i,
			"candidate_ref":   candidateRef,
		}).Debug("TAG_BASED_TRYING_CANDIDATE")

		desc, err := r.validateTagBasedReferrer(ctx, candidateRef, manifestDigest)
		candidateDuration := time.Since(candidateStart).Milliseconds()

		if err == nil && desc != nil {
			logger.WithFields(log.Fields{
				"candidate_index":        i,
				"candidate_ref":          candidateRef,
				"candidate_duration_ms":  candidateDuration,
				"total_duration_ms":      time.Since(start).Milliseconds(),
				"meta_layer_digest":      desc.Digest.String(),
				"meta_layer_size":        desc.Size,
			}).Info("TAG_BASED_CANDIDATE_SUCCESS")
			return desc, nil
		}

		logger.WithFields(log.Fields{
			"candidate_index":       i,
			"candidate_ref":         candidateRef,
			"candidate_duration_ms": candidateDuration,
			"error":                 err.Error(),
		}).Debug("TAG_BASED_CANDIDATE_FAILED")
	}

	logger.WithFields(log.Fields{
		"total_duration_ms":   time.Since(start).Milliseconds(),
		"candidates_tried":    len(candidates),
	}).Debug("TAG_BASED_ALL_CANDIDATES_FAILED")

	return nil, fmt.Errorf("no tag-based referrer found")
}

// validateTagBasedReferrer checks if a candidate reference is a valid nydus referrer
func (r *referrer) validateTagBasedReferrer(ctx context.Context, candidateRef string, expectedSubject digest.Digest) (*ocispec.Descriptor, error) {
	start := time.Now()
	logger := log.G(ctx).WithFields(log.Fields{
		"component":        "referrer_detection",
		"phase":            "validate_tag_based",
		"candidate_ref":    candidateRef,
		"expected_subject": expectedSubject.String(),
	})

	// Resolve the candidate reference
	resolveStart := time.Now()
	resolver := r.remote.Resolve(ctx, candidateRef)
	_, desc, err := resolver.Resolve(ctx, candidateRef)
	resolveDuration := time.Since(resolveStart).Milliseconds()
	if err != nil {
		logger.WithFields(log.Fields{
			"resolve_duration_ms": resolveDuration,
		}).WithError(err).Debug("VALIDATE_TAG_RESOLVE_FAILED")
		return nil, errors.Wrap(err, "resolve reference")
	}
	logger.WithFields(log.Fields{
		"resolve_duration_ms": resolveDuration,
		"resolved_digest":     desc.Digest.String(),
	}).Debug("VALIDATE_TAG_RESOLVED")

	// Fetch the manifest
	fetcherStart := time.Now()
	fetcher, err := resolver.Fetcher(ctx, candidateRef)
	fetcherDuration := time.Since(fetcherStart).Milliseconds()
	if err != nil {
		return nil, errors.Wrap(err, "get fetcher")
	}

	fetchStart := time.Now()
	rc, err := fetcher.Fetch(ctx, desc)
	fetchDuration := time.Since(fetchStart).Milliseconds()
	if err != nil {
		logger.WithFields(log.Fields{
			"resolve_duration_ms": resolveDuration,
			"fetcher_duration_ms": fetcherDuration,
			"fetch_duration_ms":   fetchDuration,
		}).WithError(err).Debug("VALIDATE_TAG_FETCH_FAILED")
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
		logger.WithFields(log.Fields{
			"has_subject":     manifest.Subject != nil,
			"subject_matches": manifest.Subject != nil && manifest.Subject.Digest == expectedSubject,
		}).Debug("VALIDATE_TAG_SUBJECT_MISMATCH")
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

	logger.WithFields(log.Fields{
		"resolve_duration_ms": resolveDuration,
		"fetcher_duration_ms": fetcherDuration,
		"fetch_duration_ms":   fetchDuration,
		"total_duration_ms":   time.Since(start).Milliseconds(),
		"meta_layer_digest":   metaLayer.Digest.String(),
	}).Debug("VALIDATE_TAG_SUCCESS")

	return &metaLayer, nil
}

// generateReferrerCandidates generates candidate referrer references using robust parsing
func (r *referrer) generateReferrerCandidates(ref string, suffixes []string) ([]string, error) {
	refspec, err := reference.Parse(ref)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	// Get the base reference (without digest)
	baseRef := refspec.Locator

	// Parse the tag from the object field
	tag, err := r.parseTagFromReference(refspec)
	if err != nil {
		return nil, err
	}

	// Generate candidate references
	candidates := make([]string, 0, len(suffixes))
	for _, suffix := range suffixes {
		candidate := baseRef + ":" + tag + suffix
		candidates = append(candidates, candidate)
	}

	return candidates, nil
}

// parseTagFromReference extracts the tag from a reference specification
// This leverages the existing Digest() method and works with the Object field structure
func (r *referrer) parseTagFromReference(refspec reference.Spec) (string, error) {
	// If Object is empty, default to "latest"
	if refspec.Object == "" {
		return "latest", nil
	}

	// If Object starts with @, it's digest-only
	if strings.HasPrefix(refspec.Object, digestDelimiter) {
		return "", fmt.Errorf("digest-only reference cannot be used for tag-based discovery")
	}

	// Check if there's a digest part using the built-in method
	if digest := refspec.Digest(); digest != "" {
		// Object format is "tag@digest", extract the tag part
		tagPart := strings.TrimSuffix(refspec.Object, digestDelimiter+digest.String())
		if tagPart == "" {
			return "", fmt.Errorf("invalid reference format: empty tag with digest")
		}
		return tagPart, nil
	}

	// Object is just a tag
	return refspec.Object, nil
}

// fetchMetadata fetches and unpacks nydus metadata file to specified path.
func (r *referrer) fetchMetadata(ctx context.Context, ref string, desc ocispec.Descriptor, metadataPath string) error {
	start := time.Now()
	logger := log.G(ctx).WithFields(log.Fields{
		"component":       "referrer_detection",
		"phase":           "fetch_metadata",
		"ref":             ref,
		"descriptor":      desc.Digest.String(),
		"descriptor_size": desc.Size,
		"metadata_path":   metadataPath,
	})
	logger.Debug("FETCH_METADATA_BLOB_START")

	attempt := 0
	handle := func() error {
		attempt++
		handleStart := time.Now()

		// Create an new resolver to request.
		fetcherStart := time.Now()
		resolver := r.remote.Resolve(ctx, ref)
		fetcher, err := resolver.Fetcher(ctx, ref)
		fetcherDuration := time.Since(fetcherStart).Milliseconds()
		if err != nil {
			logger.WithFields(log.Fields{
				"attempt":             attempt,
				"fetcher_duration_ms": fetcherDuration,
			}).WithError(err).Debug("FETCH_METADATA_FETCHER_CREATE_FAILED")
			return errors.Wrap(err, "get fetcher")
		}

		// Unpack nydus metadata file to specified path.
		downloadStart := time.Now()
		rc, err := fetcher.Fetch(ctx, desc)
		downloadDuration := time.Since(downloadStart).Milliseconds()
		if err != nil {
			logger.WithFields(log.Fields{
				"attempt":              attempt,
				"fetcher_duration_ms":  fetcherDuration,
				"download_duration_ms": downloadDuration,
			}).WithError(err).Debug("FETCH_METADATA_DOWNLOAD_FAILED")
			return errors.Wrap(err, "fetch nydus metadata")
		}
		defer rc.Close()

		logger.WithFields(log.Fields{
			"attempt":              attempt,
			"fetcher_duration_ms":  fetcherDuration,
			"download_duration_ms": downloadDuration,
		}).Debug("FETCH_METADATA_DOWNLOAD_COMPLETE")

		unpackStart := time.Now()
		if err := remote.Unpack(rc, metadataNameInLayer, metadataPath); err != nil {
			os.Remove(metadataPath)
			unpackDuration := time.Since(unpackStart).Milliseconds()
			logger.WithFields(log.Fields{
				"attempt":            attempt,
				"unpack_duration_ms": unpackDuration,
			}).WithError(err).Debug("FETCH_METADATA_UNPACK_FAILED")
			return errors.Wrap(err, "unpack metadata from layer")
		}
		unpackDuration := time.Since(unpackStart).Milliseconds()

		logger.WithFields(log.Fields{
			"attempt":              attempt,
			"fetcher_duration_ms":  fetcherDuration,
			"download_duration_ms": downloadDuration,
			"unpack_duration_ms":   unpackDuration,
			"handle_duration_ms":   time.Since(handleStart).Milliseconds(),
		}).Debug("FETCH_METADATA_HANDLE_SUCCESS")

		return nil
	}

	// TODO: check metafile already exists
	err := handle()
	if err != nil && r.remote.RetryWithPlainHTTP(ref, err) {
		logger.WithField("reason", "retry_with_plain_http").Warn("FETCH_METADATA_RETRYING_WITH_PLAIN_HTTP")
		err = handle()
	}

	if err != nil {
		logger.WithFields(log.Fields{
			"total_duration_ms": time.Since(start).Milliseconds(),
			"total_attempts":    attempt,
		}).WithError(err).Error("FETCH_METADATA_BLOB_FAILED")
	} else {
		logger.WithFields(log.Fields{
			"total_duration_ms": time.Since(start).Milliseconds(),
			"total_attempts":    attempt,
		}).Info("FETCH_METADATA_BLOB_COMPLETE")
	}

	return err
}
