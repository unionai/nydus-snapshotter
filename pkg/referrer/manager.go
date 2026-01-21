/*
 * Copyright (c) 2023. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package referrer

import (
	"context"
	"time"

	"github.com/containerd/log"
	"github.com/containerd/nydus-snapshotter/pkg/auth"
	"github.com/golang/groupcache/lru"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
)

var defaultReferrerTagSuffixes = []string{"-opt"}

type Manager struct {
	insecure            bool
	cache               *lru.Cache
	sg                  singleflight.Group
	referrerTagSuffixes []string
}

func NewManager(insecure bool, referrerTagSuffixes []string) *Manager {
	manager := Manager{
		insecure:            insecure,
		cache:               lru.New(500),
		sg:                  singleflight.Group{},
		referrerTagSuffixes: referrerTagSuffixes,
	}

	// Set default tag suffixes if not configured
	if len(manager.referrerTagSuffixes) == 0 {
		manager.referrerTagSuffixes = defaultReferrerTagSuffixes
	}

	return &manager
}

// CheckReferrer attempts to fetch the referrers and parse out
// the nydus image by specified manifest digest.
func (manager *Manager) CheckReferrer(ctx context.Context, ref string, manifestDigest digest.Digest) (*ocispec.Descriptor, error) {
	checkReferrerStart := time.Now()
	logger := log.G(ctx).WithFields(log.Fields{
		"component":       "referrer_detection",
		"ref":             ref,
		"manifest_digest": manifestDigest.String(),
	})
	logger.Debug("REFERRER_CHECK_START")

	metaLayer, err, shared := manager.sg.Do(manifestDigest.String(), func() (interface{}, error) {
		sgStart := time.Now()

		// Try to get nydus metadata layer descriptor from LRU cache.
		cacheStart := time.Now()
		if metaLayer, ok := manager.cache.Get(manifestDigest); ok {
			desc := metaLayer.(ocispec.Descriptor)
			logger.WithFields(log.Fields{
				"cache_hit":   true,
				"duration_ms": time.Since(cacheStart).Milliseconds(),
			}).Info("REFERRER_CACHE_HIT")
			return &desc, nil
		}
		logger.WithFields(log.Fields{
			"cache_hit":   false,
			"duration_ms": time.Since(cacheStart).Milliseconds(),
		}).Debug("REFERRER_CACHE_MISS")

		// Get keychain - THIS IS CALL #1
		keychainStart := time.Now()
		keyChain, err := auth.GetKeyChainByRef(ref, nil)
		keychainDuration := time.Since(keychainStart).Milliseconds()
		if err != nil {
			logger.WithFields(log.Fields{
				"keychain_duration_ms": keychainDuration,
				"call_site":            "CheckReferrer",
				"error":                err.Error(),
			}).Error("KEYCHAIN_ACQUIRE_FAILED")
			return nil, errors.Wrap(err, "get key chain")
		}
		logger.WithFields(log.Fields{
			"keychain_duration_ms": keychainDuration,
			"call_site":            "CheckReferrer",
			"has_credentials":      keyChain != nil && keyChain.Username != "",
		}).Info("KEYCHAIN_ACQUIRED")

		// No LRU cache found, try to fetch referrers and parse out
		// the nydus metadata layer descriptor.
		referrerCheckStart := time.Now()
		referrer := newReferrer(keyChain, manager.insecure, manager.referrerTagSuffixes)
		metaLayer, err := referrer.checkReferrer(ctx, ref, manifestDigest)
		referrerCheckDuration := time.Since(referrerCheckStart).Milliseconds()
		if err != nil {
			logger.WithFields(log.Fields{
				"keychain_duration_ms":       keychainDuration,
				"referrer_check_duration_ms": referrerCheckDuration,
				"singleflight_duration_ms":   time.Since(sgStart).Milliseconds(),
				"error":                      err.Error(),
			}).Warn("REFERRER_CHECK_FAILED")
			return nil, errors.Wrap(err, "check referrer")
		}

		// FIXME: how to invalidate the LRU cache if referrers update?
		manager.cache.Add(manifestDigest, *metaLayer)

		logger.WithFields(log.Fields{
			"keychain_duration_ms":       keychainDuration,
			"referrer_check_duration_ms": referrerCheckDuration,
			"singleflight_duration_ms":   time.Since(sgStart).Milliseconds(),
			"meta_layer_digest":          metaLayer.Digest.String(),
		}).Info("REFERRER_CHECK_SUCCESS")

		return metaLayer, nil
	})

	if err != nil {
		logger.WithFields(log.Fields{
			"total_duration_ms": time.Since(checkReferrerStart).Milliseconds(),
			"singleflight_shared": shared,
		}).WithError(err).Warn("REFERRER_CHECK_COMPLETE_ERROR")
		return nil, err
	}

	logger.WithFields(log.Fields{
		"total_duration_ms":   time.Since(checkReferrerStart).Milliseconds(),
		"singleflight_shared": shared,
	}).Info("REFERRER_CHECK_COMPLETE")

	return metaLayer.(*ocispec.Descriptor), nil
}

// TryFetchMetadata try to fetch and unpack nydus metadata file to specified path.
func (manager *Manager) TryFetchMetadata(ctx context.Context, ref string, manifestDigest digest.Digest, metadataPath string) error {
	fetchStart := time.Now()
	logger := log.G(ctx).WithFields(log.Fields{
		"component":       "referrer_detection",
		"ref":             ref,
		"manifest_digest": manifestDigest.String(),
		"metadata_path":   metadataPath,
	})
	logger.Debug("METADATA_FETCH_START")

	checkReferrerStart := time.Now()
	metaLayer, err := manager.CheckReferrer(ctx, ref, manifestDigest)
	checkReferrerDuration := time.Since(checkReferrerStart).Milliseconds()
	if err != nil {
		logger.WithFields(log.Fields{
			"check_referrer_duration_ms": checkReferrerDuration,
		}).WithError(err).Warn("METADATA_FETCH_CHECK_REFERRER_FAILED")
		return errors.Wrap(err, "check referrer")
	}
	logger.WithFields(log.Fields{
		"check_referrer_duration_ms": checkReferrerDuration,
		"meta_layer_digest":          metaLayer.Digest.String(),
		"meta_layer_size":            metaLayer.Size,
	}).Debug("METADATA_FETCH_CHECK_REFERRER_COMPLETE")

	// Get keychain - THIS IS CALL #2 (duplicate)
	keychainStart := time.Now()
	keyChain, err := auth.GetKeyChainByRef(ref, nil)
	keychainDuration := time.Since(keychainStart).Milliseconds()
	if err != nil {
		logger.WithFields(log.Fields{
			"keychain_duration_ms": keychainDuration,
			"call_site":            "TryFetchMetadata",
		}).WithError(err).Error("KEYCHAIN_ACQUIRE_FAILED")
		return errors.Wrap(err, "get key chain")
	}
	logger.WithFields(log.Fields{
		"keychain_duration_ms": keychainDuration,
		"call_site":            "TryFetchMetadata",
		"duplicate_call":       true,
		"has_credentials":      keyChain != nil && keyChain.Username != "",
	}).Info("KEYCHAIN_ACQUIRED")

	downloadStart := time.Now()
	referrer := newReferrer(keyChain, manager.insecure, manager.referrerTagSuffixes)
	err = referrer.fetchMetadata(ctx, ref, *metaLayer, metadataPath)
	downloadDuration := time.Since(downloadStart).Milliseconds()

	if err != nil {
		logger.WithFields(log.Fields{
			"check_referrer_duration_ms": checkReferrerDuration,
			"keychain_duration_ms":       keychainDuration,
			"download_duration_ms":       downloadDuration,
			"total_duration_ms":          time.Since(fetchStart).Milliseconds(),
		}).WithError(err).Error("METADATA_FETCH_FAILED")
		return err
	}

	logger.WithFields(log.Fields{
		"check_referrer_duration_ms": checkReferrerDuration,
		"keychain_duration_ms":       keychainDuration,
		"download_duration_ms":       downloadDuration,
		"total_duration_ms":          time.Since(fetchStart).Milliseconds(),
	}).Info("METADATA_FETCH_COMPLETE")

	return nil
}
