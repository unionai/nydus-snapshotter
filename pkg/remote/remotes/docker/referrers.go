/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package docker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func (r dockerFetcher) FetchReferrers(ctx context.Context, dgst digest.Digest, artifactTypes ...string) (io.ReadCloser, ocispec.Descriptor, error) {
	start := time.Now()
	var desc ocispec.Descriptor
	desc.MediaType = ocispec.MediaTypeImageIndex

	logger := log.G(ctx).WithFields(log.Fields{
		"component": "http_transport",
		"phase":     "fetch_referrers",
		"digest":    dgst.String(),
	})
	ctx = log.WithLogger(ctx, logger)
	logger.Debug("HTTP_FETCH_REFERRERS_START")

	hosts := r.filterHosts(HostCapabilityResolve, HostCapabilityReferrers)
	if len(hosts) == 0 {
		logger.Debug("HTTP_FETCH_REFERRERS_NO_HOSTS")
		return nil, desc, fmt.Errorf("no pull hosts: %w", errdefs.ErrNotFound)
	}
	logger.WithField("host_count", len(hosts)).Debug("HTTP_FETCH_REFERRERS_HOSTS_FOUND")

	ctx, err := ContextWithRepositoryScope(ctx, r.refspec, false)
	if err != nil {
		return nil, desc, err
	}

	for hostIdx, host := range hosts {
		hostStart := time.Now()
		hostLogger := logger.WithFields(log.Fields{
			"host_index": hostIdx,
			"host":       host.Host,
		})

		var req *request
		// Try standard referrers API first
		req = r.request(host, http.MethodGet, "referrers", dgst.String())
		for _, artifactType := range artifactTypes {
			if err := req.addQuery("artifactType", artifactType); err != nil {
				return nil, desc, err
			}
		}
		if err := req.addNamespace(r.refspec.Hostname()); err != nil {
			return nil, desc, err
		}

		hostLogger.Debug("HTTP_REFERRERS_API_ATTEMPT")
		apiStart := time.Now()
		rc, cl, err := r.open(ctx, req, desc.MediaType, 0)
		apiDuration := time.Since(apiStart).Milliseconds()

		if err != nil {
			if !errdefs.IsNotFound(err) {
				hostLogger.WithFields(log.Fields{
					"api_duration_ms": apiDuration,
				}).WithError(err).Debug("HTTP_REFERRERS_API_ERROR")
				return nil, desc, err
			}
			hostLogger.WithFields(log.Fields{
				"api_duration_ms": apiDuration,
			}).Debug("HTTP_REFERRERS_API_NOT_FOUND")
		} else {
			desc.Size = cl
			hostLogger.WithFields(log.Fields{
				"api_duration_ms":   apiDuration,
				"host_duration_ms":  time.Since(hostStart).Milliseconds(),
				"total_duration_ms": time.Since(start).Milliseconds(),
				"content_length":    cl,
				"method":            "referrers_api",
			}).Info("HTTP_FETCH_REFERRERS_SUCCESS")
			return rc, desc, nil
		}

		// Try tag-based fallback
		if host.Capabilities.Has(HostCapabilityResolve) {
			fallbackTag := strings.Replace(dgst.String(), ":", "-", 1)
			req = r.request(host, http.MethodGet, "manifests", fallbackTag)
			if err := req.addNamespace(r.refspec.Hostname()); err != nil {
				return nil, desc, err
			}

			hostLogger.WithField("fallback_tag", fallbackTag).Debug("HTTP_REFERRERS_TAG_FALLBACK_ATTEMPT")
			fallbackStart := time.Now()
			rc, cl, err := r.open(ctx, req, desc.MediaType, 0)
			fallbackDuration := time.Since(fallbackStart).Milliseconds()

			if err != nil {
				if !errdefs.IsNotFound(err) {
					hostLogger.WithFields(log.Fields{
						"fallback_duration_ms": fallbackDuration,
					}).WithError(err).Debug("HTTP_REFERRERS_TAG_FALLBACK_ERROR")
					return nil, desc, err
				}
				hostLogger.WithFields(log.Fields{
					"api_duration_ms":      apiDuration,
					"fallback_duration_ms": fallbackDuration,
					"host_duration_ms":     time.Since(hostStart).Milliseconds(),
				}).Debug("HTTP_REFERRERS_TAG_FALLBACK_NOT_FOUND")
			} else {
				desc.Size = cl
				hostLogger.WithFields(log.Fields{
					"api_duration_ms":      apiDuration,
					"fallback_duration_ms": fallbackDuration,
					"host_duration_ms":     time.Since(hostStart).Milliseconds(),
					"total_duration_ms":    time.Since(start).Milliseconds(),
					"content_length":       cl,
					"method":               "tag_fallback",
				}).Info("HTTP_FETCH_REFERRERS_SUCCESS")
				return rc, desc, nil
			}
		}
	}

	logger.WithFields(log.Fields{
		"total_duration_ms": time.Since(start).Milliseconds(),
		"hosts_tried":       len(hosts),
	}).Debug("HTTP_FETCH_REFERRERS_ALL_HOSTS_FAILED")

	return nil, ocispec.Descriptor{}, fmt.Errorf("could not be found at any host: %w", errdefs.ErrNotFound)
}
