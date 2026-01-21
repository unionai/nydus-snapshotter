/*
 * Copyright (c) 2022. Nydus Developers. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/containerd/v2/defaults"
	"github.com/containerd/containerd/v2/pkg/dialer"
	"github.com/containerd/containerd/v2/pkg/reference"
	"github.com/containerd/log"
	"github.com/containerd/stargz-snapshotter/service/keychain/cri"
	"github.com/containerd/stargz-snapshotter/service/resolver"
	distribution "github.com/distribution/reference"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const DefaultImageServiceAddress = "/run/containerd/containerd.sock"

// Should be concurrency safe
var Credentials []resolver.Credential = make([]resolver.Credential, 0, 8)

// This function is borrowed from stargz
func newCRIConn(criAddr string) (*grpc.ClientConn, error) {
	// TODO: make gRPC options configurable from config.toml
	backoffConfig := backoff.DefaultConfig
	backoffConfig.MaxDelay = 3 * time.Second
	connParams := grpc.ConnectParams{
		Backoff: backoffConfig,
	}
	gopts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(connParams),
		grpc.WithContextDialer(dialer.ContextDialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaults.DefaultMaxRecvMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(defaults.DefaultMaxSendMsgSize)),
	}
	return grpc.NewClient(dialer.DialAddress(criAddr), gopts...)
}

// from stargz-snapshotter/cmd/containerd-stargz-grpc/main.go#main
func AddImageProxy(ctx context.Context, rpc *grpc.Server, imageServiceAddress string) {
	criAddr := DefaultImageServiceAddress
	if imageServiceAddress != "" {
		criAddr = imageServiceAddress
	}

	criCred, criServer := cri.NewCRIKeychain(ctx, func() (runtime.ImageServiceClient, error) {
		conn, err := newCRIConn(criAddr)
		if err != nil {
			return nil, err
		}

		return runtime.NewImageServiceClient(conn), nil
	})

	runtime.RegisterImageServiceServer(rpc, criServer)

	Credentials = append(Credentials, criCred)

	log.G(ctx).WithField("target-image-service", criAddr).Info("setup image proxy keychain")
}

func FromCRI(host, ref string) (*PassKeyChain, error) {
	start := time.Now()
	logger := log.L.WithFields(log.Fields{
		"component": "auth",
		"phase":     "from_cri",
		"host":      host,
		"ref":       ref,
	})
	logger.Debug("CRI_AUTH_START")

	if Credentials == nil {
		logger.Debug("CRI_AUTH_NO_CREDENTIALS_PARSERS")
		return nil, errors.New("No Credentials parsers")
	}

	parseStart := time.Now()
	refSpec, err := parseReference(ref)
	parseDuration := time.Since(parseStart).Milliseconds()
	if err != nil {
		logger.WithFields(log.Fields{
			"parse_duration_ms": parseDuration,
		}).WithError(err).Error("CRI_AUTH_PARSE_REF_FAILED")
		return nil, errors.Wrapf(err, "parse image reference %s", ref)
	}
	logger.WithField("parse_duration_ms", parseDuration).Debug("CRI_AUTH_REF_PARSED")

	var u, p string
	var keychain *PassKeyChain

	for i, cred := range Credentials {
		credStart := time.Now()
		username, secret, err := cred(host, refSpec)
		credDuration := time.Since(credStart).Milliseconds()

		if err != nil {
			logger.WithFields(log.Fields{
				"credential_index": i,
				"cred_duration_ms": credDuration,
				"total_duration_ms": time.Since(start).Milliseconds(),
			}).WithError(err).Error("CRI_CREDENTIAL_PROVIDER_ERROR")
			return nil, err
		}

		if !(username == "" && secret == "") {
			u = username
			p = secret

			keychain = &PassKeyChain{
				Username: u,
				Password: p,
			}

			logger.WithFields(log.Fields{
				"credential_index":  i,
				"cred_duration_ms":  credDuration,
				"total_duration_ms": time.Since(start).Milliseconds(),
				"has_credentials":   true,
			}).Info("CRI_AUTH_FOUND")
			break
		}

		logger.WithFields(log.Fields{
			"credential_index": i,
			"cred_duration_ms": credDuration,
		}).Debug("CRI_CREDENTIAL_PROVIDER_EMPTY")
	}

	if keychain == nil {
		logger.WithFields(log.Fields{
			"total_duration_ms":    time.Since(start).Milliseconds(),
			"credentials_checked":  len(Credentials),
		}).Debug("CRI_AUTH_NOT_FOUND")
	}

	return keychain, nil
}

// from stargz-snapshotter/service/keychain/cri/cri.go
func parseReference(ref string) (reference.Spec, error) {
	namedRef, err := distribution.ParseDockerRef(ref)
	if err != nil {
		return reference.Spec{}, fmt.Errorf("failed to parse image reference %q: %w", ref, err)
	}
	return reference.Parse(namedRef.String())
}
