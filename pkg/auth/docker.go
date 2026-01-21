/*
 * Copyright (c) 2021. Ant Group. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package auth

import (
	"os"
	"time"

	"github.com/containerd/log"
	dockerconfig "github.com/docker/cli/cli/config"
)

const (
	dockerHost          = "https://index.docker.io/v1/"
	convertedDockerHost = "registry-1.docker.io"
)

// FromDockerConfig finds auth for a given host in docker's config.json settings.
func FromDockerConfig(host string) *PassKeyChain {
	start := time.Now()
	logger := log.L.WithFields(log.Fields{
		"component": "auth",
		"phase":     "from_docker_config",
		"host":      host,
	})
	logger.Debug("DOCKER_CONFIG_AUTH_START")

	if len(host) == 0 {
		return nil
	}

	originalHost := host
	// The host of docker hub image will be converted to `registry-1.docker.io` in:
	// github.com/containerd/containerd/remotes/docker/registry.go
	// But we need use the key `https://index.docker.io/v1/` to find auth from docker config.
	if host == convertedDockerHost {
		host = dockerHost
		logger.WithFields(log.Fields{
			"original_host":  originalHost,
			"converted_host": host,
		}).Debug("DOCKER_CONFIG_HOST_CONVERTED")
	}

	// This call may invoke credential helpers (e.g., docker-credential-gcloud)
	// which can be slow (755ms+ for gcloud)
	configLoadStart := time.Now()
	config := dockerconfig.LoadDefaultConfigFile(os.Stderr)
	configLoadDuration := time.Since(configLoadStart).Milliseconds()
	logger.WithField("config_load_duration_ms", configLoadDuration).Debug("DOCKER_CONFIG_LOADED")

	// THIS IS WHERE CREDENTIAL HELPERS ARE INVOKED
	authStart := time.Now()
	authConfig, err := config.GetAuthConfig(host)
	authDuration := time.Since(authStart).Milliseconds()

	if err != nil {
		logger.WithFields(log.Fields{
			"config_load_duration_ms": configLoadDuration,
			"auth_duration_ms":        authDuration,
			"total_duration_ms":       time.Since(start).Milliseconds(),
		}).WithError(err).Debug("DOCKER_CONFIG_AUTH_ERROR")
		return nil
	}

	// Do not return empty auth. It makes caller life easier.
	if len(authConfig.Username) == 0 || len(authConfig.Password) == 0 {
		logger.WithFields(log.Fields{
			"config_load_duration_ms": configLoadDuration,
			"auth_duration_ms":        authDuration,
			"total_duration_ms":       time.Since(start).Milliseconds(),
			"username_empty":          len(authConfig.Username) == 0,
			"password_empty":          len(authConfig.Password) == 0,
		}).Debug("DOCKER_CONFIG_AUTH_EMPTY")
		return nil
	}

	logger.WithFields(log.Fields{
		"config_load_duration_ms": configLoadDuration,
		"auth_duration_ms":        authDuration,
		"total_duration_ms":       time.Since(start).Milliseconds(),
		"has_credentials":         true,
	}).Info("DOCKER_CONFIG_AUTH_FOUND")

	return &PassKeyChain{
		Username: authConfig.Username,
		Password: authConfig.Password,
	}
}
