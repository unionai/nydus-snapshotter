/*
 * Copyright (c) 2020. Ant Group. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package auth

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/log"
	"github.com/pkg/errors"

	"github.com/containerd/nydus-snapshotter/pkg/label"
	distribution "github.com/distribution/reference"
	"github.com/google/go-containerregistry/pkg/authn"
)

const (
	sep = ":"
)

var (
	emptyPassKeyChain = PassKeyChain{}
)

// PassKeyChain is user/password based key chain
type PassKeyChain struct {
	Username string
	Password string
}

func FromBase64(str string) (PassKeyChain, error) {
	decoded, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return emptyPassKeyChain, err
	}
	pair := strings.Split(string(decoded), sep)
	if len(pair) != 2 {
		return emptyPassKeyChain, errors.New("invalid registry auth token")
	}
	return PassKeyChain{
		Username: pair[0],
		Password: pair[1],
	}, nil
}

func (kc PassKeyChain) ToBase64() string {
	if kc.Username == "" && kc.Password == "" {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", kc.Username, kc.Password)))
}

// TokenBase check if PassKeyChain is token based, when username is empty and password is not empty
// then password is registry token
func (kc PassKeyChain) TokenBase() bool {
	return kc.Username == "" && kc.Password != ""
}

// FromLabels finds image pull username and secret from snapshot labels.
// Returned `nil` means no valid username and secret is passed, it should
// not override input nydusd configuration.
func FromLabels(labels map[string]string) *PassKeyChain {
	u, found := labels[label.NydusImagePullUsername]
	if !found || u == "" {
		return nil
	}

	p, found := labels[label.NydusImagePullSecret]
	if !found || p == "" {
		return nil
	}

	return &PassKeyChain{
		Username: u,
		Password: p,
	}
}

// GetRegistryKeyChain get image pull keychain from (ordered):
// 1. username and secrets labels
// 2. cri request
// 3. docker config
// 4. k8s docker config secret
func GetRegistryKeyChain(host, ref string, labels map[string]string) *PassKeyChain {
	start := time.Now()
	logger := log.L.WithFields(log.Fields{
		"component": "auth",
		"phase":     "get_registry_keychain",
		"host":      host,
		"ref":       ref,
	})
	logger.Debug("AUTH_KEYCHAIN_LOOKUP_START")

	// Source 1: Labels
	labelsStart := time.Now()
	kc := FromLabels(labels)
	labelsDuration := time.Since(labelsStart).Milliseconds()
	if kc != nil {
		logger.WithFields(log.Fields{
			"source":            "labels",
			"labels_duration_ms": labelsDuration,
			"total_duration_ms": time.Since(start).Milliseconds(),
			"has_credentials":   true,
		}).Info("AUTH_KEYCHAIN_FOUND")
		return kc
	}
	logger.WithFields(log.Fields{
		"source":            "labels",
		"labels_duration_ms": labelsDuration,
	}).Debug("AUTH_SOURCE_MISS")

	// Source 2: CRI
	criStart := time.Now()
	kc, _ = FromCRI(host, ref)
	criDuration := time.Since(criStart).Milliseconds()
	if kc != nil {
		logger.WithFields(log.Fields{
			"source":           "cri",
			"cri_duration_ms":  criDuration,
			"total_duration_ms": time.Since(start).Milliseconds(),
			"has_credentials":  true,
		}).Info("AUTH_KEYCHAIN_FOUND")
		return kc
	}
	logger.WithFields(log.Fields{
		"source":          "cri",
		"cri_duration_ms": criDuration,
	}).Debug("AUTH_SOURCE_MISS")

	// Source 3: Docker config
	dockerStart := time.Now()
	kc = FromDockerConfig(host)
	dockerDuration := time.Since(dockerStart).Milliseconds()
	if kc != nil {
		logger.WithFields(log.Fields{
			"source":             "docker_config",
			"docker_duration_ms": dockerDuration,
			"total_duration_ms":  time.Since(start).Milliseconds(),
			"has_credentials":    true,
		}).Info("AUTH_KEYCHAIN_FOUND")
		return kc
	}
	logger.WithFields(log.Fields{
		"source":             "docker_config",
		"docker_duration_ms": dockerDuration,
	}).Debug("AUTH_SOURCE_MISS")

	// Source 4: Kube secret
	kubeStart := time.Now()
	kc = FromKubeSecretDockerConfig(host)
	kubeDuration := time.Since(kubeStart).Milliseconds()
	if kc != nil {
		logger.WithFields(log.Fields{
			"source":            "kube_secret",
			"kube_duration_ms":  kubeDuration,
			"total_duration_ms": time.Since(start).Milliseconds(),
			"has_credentials":   true,
		}).Info("AUTH_KEYCHAIN_FOUND")
		return kc
	}

	logger.WithFields(log.Fields{
		"labels_duration_ms": labelsDuration,
		"cri_duration_ms":    criDuration,
		"docker_duration_ms": dockerDuration,
		"kube_duration_ms":   kubeDuration,
		"total_duration_ms":  time.Since(start).Milliseconds(),
		"has_credentials":    false,
	}).Debug("AUTH_KEYCHAIN_NOT_FOUND")

	return nil
}

func GetKeyChainByRef(ref string, labels map[string]string) (*PassKeyChain, error) {
	start := time.Now()
	logger := log.L.WithFields(log.Fields{
		"component": "auth",
		"phase":     "get_keychain_by_ref",
		"ref":       ref,
	})
	logger.Debug("GET_KEYCHAIN_BY_REF_START")

	parseStart := time.Now()
	named, err := distribution.ParseDockerRef(ref)
	parseDuration := time.Since(parseStart).Milliseconds()
	if err != nil {
		logger.WithFields(log.Fields{
			"parse_duration_ms": parseDuration,
		}).WithError(err).Error("GET_KEYCHAIN_BY_REF_PARSE_FAILED")
		return nil, errors.Wrapf(err, "parse ref %s", ref)
	}

	host := distribution.Domain(named)
	logger = logger.WithField("host", host)

	keychainStart := time.Now()
	keychain := GetRegistryKeyChain(host, ref, labels)
	keychainDuration := time.Since(keychainStart).Milliseconds()

	logger.WithFields(log.Fields{
		"parse_duration_ms":    parseDuration,
		"keychain_duration_ms": keychainDuration,
		"total_duration_ms":    time.Since(start).Milliseconds(),
		"has_credentials":      keychain != nil,
	}).Info("GET_KEYCHAIN_BY_REF_COMPLETE")

	return keychain, nil
}

func (kc PassKeyChain) Resolve(_ authn.Resource) (authn.Authenticator, error) {
	return authn.FromConfig(kc.toAuthConfig()), nil
}

// toAuthConfig convert PassKeyChain to authn.AuthConfig when kc is token based,
// RegistryToken is preferred to
func (kc PassKeyChain) toAuthConfig() authn.AuthConfig {
	if kc.TokenBase() {
		return authn.AuthConfig{
			RegistryToken: kc.Password,
		}
	}
	return authn.AuthConfig{
		Username: kc.Username,
		Password: kc.Password,
	}
}
