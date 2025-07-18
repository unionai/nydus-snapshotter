package referrer

import (
	"testing"

	"github.com/containerd/containerd/v2/pkg/reference"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDockerReference(t *testing.T) {
	r := &referrer{}

	tests := []struct {
		name        string
		ref         string
		expectedTag string
		expectedErr bool
	}{
		{
			name:        "simple tag",
			ref:         "registry.example.com/repo:tag",
			expectedTag: "tag",
		},
		{
			name:        "tag with port",
			ref:         "registry.example.com:5000/repo:tag",
			expectedTag: "tag",
		},
		{
			name:        "complex tag",
			ref:         "gcr.io/project/repo:v1.2.3-alpha",
			expectedTag: "v1.2.3-alpha",
		},
		{
			name:        "GAR reference",
			ref:         "us-docker.pkg.dev/project/repo:b9679c986b164cea32ac596e6a8f9973aa9c8c3a",
			expectedTag: "b9679c986b164cea32ac596e6a8f9973aa9c8c3a",
		},
		{
			name:        "latest tag",
			ref:         "registry.example.com/repo:latest",
			expectedTag: "latest",
		},
		{
			name:        "no tag should use latest",
			ref:         "registry.example.com/repo",
			expectedTag: "latest",
		},
		{
			name:        "digest only (no tag)",
			ref:         "registry.example.com/repo@sha256:abc123",
			expectedTag: "",
			expectedErr: true, // We expect this to fail since we need a tag for suffix-based discovery
		},
		{
			name:        "both tag and digest",
			ref:         "registry.example.com/repo:tag@sha256:abc123",
			expectedTag: "tag",
		},
		{
			name:        "invalid reference",
			ref:         "invalid::reference",
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			refspec, err := reference.Parse(tt.ref)
			if err != nil && !tt.expectedErr {
				t.Fatalf("unexpected error parsing reference: %v", err)
			}
			if err != nil && tt.expectedErr {
				// Expected error during parsing, test passes
				return
			}

			tag, err := r.parseTagFromReference(refspec)

			if tt.expectedErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedTag, tag)
		})
	}
}

func TestReferrerGenerateReferrerCandidates(t *testing.T) {
	// Create a test referrer with some suffixes
	r := &referrer{
		referrerTagSuffixes: []string{"-opt", "-nydus", ".custom"},
	}

	tests := []struct {
		name         string
		ref          string
		expectedRefs []string
		expectedErr  bool
	}{
		{
			name: "simple reference",
			ref:  "registry.example.com/repo:tag",
			expectedRefs: []string{
				"registry.example.com/repo:tag-opt",
				"registry.example.com/repo:tag-nydus",
				"registry.example.com/repo:tag.custom",
			},
		},
		{
			name: "GAR reference",
			ref:  "us-docker.pkg.dev/project/repo:b9679c986b164cea32ac596e6a8f9973aa9c8c3a",
			expectedRefs: []string{
				"us-docker.pkg.dev/project/repo:b9679c986b164cea32ac596e6a8f9973aa9c8c3a-opt",
				"us-docker.pkg.dev/project/repo:b9679c986b164cea32ac596e6a8f9973aa9c8c3a-nydus",
				"us-docker.pkg.dev/project/repo:b9679c986b164cea32ac596e6a8f9973aa9c8c3a.custom",
			},
		},
		{
			name: "reference with tag and digest",
			ref:  "registry.example.com/repo:tag@sha256:abc123def456",
			expectedRefs: []string{
				"registry.example.com/repo:tag-opt",
				"registry.example.com/repo:tag-nydus",
				"registry.example.com/repo:tag.custom",
			},
		},
		{
			name: "no tag (uses latest)",
			ref:  "registry.example.com/repo",
			expectedRefs: []string{
				"registry.example.com/repo:latest-opt",
				"registry.example.com/repo:latest-nydus",
				"registry.example.com/repo:latest.custom",
			},
		},
		{
			name:        "digest-only reference",
			ref:         "registry.example.com/repo@sha256:abc123def456",
			expectedErr: true,
		},
		{
			name:        "invalid reference",
			ref:         "invalid::reference",
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates, err := r.generateReferrerCandidates(tt.ref, r.referrerTagSuffixes)

			if tt.expectedErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedRefs, candidates)
		})
	}
}
