package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestUpdateTomlConfig(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "toml-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Test cases
	testCases := []struct {
		name           string
		inputContent   string
		expectedOutput string
		runtimeHandler string
	}{
		{
			name:         "Empty config",
			inputContent: "",
			expectedOutput: `[plugins]
[plugins.'io.containerd.grpc.v1.cri']
[plugins.'io.containerd.grpc.v1.cri'.containerd]
disable_snapshot_annotations = false
discard_unpacked_layers = false
snapshotter = 'nydus'

[proxy_plugins]
[proxy_plugins.nydus]
address = '/run/containerd-nydus/containerd-nydus-grpc.sock'
type = 'snapshot'
`,
			runtimeHandler: "",
		},
		{
			name: "Nydus as Default Snapshotter",
			inputContent: `
[plugins."io.containerd.grpc.v1.cri".containerd]
  snapshotter = "overlayfs"

[proxy_plugins.someotherplugin]
  type = "proxy"
  address = "/run/containerd-proxy.sock"
`,
			expectedOutput: `[plugins]
[plugins.'io.containerd.grpc.v1.cri']
[plugins.'io.containerd.grpc.v1.cri'.containerd]
disable_snapshot_annotations = false
discard_unpacked_layers = false
snapshotter = 'nydus'

[proxy_plugins]
[proxy_plugins.nydus]
address = '/run/containerd-nydus/containerd-nydus-grpc.sock'
type = 'snapshot'

[proxy_plugins.someotherplugin]
address = '/run/containerd-proxy.sock'
type = 'proxy'
`,
			runtimeHandler: "",
		},
		{
			name: "Nydus as specific Runtime Handler",
			inputContent: `
[plugins."io.containerd.grpc.v1.cri".containerd]
  disable_snapshot_annotations = true
	discard_unpacked_layers = true
	snapshotter = "overlayfs"
`,
			expectedOutput: `[plugins]
[plugins.'io.containerd.grpc.v1.cri']
[plugins.'io.containerd.grpc.v1.cri'.containerd]
disable_snapshot_annotations = false
discard_unpacked_layers = false
snapshotter = 'overlayfs'

[plugins.'io.containerd.grpc.v1.cri'.containerd.runtimes]
[plugins.'io.containerd.grpc.v1.cri'.containerd.runtimes.nydus]
snapshotter = 'nydus'

[proxy_plugins]
[proxy_plugins.nydus]
address = '/run/containerd-nydus/containerd-nydus-grpc.sock'
type = 'snapshot'
`,
			runtimeHandler: "nydus",
		},
		{
			name: "Nydus as specific Runtime Handler inherits existing default runtime configuration",
			inputContent: `
[plugins."io.containerd.grpc.v1.cri".containerd]
	default_runtime_name = 'runc'
  disable_snapshot_annotations = true
	discard_unpacked_layers = true
	snapshotter = "overlayfs"

[plugins.'io.containerd.grpc.v1.cri'.containerd.runtimes.runc]
runtime_type = 'io.containerd.runc.v2'

[plugins.'io.containerd.grpc.v1.cri'.containerd.runtimes.runc.options]
SystemdCgroup = true
`,
			expectedOutput: `[plugins]
[plugins.'io.containerd.grpc.v1.cri']
[plugins.'io.containerd.grpc.v1.cri'.containerd]
default_runtime_name = 'runc'
disable_snapshot_annotations = false
discard_unpacked_layers = false
snapshotter = 'overlayfs'

[plugins.'io.containerd.grpc.v1.cri'.containerd.runtimes]
[plugins.'io.containerd.grpc.v1.cri'.containerd.runtimes.nydus]
runtime_type = 'io.containerd.runc.v2'
snapshotter = 'nydus'

[plugins.'io.containerd.grpc.v1.cri'.containerd.runtimes.nydus.options]
SystemdCgroup = true

[plugins.'io.containerd.grpc.v1.cri'.containerd.runtimes.runc]
runtime_type = 'io.containerd.runc.v2'

[plugins.'io.containerd.grpc.v1.cri'.containerd.runtimes.runc.options]
SystemdCgroup = true

[proxy_plugins]
[proxy_plugins.nydus]
address = '/run/containerd-nydus/containerd-nydus-grpc.sock'
type = 'snapshot'
`,
			runtimeHandler: "nydus",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
						// Create input file
			inputPath := filepath.Join(tempDir, "input.toml")
			err := os.WriteFile(inputPath, []byte(tc.inputContent), 0644)
			if err != nil {
				t.Fatalf("Failed to write input file: %v", err)
			}

			// Get the initial inode number
			initialStat, err := os.Stat(inputPath)
			if err != nil {
				t.Fatalf("Failed to get initial file stats: %v", err)
			}
			initialInode := initialStat.Sys().(*syscall.Stat_t).Ino

			// Run the function
			err = updateTomlConfig(inputPath, tc.runtimeHandler)
			if err != nil {
				t.Fatalf("updateTomlConfig failed: %v", err)
			}

			// Get the final inode number
			finalStat, err := os.Stat(inputPath)
			if err != nil {
				t.Fatalf("Failed to get final file stats: %v", err)
			}
			finalInode := finalStat.Sys().(*syscall.Stat_t).Ino

			// Check if the inode remains the same
			if initialInode != finalInode {
				t.Errorf("Inode changed. Initial: %d, Final: %d", initialInode, finalInode)
			}

			// Read the output
			output, err := os.ReadFile(inputPath)
			if err != nil {
				t.Fatalf("Failed to read output file: %v", err)
			}

			// Compare output with expected
			if strings.TrimSpace(string(output)) != strings.TrimSpace(tc.expectedOutput) {
				t.Errorf("Output does not match expected.\nGot:\n%s\nWant:\n%s", string(output), tc.expectedOutput)
			}
		})
	}
}

func TestDeepMerge(t *testing.T) {
	testCases := []struct {
		name        string
		source      map[string]interface{}
		destination map[string]interface{}
		expected    map[string]interface{}
	}{
		{
			name: "Merge with empty destination",
			source: map[string]interface{}{
				"key1": "value1",
				"key2": map[string]interface{}{
					"subkey1": "subvalue1",
				},
			},
			destination: map[string]interface{}{},
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": map[string]interface{}{
					"subkey1": "subvalue1",
				},
			},
		},
		{
			name: "Merge with existing keys",
			source: map[string]interface{}{
				"key1": "new_value1",
				"key2": map[string]interface{}{
					"subkey1": "new_subvalue1",
					"subkey2": "subvalue2",
				},
			},
			destination: map[string]interface{}{
				"key1": "old_value1",
				"key2": map[string]interface{}{
					"subkey1": "old_subvalue1",
				},
				"key3": "value3",
			},
			expected: map[string]interface{}{
				"key1": "new_value1",
				"key2": map[string]interface{}{
					"subkey1": "new_subvalue1",
					"subkey2": "subvalue2",
				},
				"key3": "value3",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := deepMerge(tc.source, tc.destination)
			if !mapsEqual(result, tc.expected) {
				t.Errorf("DeepMerge result does not match expected.\nGot: %v\nWant: %v", result, tc.expected)
			}
		})
	}
}

// Helper function to compare maps
func mapsEqual(map1, map2 map[string]interface{}) bool {
	if len(map1) != len(map2) {
		return false
	}
	for key, value1 := range map1 {
		value2, ok := map2[key]
		if !ok {
			return false
		}
		switch v1 := value1.(type) {
		case map[string]interface{}:
			if v2, ok := value2.(map[string]interface{}); !ok || !mapsEqual(v1, v2) {
				return false
			}
		default:
			if value1 != value2 {
				return false
			}
		}
	}
	return true
}
