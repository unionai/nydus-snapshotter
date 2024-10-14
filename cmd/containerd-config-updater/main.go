package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pelletier/go-toml/v2"
)

// Attempt to find the default runtime configuration, if it exists.
func extractDefaultRuntimeConfig(config map[string]interface{}) map[string]interface{} {
	plugins, ok := config["plugins"].(map[string]interface{})
	if !ok {
		return nil
	}

	cri, ok := plugins["io.containerd.grpc.v1.cri"].(map[string]interface{})
	if !ok {
		return nil
	}

	containerd, ok := cri["containerd"].(map[string]interface{})
	if !ok {
		return nil
	}

	defaultRuntimeName, ok := containerd["default_runtime_name"].(string)
	if !ok {
		return nil
	}

	runtimes, ok := containerd["runtimes"].(map[string]interface{})
	if !ok {
		return nil
	}

	runtimeConfig, ok := runtimes[defaultRuntimeName].(map[string]interface{})
	if !ok {
		return nil
	}

	return runtimeConfig
}

func deepMerge(source, destination map[string]interface{}) map[string]interface{} {
	if destination == nil {
		return source
	}
	for key, value := range source {
		if destValue, exists := destination[key]; exists {
			if sourceMap, isSourceMap := value.(map[string]interface{}); isSourceMap {
				if destMap, isDestMap := destValue.(map[string]interface{}); isDestMap {
					destination[key] = deepMerge(sourceMap, destMap)
					continue
				}
			}
		}
		destination[key] = value
	}
	return destination
}

// updateTomlConfig updates the TOML configuration file at the given path with the Nydus-specific configuration.
//
// runtimeHandler is the name of the runtime handler to use for the snapshotter. Empty string will result in
// containerd using nydus as the default snapshotter.
func updateTomlConfig(configPath, runtimeHandler string) error {
	// Open the existing TOML file
	file, err := os.OpenFile(configPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening config file: %w", err)
	}
	defer file.Close()

	// Read the existing content
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}

	var config map[string]interface{}
	if err := toml.Unmarshal(configBytes, &config); err != nil {
		return fmt.Errorf("error unmarshalling config file: %w", err)
	}

	var defaultSnapshotter map[string]interface{}
	var containerdRuntime map[string]interface{}
	if runtimeHandler == "" {
		defaultSnapshotter = map[string]interface{}{
			"snapshotter": "nydus",
		}
	} else {
		defaultRuntimeConfig := extractDefaultRuntimeConfig(config)

		containerdRuntime = map[string]interface{}{
			"runtimes": map[string]interface{}{
				runtimeHandler: deepMerge(defaultRuntimeConfig, map[string]interface{}{
					"snapshotter": "nydus",
				}),
			},
		}
	}

	// Define the Nydus-specific configuration
	nydusConfig := map[string]interface{}{
		"proxy_plugins": map[string]interface{}{
			"nydus": map[string]interface{}{
				"type":    "snapshot",
				"address": "/run/containerd-nydus/containerd-nydus-grpc.sock",
			},
		},
		"plugins": map[string]interface{}{
			"io.containerd.grpc.v1.cri": map[string]interface{}{
				"containerd": deepMerge(deepMerge(map[string]interface{}{
					"discard_unpacked_layers":      false,
					"disable_snapshot_annotations": false,
				}, defaultSnapshotter), containerdRuntime),
			},
		},
	}

	// Perform a deep merge of the configurations
	updatedConfig := deepMerge(nydusConfig, config)

	// Replace the file in place to prevent changing underlying inodes.
	// Truncate the file to ensure it's empty
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("error truncating file: %w", err)
	}

	// Move the file pointer to the beginning
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("error seeking file: %w", err)
	}

	// Write the updated configuration to the file
	encoder := toml.NewEncoder(file)
	if err := encoder.Encode(updatedConfig); err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}

	fmt.Fprintf(os.Stderr, "TOML configuration updated in-place at: %s\n", configPath)
	return nil
}

func main() {
	configPath := flag.String("c", "", "Path to the Containerd TOML configuration file")
	runtimeHandler := flag.String("r", "", "Containerd Runtime Handler name. If not specified, nydus is used as the default snapshotter")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "Error: Config file path is required")
		flag.Usage()
		os.Exit(1)
	}

	if err := updateTomlConfig(*configPath, *runtimeHandler); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
