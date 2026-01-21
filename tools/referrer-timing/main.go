// Referrer Detection Timing Test Harness
//
// This program tests the HTTP paths used by nydus-snapshotter's referrer detection
// using the same resolver and fetcher code.
//
// Usage:
//   go run main.go -ref us-docker.pkg.dev/dogfood-gcp-dataplane/orgs/dogfood-gcp/flyte:ae7937418a56fdb730bda165d004908b
//   go run main.go -ref 992382529030.dkr.ecr.us-east-2.amazonaws.com/union/dogfood:flyte-4226d5b6bc1d573fe3264aeaa1792702

package main

import (
	"context"
	"encoding/json"
	"flag"
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
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const maxManifestIndexSize = 0x800000

var (
	imageRef = flag.String("ref", "", "Full image reference (required)")
	insecure = flag.Bool("insecure", false, "Allow insecure registry connections")
	verbose  = flag.Bool("verbose", false, "Verbose output")
)

type TimingResult struct {
	Phase       string  `json:"phase"`
	Description string  `json:"description"`
	DurationMs  float64 `json:"duration_ms"`
	Error       string  `json:"error,omitempty"`
}

func main() {
	flag.Parse()

	if *imageRef == "" {
		fmt.Println("Usage: go run main.go -ref <image-reference>")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  go run main.go -ref us-docker.pkg.dev/dogfood-gcp-dataplane/orgs/dogfood-gcp/flyte:ae7937418a56fdb730bda165d004908b")
		fmt.Println("  go run main.go -ref 992382529030.dkr.ecr.us-east-2.amazonaws.com/union/dogfood:flyte-4226d5b6bc1d573fe3264aeaa1792702")
		os.Exit(1)
	}

	if *verbose {
		log.L.Logger.SetLevel(log.DebugLevel)
	} else {
		log.L.Logger.SetLevel(log.WarnLevel)
	}

	timings := make([]TimingResult, 0)

	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║     Nydus-Snapshotter Referrer Detection Timing Harness         ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Image: %s\n", *imageRef)
	fmt.Println()

	totalStart := time.Now()
	ctx := context.Background()

	// Phase 1: Parse reference
	start := time.Now()
	refspec, err := reference.Parse(*imageRef)
	dur := time.Since(start)
	timings = append(timings, TimingResult{
		Phase:       "1-parse-reference",
		Description: "Parse image reference",
		DurationMs:  float64(dur.Microseconds()) / 1000.0,
	})
	fmt.Printf("Phase 1:  Parse reference           %8.1fms\n", float64(dur.Microseconds())/1000.0)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}

	// Phase 2: Get keychain
	start = time.Now()
	keyChain, err := auth.GetKeyChainByRef(*imageRef, nil)
	dur = time.Since(start)
	timings = append(timings, TimingResult{
		Phase:       "2-get-keychain",
		Description: "Get authentication keychain",
		DurationMs:  float64(dur.Microseconds()) / 1000.0,
	})
	fmt.Printf("Phase 2:  Get keychain (auth)       %8.1fms\n", float64(dur.Microseconds())/1000.0)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}

	// Phase 3: Create remote
	start = time.Now()
	rem := remote.New(keyChain, *insecure)
	dur = time.Since(start)
	timings = append(timings, TimingResult{
		Phase:       "3-create-remote",
		Description: "Create remote client",
		DurationMs:  float64(dur.Microseconds()) / 1000.0,
	})
	fmt.Printf("Phase 3:  Create remote             %8.1fms\n", float64(dur.Microseconds())/1000.0)

	// Phase 4: Create resolver
	start = time.Now()
	resolver := rem.Resolve(ctx, *imageRef)
	dur = time.Since(start)
	timings = append(timings, TimingResult{
		Phase:       "4-create-resolver",
		Description: "Create resolver",
		DurationMs:  float64(dur.Microseconds()) / 1000.0,
	})
	fmt.Printf("Phase 4:  Create resolver           %8.1fms\n", float64(dur.Microseconds())/1000.0)

	// Phase 5: Resolve reference (HTTP)
	start = time.Now()
	_, desc, err := resolver.Resolve(ctx, *imageRef)
	dur = time.Since(start)
	timings = append(timings, TimingResult{
		Phase:       "5-resolve-reference",
		Description: "Resolve reference to digest (HTTP HEAD)",
		DurationMs:  float64(dur.Microseconds()) / 1000.0,
	})
	fmt.Printf("Phase 5:  Resolve reference         %8.1fms  ← HTTP\n", float64(dur.Microseconds())/1000.0)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}
	manifestDigest := desc.Digest
	fmt.Printf("          Digest: %s\n", manifestDigest)

	// Phase 6: Create fetcher
	start = time.Now()
	fetcher, err := rem.Fetcher(ctx, *imageRef)
	dur = time.Since(start)
	timings = append(timings, TimingResult{
		Phase:       "6-create-fetcher",
		Description: "Create fetcher",
		DurationMs:  float64(dur.Microseconds()) / 1000.0,
	})
	fmt.Printf("Phase 6:  Create fetcher            %8.1fms\n", float64(dur.Microseconds())/1000.0)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}

	// Phase 7: Fetch referrers (HTTP)
	start = time.Now()
	rc, _, err := fetcher.(remotes.ReferrersFetcher).FetchReferrers(ctx, manifestDigest)
	dur = time.Since(start)
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	timings = append(timings, TimingResult{
		Phase:       "7-fetch-referrers",
		Description: "Fetch referrers via standard API (HTTP GET)",
		DurationMs:  float64(dur.Microseconds()) / 1000.0,
		Error:       errStr,
	})
	fmt.Printf("Phase 7:  Fetch referrers API       %8.1fms  ← HTTP\n", float64(dur.Microseconds())/1000.0)

	var index ocispec.Index
	var hasReferrers bool

	if err == nil {
		start = time.Now()
		bytes, _ := io.ReadAll(io.LimitReader(rc, maxManifestIndexSize))
		rc.Close()
		json.Unmarshal(bytes, &index)
		dur = time.Since(start)
		timings = append(timings, TimingResult{
			Phase:       "8-parse-referrers",
			Description: "Parse referrers response",
			DurationMs:  float64(dur.Microseconds()) / 1000.0,
		})
		fmt.Printf("Phase 8:  Parse referrers           %8.1fms\n", float64(dur.Microseconds())/1000.0)
		fmt.Printf("          Manifests found: %d\n", len(index.Manifests))
		hasReferrers = len(index.Manifests) > 0
	} else {
		fmt.Printf("          Error: %v\n", err)
	}

	if hasReferrers {
		// Fetch referrer manifest
		start = time.Now()
		rc, err := fetcher.Fetch(ctx, index.Manifests[0])
		dur = time.Since(start)
		timings = append(timings, TimingResult{
			Phase:       "9-fetch-referrer-manifest",
			Description: "Fetch referrer manifest (HTTP GET)",
			DurationMs:  float64(dur.Microseconds()) / 1000.0,
		})
		fmt.Printf("Phase 9:  Fetch referrer manifest   %8.1fms  ← HTTP\n", float64(dur.Microseconds())/1000.0)

		if err == nil {
			var manifest ocispec.Manifest
			bytes, _ := io.ReadAll(rc)
			rc.Close()
			json.Unmarshal(bytes, &manifest)

			if len(manifest.Layers) > 0 {
				metaLayer := manifest.Layers[len(manifest.Layers)-1]
				if label.IsNydusMetaLayer(metaLayer.Annotations) {
					fmt.Printf("          Bootstrap: %s\n", metaLayer.Digest)

					start = time.Now()
					blobRc, err := fetcher.Fetch(ctx, metaLayer)
					dur = time.Since(start)
					timings = append(timings, TimingResult{
						Phase:       "10-fetch-bootstrap",
						Description: "Fetch bootstrap blob (HTTP GET)",
						DurationMs:  float64(dur.Microseconds()) / 1000.0,
					})
					fmt.Printf("Phase 10: Fetch bootstrap blob      %8.1fms  ← HTTP\n", float64(dur.Microseconds())/1000.0)

					if err == nil {
						io.Copy(io.Discard, blobRc)
						blobRc.Close()
					}
				}
			}
		}
	} else {
		// Tag-based fallback
		fmt.Println()
		fmt.Println("─── Tag-based fallback ───")
		tag := extractTag(refspec)
		if tag != "" {
			optTag := tag + "-opt"
			optRef := refspec.Locator + ":" + optTag
			fmt.Printf("          Trying: %s\n", optRef)

			start = time.Now()
			optResolver := rem.Resolve(ctx, optRef)
			dur = time.Since(start)
			timings = append(timings, TimingResult{
				Phase:       "9a-create-opt-resolver",
				Description: "Create resolver for -opt tag",
				DurationMs:  float64(dur.Microseconds()) / 1000.0,
			})
			fmt.Printf("Phase 9a: Create opt resolver       %8.1fms\n", float64(dur.Microseconds())/1000.0)

			start = time.Now()
			_, optDesc, err := optResolver.Resolve(ctx, optRef)
			dur = time.Since(start)
			errStr := ""
			if err != nil {
				errStr = err.Error()
			}
			timings = append(timings, TimingResult{
				Phase:       "9b-resolve-opt-tag",
				Description: "Resolve -opt tag (HTTP HEAD)",
				DurationMs:  float64(dur.Microseconds()) / 1000.0,
				Error:       errStr,
			})
			fmt.Printf("Phase 9b: Resolve opt tag           %8.1fms  ← HTTP\n", float64(dur.Microseconds())/1000.0)

			if err != nil {
				fmt.Printf("          Not found: %v\n", err)
			} else {
				fmt.Printf("          Found: %s\n", optDesc.Digest)

				start = time.Now()
				optFetcher, _ := optResolver.Fetcher(ctx, optRef)
				rc, err := optFetcher.Fetch(ctx, optDesc)
				dur = time.Since(start)
				timings = append(timings, TimingResult{
					Phase:       "9c-fetch-opt-manifest",
					Description: "Fetch -opt manifest (HTTP GET)",
					DurationMs:  float64(dur.Microseconds()) / 1000.0,
				})
				fmt.Printf("Phase 9c: Fetch opt manifest        %8.1fms  ← HTTP\n", float64(dur.Microseconds())/1000.0)

				if err == nil {
					var manifest ocispec.Manifest
					bytes, _ := io.ReadAll(rc)
					rc.Close()
					json.Unmarshal(bytes, &manifest)

					if manifest.Subject != nil && manifest.Subject.Digest == manifestDigest {
						fmt.Printf("          Subject matches ✓\n")

						if len(manifest.Layers) > 0 {
							metaLayer := manifest.Layers[len(manifest.Layers)-1]
							if label.IsNydusMetaLayer(metaLayer.Annotations) {
								fmt.Printf("          Bootstrap: %s\n", metaLayer.Digest)

								start = time.Now()
								blobRc, err := optFetcher.Fetch(ctx, metaLayer)
								dur = time.Since(start)
								timings = append(timings, TimingResult{
									Phase:       "9d-fetch-bootstrap",
									Description: "Fetch bootstrap blob (HTTP GET)",
									DurationMs:  float64(dur.Microseconds()) / 1000.0,
								})
								fmt.Printf("Phase 9d: Fetch bootstrap           %8.1fms  ← HTTP\n", float64(dur.Microseconds())/1000.0)

								if err == nil {
									io.Copy(io.Discard, blobRc)
									blobRc.Close()
								}
							}
						}
					}
				}
			}
		}
	}

	totalDuration := time.Since(totalStart)

	fmt.Println()
	fmt.Println("════════════════════════════════════════════════════════════════════")
	fmt.Printf("TOTAL TIME: %.3fs (%.1fms)\n", totalDuration.Seconds(), float64(totalDuration.Microseconds())/1000.0)
	fmt.Println("════════════════════════════════════════════════════════════════════")
	fmt.Println()

	// Sum up HTTP phases
	var httpTotal float64
	for _, t := range timings {
		if strings.Contains(t.Description, "HTTP") {
			httpTotal += t.DurationMs
		}
	}
	fmt.Printf("HTTP requests only: %.1fms\n", httpTotal)
	fmt.Printf("Overhead (auth, resolver, etc): %.1fms\n", float64(totalDuration.Microseconds())/1000.0-httpTotal)
}

func extractTag(refspec reference.Spec) string {
	if refspec.Object == "" {
		return "latest"
	}
	if strings.HasPrefix(refspec.Object, "@") {
		return ""
	}
	if idx := strings.Index(refspec.Object, "@"); idx > 0 {
		return refspec.Object[:idx]
	}
	return refspec.Object
}
