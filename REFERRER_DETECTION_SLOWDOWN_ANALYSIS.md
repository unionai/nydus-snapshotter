# Referrer Detection Slowdown Analysis Report

## Executive Summary

This report investigates the 10-20 second latency observed during container creation and startup when referrer detection is enabled in nydus-snapshotter. The slowdown manifests specifically in:
1. **GKE**: Running workloads with vanilla image tags where optimized tags exist
2. **EKS/GKE**: Running workloads with vanilla image tags where optimized tags do NOT exist

### Key Finding from External Testing

**Registry APIs are faster when tested from outside the cluster:**

| Scenario | GAR | ECR | Notes |
|----------|-----|-----|-------|
| Full flow (optimized found via referrers) | **2.2s** | N/A | 4 HTTP requests |
| Full flow (optimized found via tag-based) | **1.5s** | N/A | 3 HTTP requests |
| Detection only (no optimized exists) | **0.85s** | **0.88s** | 2 HTTP requests, then OCI fallback |

**Individual API call latencies:**
| API Call | GAR | ECR |
|----------|-----|-----|
| Referrers API | ~0.4-0.5s | ~0.35s |
| Tag-based HEAD/GET | ~0.4-0.6s | ~0.28-0.4s |
| Bootstrap blob fetch | ~0.4-0.5s | N/A |

**The 10-20s observed delay vs 1.5-2.2s external suggests a 5-10x slowdown inside the cluster.** Possible causes: auth token acquisition, CRI keychain overhead, DNS resolution, or connection establishment.

Five code-explorer agents independently analyzed different aspects of the codebase. This report synthesizes their findings, presents hypotheses ordered by plausibility, and includes an agreement matrix.

---

## Methodology

Five parallel code-explorer agents investigated:
1. **Agent 1**: Referrer detection mechanism
2. **Agent 2**: Registry interaction code (HTTP clients, timeouts, retries)
3. **Agent 3**: Image resolution and tag-based fallback logic
4. **Agent 4**: Container creation and snapshot preparation flow
5. **Agent 5**: Error handling, retry logic, and timeout behavior

---

## Key Findings Summary

### Finding 1: Synchronous, Sequential Operations on Critical Path

The referrer detection flow is **fully synchronous** and blocks container startup:

```
Prepare() → CheckReferrer() → [Network I/O] → TryFetchMetadata() → [Network I/O] → Mount()
```

**Critical path timing breakdown**:
| Operation | Best Case | Typical | Worst Case |
|-----------|-----------|---------|------------|
| Standard Referrers API | 50ms | 200ms | 30s (timeout) |
| Tag-based fallback | 100ms | 500ms | 30s (per suffix) |
| Metadata download | 100ms | 1s | 10s |
| **Total overhead** | 250ms | 1.7s | **60s+** |

### Finding 2: Two-Tier Fallback Strategy with Sequential Execution

When the standard OCI Referrers API fails (common for many registries), the system falls back to tag-based discovery:

```go
// pkg/referrer/referrer.go:48-66
func (r *referrer) checkReferrer(ctx, ref, manifestDigest) {
    desc, err := r.checkReferrerStandard(ctx, ref, manifestDigest)  // Try first
    if err == nil {
        return desc, nil
    }
    return r.checkReferrerTagBased(ctx, ref, manifestDigest)  // Then fallback
}
```

**Problem**: Both phases are sequential, and each can consume the full HTTP timeout (30s).

### Finding 3: No Aggressive Timeouts for Referrer Detection

The HTTP client uses default transport settings with no referrer-specific timeouts:

| Timeout Type | Value | Location |
|--------------|-------|----------|
| Connection establishment | 30s | `config/hosts.go:135` |
| TLS handshake | 10s | `config/hosts.go:141` |
| **Overall HTTP request** | **None** | Relies on context |
| HTTP client (blob redirect) | 60s | `transport/pool.go:21` |

**Key Issue**: No `ResponseHeaderTimeout` or overall request timeout. A slow registry can block indefinitely.

### Finding 4: Immediate Retry Without Backoff

HTTP 408 (Request Timeout) and 429 (Too Many Requests) trigger **immediate retries** without exponential backoff:

```go
// pkg/remote/remotes/docker/resolver.go:607-656
case http.StatusRequestTimeout, http.StatusTooManyRequests:
    return true, nil  // Immediate retry - NO BACKOFF
```

**Maximum 5 retries** hardcoded, but with no delay between them.

### Finding 5: Recent Serialization Fix Adds Blocking

Commit `a59816a` added mutex-based serialization to prevent metadata file clobbering:

```go
// pkg/filesystem/referer_adaptor.go:58-70
mu := fs.getMetadataMutex(metadataPath)
mu.Lock()  // BLOCKS here
defer mu.Unlock()

if _, err := os.Stat(metadataPath); err == nil {
    return nil  // Skip if already exists
}
// ... fetch metadata
```

**Impact**: First container blocks all subsequent containers starting the same image.

### Finding 6: No Circuit Breaker Pattern

Repeated failures don't disable referrer detection. Each container startup independently attempts referrer detection, even if the registry consistently fails.

---

## Agreement Matrix

| Finding | Agent 1 | Agent 2 | Agent 3 | Agent 4 | Agent 5 |
|---------|:-------:|:-------:|:-------:|:-------:|:-------:|
| Synchronous blocking on critical path | ✓ | ✓ | ✓ | ✓ | ✓ |
| Two-tier fallback (std API + tag-based) | ✓ | ✓ | ✓ | - | ✓ |
| No aggressive timeouts | - | ✓ | - | ✓ | ✓ |
| Immediate retry without backoff | - | ✓ | - | - | ✓ |
| Mutex serialization blocks parallel starts | ✓ | - | - | ✓ | - |
| No circuit breaker for failing registries | - | ✓ | - | ✓ | ✓ |
| Tag-based fallback iterates sequentially | ✓ | - | ✓ | - | ✓ |
| LRU cache (500 entries) provides warm path | ✓ | ✓ | ✓ | ✓ | ✓ |
| Singleflight prevents duplicate network calls | ✓ | - | - | ✓ | ✓ |

**Legend**: ✓ = Explicitly identified by agent, - = Not mentioned

---

## Hypotheses (Ordered by Plausibility)

### Hypothesis 1: GAR's Referrers API Returns Slow or Delayed 404s (HIGH CONFIDENCE)

**Plausibility: ★★★★★ (Most Likely)**

**Description**: Google Artifact Registry (GAR) may not support the OCI Referrers API or returns slow 404 responses. This forces the system through the full timeout before falling back to tag-based discovery.

**Evidence**:
- GKE slowdowns occur with vanilla tags where optimized tags exist
- ECR (which supports referrers) shows faster behavior on the same workloads
- Code shows standard API is tried first, only then falls back (`referrer.go:48-66`)
- No GAR-specific handling or timeout optimization in the codebase

**Timeline Analysis**:
```
Container Start (GKE + vanilla tag + optimized exists)
├─ checkReferrerStandard() → GAR Referrers API
│  └─ Wait 5-30 seconds for 404/timeout from GAR
├─ checkReferrerTagBased() → Try registry:tag-opt
│  ├─ Resolve → 200 OK (found!)
│  └─ Fetch manifest + validate → Success
├─ TryFetchMetadata() → Download image.boot
│  └─ 1-5 seconds
└─ Total: 6-35 seconds (mostly waiting for standard API failure)
```

**Why this explains the scenarios**:
- **GKE + optimized tag exists**: Slow 404 from standard API + successful tag fallback = 10-20s
- **EKS + no optimized tag**: ECR's faster 404 + tag fallback 404 = faster (less slowdown)
- **EKS + optimized tag exists**: ECR may support referrers API = fast path

**Recommended Fix**: Add short timeout (2-5s) specifically for the standard Referrers API before falling back to tag-based discovery.

---

### Hypothesis 2: Full HTTP Timeout Consumed When Optimized Image Doesn't Exist (HIGH CONFIDENCE)

**Plausibility: ★★★★☆**

**Description**: When no optimized image exists at all, both the standard API and tag-based fallback must fail, consuming up to 60 seconds of timeout waiting.

**Evidence**:
- HTTP connection timeout: 30s (`config/hosts.go:135`)
- No overall request timeout configured
- Sequential fallback: Standard API (up to 30s) → Tag-based (up to 30s) = 60s max
- Multiple tag suffixes iterate sequentially (each can timeout)

**Timeline Analysis**:
```
Container Start (no optimized image anywhere)
├─ checkReferrerStandard() → Registry Referrers API
│  └─ 404 returned (50ms - 30s depending on registry)
├─ checkReferrerTagBased() → Try registry:tag-opt
│  ├─ Resolve → 404 (50ms - 30s)
│  └─ (repeat for each configured suffix)
├─ Return to snapshotter → Fall back to OCI pull
└─ Total: 1-60 seconds wasted
```

**Why this explains the scenarios**:
- **No optimized tag exists**: Must try all paths before giving up
- Different registries have different latencies for 404 responses
- EKS/GKE both affected, but registry latency characteristics differ

**Recommended Fix**:
1. Add aggressive timeout (5s) for referrer detection operations
2. Parallelize tag suffix attempts instead of sequential iteration

---

### Hypothesis 3: Metadata Fetch Mutex Creates Convoy Effect (LOW CONFIDENCE - UNLIKELY)

**Plausibility: ★☆☆☆☆**

**Description**: When multiple containers start simultaneously on the same image, the per-path mutex in `TryFetchMetadata()` creates a convoy effect where all containers wait for the first one.

**Why this is UNLIKELY to cause 10-20s delays**:
- The metadata file (`image.boot`) is small - typically a few hundred KB to a few MB
- Download time is fast: <1-2 seconds even on slower networks
- Mutex only blocks during this short download
- Subsequent containers hit file-exists check and return immediately

**Evidence against**:
- First container: ~1s download (not 10-20s)
- Second+ containers: millisecond file-exists check
- The serialization fix is correct behavior - it prevents file corruption without significant performance impact

**Conclusion**: The 10-20s delay must occur **before** `TryFetchMetadata()` - in the `CheckReferrer()` network calls (Hypotheses 1 and 2).

---

### Hypothesis 4: Retry Storm on Rate-Limited Registries (MEDIUM CONFIDENCE)

**Plausibility: ★★★☆☆**

**Description**: When registries rate-limit requests (429), immediate retries without backoff can extend the overall operation time.

**Evidence**:
- 429 triggers immediate retry (`resolver.go:650`)
- Maximum 5 retries, no delay between them
- No exponential backoff implementation at HTTP layer
- Could trigger extended rate limiting from registry

**Timeline Analysis**:
```
Container Start (registry under load)
├─ Request 1 → 429 (rate limited)
├─ Immediate retry → 429
├─ Immediate retry → 429
├─ Immediate retry → 429
├─ Immediate retry → 429 (5th attempt, give up)
└─ Fall through to next phase...
```

**Why this might explain the scenarios**:
- Cloud registries (GAR, ECR) may rate limit during high traffic
- Multiple containers starting simultaneously could trigger rate limits
- Cluster-wide burst of container starts = registry pressure

**Recommended Fix**: Implement exponential backoff with jitter for 429/408 responses.

---

### Hypothesis 5: Docker Reference Parsing Edge Cases with GAR (LOWER CONFIDENCE)

**Plausibility: ★★☆☆☆**

**Description**: Recent commits (`d9fb349`, `97dccc3`) fixed edge cases in Docker reference parsing. GAR's reference format might still trigger slow paths.

**Evidence**:
- Commit history shows active fixes for reference parsing
- GAR uses different reference formats than ECR
- Tag-based fallback depends on correctly parsing original reference
- If parsing fails or produces wrong candidates, extra network round-trips occur

**Timeline Analysis**:
```
Container Start (GAR with unusual reference format)
├─ parseTagFromReference() → Might return wrong tag
├─ generateReferrerCandidates() → Wrong candidates generated
├─ validateTagBasedReferrer() → Multiple 404s trying wrong tags
└─ Eventually gives up or accidentally finds right tag
```

**Why this might explain the scenarios**:
- ECR uses standard Docker reference format (works well)
- GAR might use different format requiring special handling
- Recent fixes suggest this area was problematic

**Recommended Fix**: Add extensive logging to reference parsing and validate GAR reference formats.

---

## Summary Table (REVISED after external testing)

| Hypothesis | Confidence | Status | Key Evidence |
|------------|------------|--------|--------------|
| H1: GAR slow 404s | ~~★★★★★~~ → ★☆☆☆☆ | **DISPROVEN** | GAR returns in ~0.4s from external |
| H2: Full timeout consumed | ~~★★★★☆~~ → ★★☆☆☆ | **UNLIKELY** | APIs respond fast, no timeouts hit |
| H3: Mutex convoy effect | ★☆☆☆☆ | UNLIKELY | Metadata download is fast (<2s) |
| H4: Retry storm | ★★★☆☆ | POSSIBLE | Still needs in-cluster validation |
| H5: Reference parsing | ★★☆☆☆ | POSSIBLE | Recent parsing fixes |
| **H6: In-cluster overhead** | **★★★★★** | **NEW - LIKELY** | External tests fast, in-cluster slow |

### New Hypothesis: In-Cluster Overhead (H6) - PARTIALLY CONFIRMED

The 10-20s delay is NOT from registry API latency. **Credential helper overhead is a significant factor.**

#### Test Harness Results (Docker on macOS)

| Registry | Auth Method | Keychain Time | HTTP Time | Total |
|----------|-------------|---------------|-----------|-------|
| **GAR** | gcloud credential helper | **755ms** | 2044ms | **4.4s** |
| **ECR** | Direct token in docker config | **2ms** | 1460ms | **1.5s** |

**Key Finding**: The gcloud credential helper adds **~750ms overhead per invocation**. This is called every time `auth.GetKeyChainByRef()` runs.

#### Likely causes of 10-20s in-cluster delay:

1. **Auth Token Acquisition** - GKE Workload Identity token exchange may be slower than gcloud credential helper
2. **CRI Keychain Proxy** - `enable_cri_keychain = true` routes through containerd, adding latency
3. **Multiple credential helper invocations** - Each resolver/fetcher creation may trigger auth
4. **DNS Resolution** - First DNS lookup for registry from within pod
5. **TLS Connection Cold Start** - Establishing new TLS connections

---

## Recommended Investigation Steps

### Immediate (Diagnostic) - GO TEST HARNESS AVAILABLE

A Go test harness has been created at `tools/referrer-timing/main.go` that uses the **exact same code paths** as nydus-snapshotter. This provides accurate timing for each phase of referrer detection.

**External testing completed** - registries respond fast (0.3-0.6s per request).

**Next step: Run the test harness inside GKE/EKS clusters** to compare in-cluster vs external timing.

```bash
# Build for Linux
cd tools/referrer-timing
GOOS=linux GOARCH=amd64 go build -o referrer-timing-linux .

# Build Docker image
docker build -t YOUR_REGISTRY/referrer-timing:latest .
docker push YOUR_REGISTRY/referrer-timing:latest

# Deploy to GKE (edit k8s-job-gke.yaml first)
kubectl apply -f k8s-job-gke.yaml
kubectl logs -f job/referrer-timing
```

See `tools/referrer-timing/README.md` for complete instructions.

### Alternative: Add timing logs to nydus-snapshotter

If the test harness is not conclusive, add timing logs directly:

```go
// In pkg/referrer/referrer.go:checkReferrer()
start := time.Now()
defer func() { log.Infof("checkReferrer took %v", time.Since(start)) }()
```

### Short-term (Code Changes)

1. **Add aggressive timeout** for referrer operations:
   ```go
   ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
   defer cancel()
   ```

2. **Parallelize tag suffix attempts** using `errgroup`:
   ```go
   g, ctx := errgroup.WithContext(ctx)
   for _, suffix := range suffixes {
       g.Go(func() error { return tryCandidate(ctx, suffix) })
   }
   ```

3. **Move file existence check** before mutex acquisition in `TryFetchMetadata()`

### Long-term (Architecture)

1. **Implement circuit breaker** for registries that consistently fail referrer detection
2. **Add background prefetch** during image pull instead of blocking container start
3. **Add metrics** for referrer detection success/failure/latency by registry type

---

## Files to Examine for Fixes

| File | Lines | Purpose |
|------|-------|---------|
| `pkg/referrer/referrer.go` | 48-66 | Add timeout to checkReferrer |
| `pkg/referrer/referrer.go` | 123-140 | Parallelize tag candidates |
| `pkg/filesystem/referer_adaptor.go` | 47-78 | Optimize mutex handling |
| `pkg/remote/remotes/docker/resolver.go` | 607-656 | Add exponential backoff |
| `config/config.go` | 126-132 | Add timeout configuration |

---

## Conclusion

The most likely cause of the 10-20 second slowdown is **Hypothesis 1**: GAR's Referrers API returns slow or delayed 404 responses, forcing the system to wait through the full timeout before falling back to tag-based discovery.

This is compounded by:
- No aggressive timeouts for referrer-specific operations
- Sequential (not parallel) fallback attempts
- No circuit breaker to skip consistently failing registries

The recommended immediate fix is to add a short timeout (2-5 seconds) specifically for the standard Referrers API call, allowing faster fallback to tag-based discovery when the registry doesn't support or is slow to respond to the Referrers API.
