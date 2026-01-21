# Referrer Detection Timing Analysis

This directory contains tools to debug and understand the referrer detection latency in nydus-snapshotter.

## Understanding the Referrer Detection Flow

When `enable_referrer_detect = true` in config, nydus-snapshotter attempts to find an optimized Nydus image for any vanilla OCI image being pulled. Here's the exact sequence:

### Phase 1: Standard OCI Referrers API

```
checkReferrerStandard() [pkg/referrer/referrer.go:69]
    │
    ▼
FetchReferrers() [pkg/remote/remotes/docker/referrers.go:32]
    │
    ├─► Request 1: GET /v2/{repo}/referrers/{digest}
    │       │
    │       ├─ If 200: Parse index, fetch first referrer manifest
    │       │          └─► Request 2: GET /v2/{repo}/manifests/{referrer-digest}
    │       │
    │       └─ If 404: Try OCI fallback tag format
    │                  └─► Request 2: GET /v2/{repo}/manifests/sha256-{digest}
    │
    └─► If all fail: Return error → triggers Phase 2
```

### Phase 2: Tag-Based Fallback

```
checkReferrerTagBased() [pkg/referrer/referrer.go:123]
    │
    ▼
For each suffix in referrer_tag_suffixes (default: ["-opt"]):
    │
    ├─► generateReferrerCandidates(): image:tag → image:tag-opt
    │
    └─► validateTagBasedReferrer() [pkg/referrer/referrer.go:142]
            │
            ├─► Request 1: HEAD /v2/{repo}/manifests/{tag}-opt  (resolve)
            │       │
            │       └─ If 404: Continue to next suffix
            │
            ├─► Request 2: GET /v2/{repo}/manifests/{tag}-opt   (fetch)
            │
            └─► Validate: manifest.Subject.Digest == original digest
                         manifest has nydus-bootstrap annotation
```

### Phase 3: Metadata Fetch (if referrer found)

```
fetchMetadata() [pkg/referrer/referrer.go:246]
    │
    └─► Request: GET /v2/{repo}/blobs/{metadata-layer-digest}
            │
            └─► Extract image/image.boot from layer tarball
```

## Why Specific Scenarios Are Slow

### Scenario 1: GKE + vanilla tag + optimized exists = 10-20s

```
Timeline:
T+0.0s   Start checkReferrerStandard()
T+0.0s   GET /v2/repo/referrers/sha256:abc...
         ↓
         [GAR processes request internally]
         [GAR determines no referrers exist]
         [GAR returns 404]
         ↓
T+10-20s Receive 404 response  ← THE BOTTLENECK
T+10-20s Try OCI fallback: GET /v2/repo/manifests/sha256-abc...
T+10-20s Receive 404 (fast)
T+10-20s Start checkReferrerTagBased()
T+10-20s HEAD /v2/repo/manifests/tag-opt → 200 (exists!)
T+10-20s GET /v2/repo/manifests/tag-opt → 200
T+10-20s Validate and return success
T+10-22s Total time: 10-22 seconds
```

**Root Cause**: GAR takes 10-20 seconds to return a 404 for the referrers API.

### Scenario 2: EKS/GKE + vanilla tag + NO optimized = 10-20s

```
Timeline:
T+0.0s   Start checkReferrerStandard()
T+0.0s   GET /v2/repo/referrers/sha256:abc...
         ↓
         [Registry returns 404]
         ↓
T+0.5-20s Receive 404 (ECR: fast, GAR: slow)
T+0.5-20s Try OCI fallback: GET /v2/repo/manifests/sha256-abc... → 404
T+0.5-20s Start checkReferrerTagBased()
T+0.5-20s HEAD /v2/repo/manifests/tag-opt → 404 (doesn't exist)
T+0.5-20s Return "no tag-based referrer found"
T+0.5-20s Fall back to standard OCI pull
```

**Root Cause**:
- On GAR: Slow 404 from referrers API
- On ECR: Possibly slow 404 from referrers API (needs testing)
- Both: No optimized image to find, so time is "wasted"

### Scenario 3: EKS + vanilla tag + optimized exists = Fast

```
Timeline:
T+0.0s   Start checkReferrerStandard()
T+0.0s   GET /v2/repo/referrers/sha256:abc... → 404 (fast!)
T+0.5s   Try OCI fallback → 404 (fast)
T+0.5s   Start checkReferrerTagBased()
T+0.5s   HEAD /v2/repo/manifests/tag-opt → 200
T+0.8s   GET /v2/repo/manifests/tag-opt → 200
T+1.0s   Success!
```

**Why Fast**: ECR returns 404 quickly, so tag-based fallback happens immediately.

## Key Insight: TTFB (Time To First Byte)

The critical metric is **TTFB** - how long until the registry sends the HTTP response status line.

| Registry | Expected TTFB for Referrers 404 |
|----------|--------------------------------|
| ECR | < 1 second |
| GAR | **10-20 seconds** (hypothesis) |
| Docker Hub | < 2 seconds |

## Test Scripts

### quick-test.sh

Minimal test to measure TTFB for the referrers API:

```bash
# First, get the digest of your image:

# For GAR:
gcloud artifacts docker images describe \
  us-docker.pkg.dev/dogfood-gcp-dataplane/orgs/dogfood-gcp/flyte:TAG \
  --format='value(image_summary.digest)'

# For ECR:
aws ecr describe-images \
  --repository-name union/dogfood \
  --image-ids imageTag=flyte-4226d5b6bc1d573fe3264aeaa1792702 \
  --query 'imageDetails[0].imageDigest' \
  --output text

# Then run the test:
./quick-test.sh gar sha256:abc123...
./quick-test.sh ecr sha256:def456...
```

### test-referrer-timing.sh

Comprehensive test that simulates the full referrer detection flow:

```bash
./test-referrer-timing.sh [--verbose]
```

This tests:
1. Standard Referrers API endpoint
2. OCI fallback tag format
3. Tag-based discovery
4. Full simulation of the referrer detection flow

## Expected Results

If Hypothesis 1 is correct (GAR slow 404s):

```
GAR Referrers API:
  TTFB: 12.345s  ← SLOW!
  Total: 12.456s

ECR Referrers API:
  TTFB: 0.234s   ← Fast
  Total: 0.345s
```

## Fixing the Issue

Based on the analysis, the recommended fix is:

1. **Add aggressive timeout (2-5s) for Standard Referrers API**
   - If registry doesn't respond within 2s, skip to tag-based fallback
   - Most registries that support referrers respond in < 1s

2. **Keep reasonable timeout (10-15s) for tag-based fallback**
   - This is the likely success path
   - Needs time for manifest resolution and validation

3. **Add overall timeout (15-20s) for entire referrer detection**
   - Prevents unbounded waiting
   - Allows graceful fallback to OCI pull

## Image References for Testing

### GAR (has optimized version)
- Vanilla: `us-docker.pkg.dev/dogfood-gcp-dataplane/orgs/dogfood-gcp/flyte:ae7937418a56fdb730bda165d004908b`
- Optimized: `us-docker.pkg.dev/dogfood-gcp-dataplane/orgs/dogfood-gcp/flyte:ae7937418a56fdb730bda165d004908b-opt`

**Note**: The tag `ae7937418a56fdb730bda165d004908b` is a build/commit ID, not a digest.
The tag-based fallback appends `-opt` to find the optimized version.

### ECR (no optimized version)
- Vanilla: `992382529030.dkr.ecr.us-east-2.amazonaws.com/union/dogfood:flyte-4226d5b6bc1d573fe3264aeaa1792702`
- No optimized version exists (tag-based fallback will fail)

## HTTP Client Timeout Analysis

Current timeout configuration in nydus-snapshotter:

| Timeout | Value | Where |
|---------|-------|-------|
| TCP Connect | 30s | `config/hosts.go:135` |
| TLS Handshake | 10s | `config/hosts.go:141` |
| **Response Headers** | **∞** | NOT SET |
| **Overall Request** | **∞** | NOT SET |

This means once TCP+TLS complete, the client waits **forever** for a response.

---

## Go Test Harness (main.go)

The Go test harness uses the **exact same code paths** as nydus-snapshotter for referrer detection. This allows accurate measurement of:

- Reference parsing
- Authentication keychain acquisition
- Resolver creation
- HTTP requests (resolve, fetch referrers, fetch manifests)
- Bootstrap blob fetching

### Building

```bash
cd tools/referrer-timing

# Build for Linux (required for in-cluster testing)
GOOS=linux GOARCH=amd64 go build -o referrer-timing-linux .

# Build for local testing (macOS/Linux)
go build -o referrer-timing .
```

### Local Testing

```bash
# With gcloud credentials configured
./referrer-timing -ref us-docker.pkg.dev/dogfood-gcp-dataplane/orgs/dogfood-gcp/flyte:ae7937418a56fdb730bda165d004908b

# With AWS credentials configured
AWS_PROFILE=dogfood-2 ./referrer-timing -ref 992382529030.dkr.ecr.us-east-2.amazonaws.com/union/dogfood:flyte-4226d5b6bc1d573fe3264aeaa1792702

# Verbose output
./referrer-timing -ref IMAGE_REF -verbose
```

### In-Cluster Testing (GKE)

This is the critical test - comparing in-cluster timing vs external timing to identify overhead.

1. **Build and push the container image:**

```bash
cd tools/referrer-timing

# Build the Linux binary
GOOS=linux GOARCH=amd64 go build -o referrer-timing-linux .

# Build and push the Docker image
docker build -t us-docker.pkg.dev/YOUR_PROJECT/YOUR_REPO/referrer-timing:latest .
docker push us-docker.pkg.dev/YOUR_PROJECT/YOUR_REPO/referrer-timing:latest
```

2. **Configure Workload Identity:**

```bash
# Create GCP service account (if needed)
gcloud iam service-accounts create referrer-timing-sa \
  --display-name="Referrer Timing Test SA"

# Grant artifact registry read access
gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:referrer-timing-sa@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"

# Allow Kubernetes SA to impersonate GCP SA
gcloud iam service-accounts add-iam-policy-binding \
  referrer-timing-sa@YOUR_PROJECT.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:YOUR_PROJECT.svc.id.goog[default/referrer-timing-sa]"
```

3. **Edit and deploy the Job:**

```bash
# Edit k8s-job-gke.yaml to set your project/SA details
kubectl apply -f k8s-job-gke.yaml

# Watch the logs
kubectl logs -f job/referrer-timing
```

4. **Compare results:**

| Metric | External | In-Cluster | Delta |
|--------|----------|------------|-------|
| Phase 5: Resolve reference | ? ms | ? ms | ? |
| Phase 7: Fetch referrers | ? ms | ? ms | ? |
| Total | ? ms | ? ms | ? |

### In-Cluster Testing (EKS)

1. **Build and push to ECR:**

```bash
cd tools/referrer-timing

# Authenticate with ECR
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 992382529030.dkr.ecr.us-east-2.amazonaws.com

# Create repository (if needed)
aws ecr create-repository --repository-name referrer-timing --region us-east-2

# Build and push
GOOS=linux GOARCH=amd64 go build -o referrer-timing-linux .
docker build -t 992382529030.dkr.ecr.us-east-2.amazonaws.com/referrer-timing:latest .
docker push 992382529030.dkr.ecr.us-east-2.amazonaws.com/referrer-timing:latest
```

2. **Configure IRSA:**

```bash
eksctl create iamserviceaccount \
  --name referrer-timing-sa \
  --namespace default \
  --cluster YOUR_CLUSTER \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly \
  --approve
```

3. **Deploy and monitor:**

```bash
# Edit k8s-job-eks.yaml to set your IAM role ARN
kubectl apply -f k8s-job-eks.yaml
kubectl logs -f job/referrer-timing
```

---

## Expected Results

### External Testing (curl from laptop)

From curl-based testing, we observed:

| Scenario | GAR | ECR |
|----------|-----|-----|
| Full flow (optimized found via referrers) | ~2.2s | N/A |
| Full flow (optimized found via tag-based) | ~1.5s | N/A |
| Detection only (no optimized exists) | ~0.85s | ~0.88s |

Individual API latencies:
| API Call | GAR | ECR |
|----------|-----|-----|
| Referrers API | ~0.4-0.5s | ~0.35s |
| Tag-based HEAD/GET | ~0.4-0.6s | ~0.28-0.4s |

### Go Test Harness Results (Docker on macOS)

Using the actual nydus-snapshotter code paths:

| Registry | Auth Method | Keychain (Phase 2) | Resolve (Phase 5) | Referrers (Phase 7) | Total |
|----------|-------------|-------------------|-------------------|---------------------|-------|
| **GAR** | gcloud credential helper | **755ms** | 1171ms | 873ms | **4.4s** |
| **ECR** | Direct token in docker config | **2ms** | 631ms | 492ms | **1.5s** |

**Key Finding**: The gcloud credential helper adds **~750ms overhead** per invocation.

#### GAR Detailed Breakdown (optimized image found via referrers API)
```
Phase 1:  Parse reference                0.7ms
Phase 2:  Get keychain (auth)          754.7ms  ← CREDENTIAL HELPER OVERHEAD
Phase 3:  Create remote                  0.0ms
Phase 4:  Create resolver                0.6ms
Phase 5:  Resolve reference           1171.4ms  ← HTTP HEAD
Phase 6:  Create fetcher                 0.4ms
Phase 7:  Fetch referrers API          872.5ms  ← HTTP GET referrers
Phase 8:  Parse referrers                1.7ms
Phase 9:  Fetch referrer manifest        0.5ms  ← HTTP GET (cached conn)
Phase 10: Fetch bootstrap blob           0.0ms  ← HTTP GET (cached conn)
TOTAL: 4.4s
```

#### ECR Detailed Breakdown (no optimized image - tag fallback fails)
```
Phase 1:  Parse reference                1.3ms
Phase 2:  Get keychain (auth)            2.0ms  ← DIRECT TOKEN (FAST)
Phase 3:  Create remote                  0.0ms
Phase 4:  Create resolver                0.3ms
Phase 5:  Resolve reference            630.8ms  ← HTTP HEAD
Phase 6:  Create fetcher                 0.3ms
Phase 7:  Fetch referrers API          491.5ms  ← HTTP GET (empty list)
Phase 9b: Resolve opt tag              337.7ms  ← HTTP HEAD (404)
TOTAL: 1.5s
```

### In-Cluster Testing

If in-cluster testing shows significantly higher latencies (5-10x), the bottleneck is likely:

1. **Auth token acquisition** - GKE Workload Identity or EKS IRSA token exchange
2. **CRI keychain proxy** - `enable_cri_keychain = true` routes auth through containerd
3. **DNS resolution** - First DNS lookup for registry hostname
4. **TLS connection cold start** - New TLS handshake for each connection

To diagnose further, run the test multiple times:
- First run: Cold start (auth + DNS + TLS)
- Second run: Warm (cached credentials, connection pooling)

```bash
# Delete and recreate job for multiple runs
kubectl delete job referrer-timing
kubectl apply -f k8s-job-gke.yaml
```
