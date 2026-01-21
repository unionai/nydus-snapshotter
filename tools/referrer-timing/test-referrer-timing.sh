#!/bin/bash
#
# Referrer Detection Timing Test Script
#
# This script tests the HTTP endpoints used by nydus-snapshotter's referrer detection
# to identify where latency is occurring.
#
# Usage:
#   ./test-referrer-timing.sh [--verbose]
#
# Prerequisites:
#   - gcloud CLI authenticated (for GAR)
#   - aws CLI authenticated (for ECR)
#   - curl, jq installed
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

VERBOSE=${1:-""}

# =============================================================================
# Test Image Configuration
# =============================================================================

# GAR Images (tag-based, not digest-based)
GAR_REGISTRY="us-docker.pkg.dev"
GAR_REPO="dogfood-gcp-dataplane/orgs/dogfood-gcp/flyte"
GAR_VANILLA_TAG="ae7937418a56fdb730bda165d004908b"
GAR_OPT_TAG="ae7937418a56fdb730bda165d004908b-opt"

# ECR Images (no optimized version exists)
ECR_REGISTRY="992382529030.dkr.ecr.us-east-2.amazonaws.com"
ECR_REPO="union/dogfood"
ECR_TAG="flyte-4226d5b6bc1d573fe3264aeaa1792702"

# =============================================================================
# Helper Functions
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_header() {
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN} $1${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════════${NC}"
}

log_subheader() {
    echo ""
    echo -e "${BLUE}─── $1 ───${NC}"
}

# Get timing metrics from curl
# Returns JSON with timing data
curl_timing() {
    local url="$1"
    local auth_header="$2"
    local method="${3:-GET}"
    local accept="${4:-application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, */*}"

    local format='{"dns":"%{time_namelookup}","connect":"%{time_connect}","tls":"%{time_appconnect}","ttfb":"%{time_starttransfer}","total":"%{time_total}","status":"%{http_code}","size":"%{size_download}"}'

    if [[ "$VERBOSE" == "--verbose" ]]; then
        echo -e "    ${BLUE}→${NC} $method $url" >&2
    fi

    curl -s -o /dev/null -w "$format" \
        -X "$method" \
        -H "Authorization: $auth_header" \
        -H "Accept: $accept" \
        --connect-timeout 60 \
        --max-time 120 \
        "$url" 2>/dev/null || echo '{"dns":"0","connect":"0","tls":"0","ttfb":"0","total":"0","status":"000","size":"0"}'
}

# Format timing result for display
format_timing() {
    local json="$1"
    local ttfb=$(echo "$json" | jq -r '.ttfb')
    local total=$(echo "$json" | jq -r '.total')
    local status=$(echo "$json" | jq -r '.status')

    # Convert to milliseconds
    local ttfb_ms=$(printf "%.0f" $(echo "$ttfb * 1000" | bc 2>/dev/null || echo "0"))
    local total_ms=$(printf "%.0f" $(echo "$total * 1000" | bc 2>/dev/null || echo "0"))

    # Color code based on speed
    local ttfb_color="$GREEN"
    if (( ttfb_ms > 2000 )); then
        ttfb_color="$RED"
    elif (( ttfb_ms > 500 )); then
        ttfb_color="$YELLOW"
    fi

    printf "Status: %s | TTFB: ${ttfb_color}%dms${NC} | Total: %dms" "$status" "$ttfb_ms" "$total_ms"
}

# Get detailed timing breakdown
format_timing_detail() {
    local json="$1"
    local dns=$(echo "$json" | jq -r '.dns')
    local connect=$(echo "$json" | jq -r '.connect')
    local tls=$(echo "$json" | jq -r '.tls')
    local ttfb=$(echo "$json" | jq -r '.ttfb')
    local total=$(echo "$json" | jq -r '.total')

    local dns_ms=$(printf "%.0f" $(echo "$dns * 1000" | bc 2>/dev/null || echo "0"))
    local connect_ms=$(printf "%.0f" $(echo "$connect * 1000" | bc 2>/dev/null || echo "0"))
    local tls_ms=$(printf "%.0f" $(echo "$tls * 1000" | bc 2>/dev/null || echo "0"))
    local ttfb_ms=$(printf "%.0f" $(echo "$ttfb * 1000" | bc 2>/dev/null || echo "0"))
    local total_ms=$(printf "%.0f" $(echo "$total * 1000" | bc 2>/dev/null || echo "0"))

    echo "    DNS: ${dns_ms}ms → Connect: ${connect_ms}ms → TLS: ${tls_ms}ms → TTFB: ${ttfb_ms}ms → Total: ${total_ms}ms"
}

# Check if TTFB is slow (> 2 seconds)
is_slow_ttfb() {
    local json="$1"
    local threshold="${2:-2.0}"
    local ttfb=$(echo "$json" | jq -r '.ttfb')
    local result=$(echo "$ttfb > $threshold" | bc 2>/dev/null || echo "0")
    [[ "$result" == "1" ]]
}

get_ttfb_seconds() {
    local json="$1"
    echo "$json" | jq -r '.ttfb'
}

get_status() {
    local json="$1"
    echo "$json" | jq -r '.status'
}

# =============================================================================
# Authentication
# =============================================================================

get_gar_token() {
    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI not found"
        return 1
    fi

    local token
    token=$(gcloud auth print-access-token 2>/dev/null)
    if [[ -z "$token" ]]; then
        log_error "Failed to get GAR token. Run: gcloud auth login"
        return 1
    fi
    echo "Bearer $token"
}

get_ecr_token() {
    if ! command -v aws &> /dev/null; then
        log_error "aws CLI not found"
        return 1
    fi

    local token
    token=$(aws ecr get-login-password --region us-east-2 2>/dev/null)
    if [[ -z "$token" ]]; then
        log_error "Failed to get ECR token. Run: aws configure"
        return 1
    fi
    echo "Basic $(echo -n "AWS:$token" | base64)"
}

# =============================================================================
# Resolve manifest digest from tag
# =============================================================================

resolve_digest() {
    local registry="$1"
    local repo="$2"
    local tag="$3"
    local auth="$4"

    local url="https://${registry}/v2/${repo}/manifests/${tag}"

    local digest
    digest=$(curl -s -I \
        -H "Authorization: $auth" \
        -H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" \
        "$url" 2>/dev/null | grep -i "docker-content-digest" | awk '{print $2}' | tr -d '\r')

    echo "$digest"
}

# =============================================================================
# Test Functions
# =============================================================================

test_standard_referrers_api() {
    local registry="$1"
    local repo="$2"
    local digest="$3"
    local auth="$4"
    local name="$5"

    log_subheader "Standard OCI Referrers API"

    # Request 1: GET /v2/{repo}/referrers/{digest}
    local url="https://${registry}/v2/${repo}/referrers/${digest}"
    log_info "GET /v2/${repo}/referrers/${digest}"

    local result
    result=$(curl_timing "$url" "$auth" "GET")
    echo "    $(format_timing "$result")"
    format_timing_detail "$result"

    local status=$(get_status "$result")
    local ttfb=$(get_ttfb_seconds "$result")

    if is_slow_ttfb "$result" 2.0; then
        echo ""
        log_warn "🐌 SLOW TTFB: ${ttfb}s - This is likely causing container startup delays!"
    fi

    if [[ "$status" == "404" ]]; then
        log_info "→ Registry returned 404 (referrers not found or API not supported)"

        # Request 2: OCI fallback format
        local fallback_tag="${digest//:/-}"
        local fallback_url="https://${registry}/v2/${repo}/manifests/${fallback_tag}"
        echo ""
        log_info "GET /v2/${repo}/manifests/${fallback_tag} (OCI fallback)"

        result=$(curl_timing "$fallback_url" "$auth" "GET")
        echo "    $(format_timing "$result")"

        if is_slow_ttfb "$result" 2.0; then
            log_warn "🐌 SLOW fallback response as well!"
        fi
    elif [[ "$status" == "200" ]]; then
        log_success "→ Referrers API returned data"
    fi

    echo "$result"
}

test_tag_based_discovery() {
    local registry="$1"
    local repo="$2"
    local vanilla_tag="$3"
    local opt_tag="$4"
    local auth="$5"

    log_subheader "Tag-Based Discovery"

    # Request 1: HEAD to check if optimized tag exists
    local url="https://${registry}/v2/${repo}/manifests/${opt_tag}"
    log_info "HEAD /v2/${repo}/manifests/${opt_tag}"

    local result
    result=$(curl_timing "$url" "$auth" "HEAD")
    echo "    $(format_timing "$result")"

    local status=$(get_status "$result")

    if [[ "$status" == "200" ]]; then
        log_success "→ Optimized tag EXISTS"

        # Request 2: GET manifest
        echo ""
        log_info "GET /v2/${repo}/manifests/${opt_tag}"
        result=$(curl_timing "$url" "$auth" "GET")
        echo "    $(format_timing "$result")"
        format_timing_detail "$result"

    elif [[ "$status" == "404" ]]; then
        log_info "→ Optimized tag does NOT exist (404)"

        if is_slow_ttfb "$result" 1.0; then
            log_warn "🐌 Even the 404 is slow!"
        fi
    fi

    echo "$result"
}

# =============================================================================
# Full Simulation
# =============================================================================

simulate_full_referrer_detection() {
    local registry="$1"
    local repo="$2"
    local vanilla_tag="$3"
    local opt_tag="$4"
    local auth="$5"
    local name="$6"

    log_header "FULL SIMULATION: $name"

    echo ""
    log_info "Image: ${registry}/${repo}:${vanilla_tag}"
    log_info "Optimized: ${registry}/${repo}:${opt_tag}"
    echo ""

    # Step 0: Resolve vanilla tag to digest
    log_info "Resolving vanilla tag to digest..."
    local digest
    digest=$(resolve_digest "$registry" "$repo" "$vanilla_tag" "$auth")

    if [[ -z "$digest" ]]; then
        log_error "Could not resolve digest for tag: $vanilla_tag"
        return 1
    fi
    log_success "Digest: $digest"
    echo ""

    local total_start=$(date +%s.%N)
    local phase1_time=0
    local phase2_time=0

    # =========================================================================
    # Phase 1: Standard Referrers API
    # =========================================================================
    echo -e "${YELLOW}▶ PHASE 1: Standard OCI Referrers API${NC}"
    local phase1_start=$(date +%s.%N)

    # Request 1a: Referrers endpoint
    local referrers_url="https://${registry}/v2/${repo}/referrers/${digest}"
    log_info "GET ${referrers_url}"

    local result1
    result1=$(curl_timing "$referrers_url" "$auth" "GET")
    echo "    $(format_timing "$result1")"

    local status1=$(get_status "$result1")
    local phase1_api_failed=false

    if [[ "$status1" != "200" ]]; then
        phase1_api_failed=true

        if is_slow_ttfb "$result1" 2.0; then
            local ttfb1=$(get_ttfb_seconds "$result1")
            echo ""
            log_warn "🐌 BOTTLENECK DETECTED: Waited ${ttfb1}s for 404 response!"
        fi

        # Request 1b: OCI fallback
        local fallback_tag="${digest//:/-}"
        local fallback_url="https://${registry}/v2/${repo}/manifests/${fallback_tag}"
        log_info "GET /v2/${repo}/manifests/${fallback_tag} (OCI fallback)"

        local result1b
        result1b=$(curl_timing "$fallback_url" "$auth" "GET")
        echo "    $(format_timing "$result1b")"
    fi

    local phase1_end=$(date +%s.%N)
    phase1_time=$(echo "$phase1_end - $phase1_start" | bc)
    echo ""
    echo -e "    ${BLUE}Phase 1 Duration: ${phase1_time}s${NC}"

    # =========================================================================
    # Phase 2: Tag-Based Discovery (if Phase 1 failed)
    # =========================================================================
    if [[ "$phase1_api_failed" == "true" ]]; then
        echo ""
        echo -e "${YELLOW}▶ PHASE 2: Tag-Based Discovery${NC}"
        local phase2_start=$(date +%s.%N)

        # Request 2a: HEAD for optimized tag
        local opt_url="https://${registry}/v2/${repo}/manifests/${opt_tag}"
        log_info "HEAD /v2/${repo}/manifests/${opt_tag}"

        local result2a
        result2a=$(curl_timing "$opt_url" "$auth" "HEAD")
        echo "    $(format_timing "$result2a")"

        local status2=$(get_status "$result2a")

        if [[ "$status2" == "200" ]]; then
            # Request 2b: GET manifest
            log_info "GET /v2/${repo}/manifests/${opt_tag}"
            local result2b
            result2b=$(curl_timing "$opt_url" "$auth" "GET")
            echo "    $(format_timing "$result2b")"

            log_success "→ Found optimized image via tag-based discovery!"
        else
            log_info "→ Optimized tag not found (404)"
        fi

        local phase2_end=$(date +%s.%N)
        phase2_time=$(echo "$phase2_end - $phase2_start" | bc)
        echo ""
        echo -e "    ${BLUE}Phase 2 Duration: ${phase2_time}s${NC}"
    fi

    # =========================================================================
    # Summary
    # =========================================================================
    local total_end=$(date +%s.%N)
    local total_time=$(echo "$total_end - $total_start" | bc)

    echo ""
    echo -e "${CYAN}────────────────────────────────────────────────────────────────${NC}"
    echo -e "${CYAN} TIMING SUMMARY${NC}"
    echo -e "${CYAN}────────────────────────────────────────────────────────────────${NC}"
    printf "    Phase 1 (Standard API):     %6.3fs\n" "$phase1_time"
    if [[ "$phase1_api_failed" == "true" ]]; then
        printf "    Phase 2 (Tag-Based):        %6.3fs\n" "$phase2_time"
    fi
    echo "    ─────────────────────────────────────"
    printf "    TOTAL REFERRER DETECTION:   %6.3fs\n" "$total_time"
    echo ""

    # Analysis
    if (( $(echo "$total_time > 5.0" | bc -l) )); then
        log_warn "⚠️  Total time > 5s - This explains container startup delays!"

        if (( $(echo "$phase1_time > 2.0" | bc -l) )); then
            echo ""
            log_error "ROOT CAUSE: Phase 1 (Standard Referrers API) is slow!"
            log_info "The registry takes $(printf "%.1f" $phase1_time)s to return 404."
            log_info "Recommendation: Add 2-3s timeout for Standard API to fail fast."
        fi
    else
        log_success "✓ Total time is acceptable (< 5s)"
    fi
    echo ""
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║       Nydus-Snapshotter Referrer Detection Timing Test          ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    # Check dependencies
    for cmd in curl jq bc; do
        if ! command -v $cmd &> /dev/null; then
            log_error "Required command '$cmd' not found. Please install it."
            exit 1
        fi
    done

    # ==========================================================================
    # Authentication
    # ==========================================================================
    log_header "Authentication"

    local gar_token=""
    local ecr_token=""

    if command -v gcloud &> /dev/null; then
        log_info "Getting GAR token..."
        gar_token=$(get_gar_token 2>/dev/null) || true
        if [[ -n "$gar_token" ]]; then
            log_success "GAR authenticated"
        else
            log_warn "GAR authentication failed"
        fi
    else
        log_warn "gcloud not available, skipping GAR tests"
    fi

    if command -v aws &> /dev/null; then
        log_info "Getting ECR token..."
        ecr_token=$(get_ecr_token 2>/dev/null) || true
        if [[ -n "$ecr_token" ]]; then
            log_success "ECR authenticated"
        else
            log_warn "ECR authentication failed"
        fi
    else
        log_warn "aws CLI not available, skipping ECR tests"
    fi

    # ==========================================================================
    # GAR Tests
    # ==========================================================================
    if [[ -n "$gar_token" ]]; then
        simulate_full_referrer_detection \
            "$GAR_REGISTRY" \
            "$GAR_REPO" \
            "$GAR_VANILLA_TAG" \
            "$GAR_OPT_TAG" \
            "$gar_token" \
            "GAR (vanilla tag with optimized version)"
    fi

    # ==========================================================================
    # ECR Tests
    # ==========================================================================
    if [[ -n "$ecr_token" ]]; then
        # ECR has no optimized version, so tag-based will fail
        simulate_full_referrer_detection \
            "$ECR_REGISTRY" \
            "$ECR_REPO" \
            "$ECR_TAG" \
            "${ECR_TAG}-opt" \
            "$ecr_token" \
            "ECR (vanilla tag, NO optimized version)"
    fi

    # ==========================================================================
    # Final Analysis
    # ==========================================================================
    log_header "ANALYSIS"
    echo ""
    echo "Compare the Phase 1 durations between GAR and ECR:"
    echo ""
    echo "  • If GAR Phase 1 >> ECR Phase 1: GAR's Referrers API is the bottleneck"
    echo "  • If both are slow: Both registries have slow Referrers API"
    echo "  • If both are fast: The issue is elsewhere (unlikely based on symptoms)"
    echo ""
    echo "Recommended fix: Add context.WithTimeout(ctx, 2*time.Second) for"
    echo "the Standard Referrers API call in pkg/referrer/referrer.go:51"
    echo ""
}

main "$@"
