#!/bin/bash
#
# Quick Referrer API Timing Test
#
# This is a minimal test to quickly validate if GAR's referrers API is slow.
#
# Usage:
#   # For GAR:
#   ./quick-test.sh gar <digest>
#
#   # For ECR:
#   ./quick-test.sh ecr <digest>
#
# Examples:
#   ./quick-test.sh gar sha256:abc123...
#   ./quick-test.sh ecr sha256:def456...
#

set -euo pipefail

REGISTRY_TYPE="${1:-}"
DIGEST="${2:-}"

usage() {
    echo "Usage: $0 <gar|ecr> <digest>"
    echo ""
    echo "Examples:"
    echo "  $0 gar sha256:ae7937418a56fdb730bda165d004908b..."
    echo "  $0 ecr sha256:..."
    echo ""
    echo "First, get the digest of your image:"
    echo "  # For GAR:"
    echo "  gcloud artifacts docker images describe us-docker.pkg.dev/PROJECT/REPO/IMAGE:TAG --format='value(image_summary.digest)'"
    echo ""
    echo "  # For ECR:"
    echo "  aws ecr describe-images --repository-name REPO --image-ids imageTag=TAG --query 'imageDetails[0].imageDigest' --output text"
    exit 1
}

if [[ -z "$REGISTRY_TYPE" ]] || [[ -z "$DIGEST" ]]; then
    usage
fi

# Timing format
FORMAT='
  HTTP Status:     %{http_code}
  DNS Lookup:      %{time_namelookup}s
  TCP Connect:     %{time_connect}s
  TLS Handshake:   %{time_appconnect}s
  TTFB:            %{time_starttransfer}s  ← KEY METRIC
  Total Time:      %{time_total}s
  Downloaded:      %{size_download} bytes
'

case "$REGISTRY_TYPE" in
    gar)
        echo "=== Testing GAR Referrers API ==="
        echo ""

        # Get token
        TOKEN=$(gcloud auth print-access-token)
        if [[ -z "$TOKEN" ]]; then
            echo "ERROR: Could not get GAR token. Run: gcloud auth login"
            exit 1
        fi

        # GAR settings - UPDATE THESE
        REGISTRY="us-docker.pkg.dev"
        REPO="dogfood-gcp-dataplane/orgs/dogfood-gcp/flyte"

        URL="https://${REGISTRY}/v2/${REPO}/referrers/${DIGEST}"
        echo "URL: $URL"
        echo ""
        echo "Timing:"

        curl -s -o /dev/null -w "$FORMAT" \
            -H "Authorization: Bearer $TOKEN" \
            -H "Accept: application/vnd.oci.image.index.v1+json" \
            --connect-timeout 60 \
            --max-time 120 \
            "$URL"

        echo ""
        echo "If TTFB > 5 seconds, this confirms GAR is slow to respond."
        ;;

    ecr)
        echo "=== Testing ECR Referrers API ==="
        echo ""

        # Get token
        PASSWORD=$(aws ecr get-login-password --region us-east-2)
        if [[ -z "$PASSWORD" ]]; then
            echo "ERROR: Could not get ECR token. Run: aws configure"
            exit 1
        fi
        TOKEN=$(echo -n "AWS:$PASSWORD" | base64)

        # ECR settings - UPDATE THESE
        REGISTRY="992382529030.dkr.ecr.us-east-2.amazonaws.com"
        REPO="union/dogfood"

        URL="https://${REGISTRY}/v2/${REPO}/referrers/${DIGEST}"
        echo "URL: $URL"
        echo ""
        echo "Timing:"

        curl -s -o /dev/null -w "$FORMAT" \
            -H "Authorization: Basic $TOKEN" \
            -H "Accept: application/vnd.oci.image.index.v1+json" \
            --connect-timeout 60 \
            --max-time 120 \
            "$URL"

        echo ""
        echo "Compare this TTFB with GAR's TTFB to see the difference."
        ;;

    *)
        echo "Unknown registry type: $REGISTRY_TYPE"
        usage
        ;;
esac
