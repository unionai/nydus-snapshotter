#!/bin/bash
#
# Resolve full digests for test images
#
# This script fetches the actual SHA256 digests needed for referrer API testing.
#

set -euo pipefail

echo "=== Resolving Image Digests ==="
echo ""

# GAR image
echo "--- GAR Images ---"
echo ""

GAR_REGISTRY="us-docker.pkg.dev"
GAR_REPO="dogfood-gcp-dataplane/orgs/dogfood-gcp/flyte"

# The provided reference looks like it might be a tag, not a digest
# Let's try to resolve it

if command -v gcloud &> /dev/null; then
    echo "Getting GAR token..."
    GAR_TOKEN=$(gcloud auth print-access-token 2>/dev/null || true)

    if [[ -n "$GAR_TOKEN" ]]; then
        echo ""
        echo "Trying to list tags/digests for GAR repo..."
        echo "Repository: ${GAR_REGISTRY}/${GAR_REPO}"
        echo ""

        # Try to get the manifest for a known tag pattern
        # The reference "ae7937418a56fdb730bda165d004908b" might be a tag

        # First, let's see what the -opt tag resolves to
        OPT_TAG="ae7937418a56fdb730bda165d004908b-opt"
        echo "Resolving optimized tag: ${OPT_TAG}"

        OPT_DIGEST=$(curl -s -I \
            -H "Authorization: Bearer $GAR_TOKEN" \
            -H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" \
            "https://${GAR_REGISTRY}/v2/${GAR_REPO}/manifests/${OPT_TAG}" 2>/dev/null \
            | grep -i "docker-content-digest" | awk '{print $2}' | tr -d '\r' || true)

        if [[ -n "$OPT_DIGEST" ]]; then
            echo "  Optimized image digest: $OPT_DIGEST"
        else
            echo "  Could not resolve optimized tag (might not exist or auth issue)"
        fi

        # Now try the vanilla tag (without -opt suffix)
        VANILLA_TAG="ae7937418a56fdb730bda165d004908b"
        echo ""
        echo "Resolving vanilla tag: ${VANILLA_TAG}"

        VANILLA_DIGEST=$(curl -s -I \
            -H "Authorization: Bearer $GAR_TOKEN" \
            -H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" \
            "https://${GAR_REGISTRY}/v2/${GAR_REPO}/manifests/${VANILLA_TAG}" 2>/dev/null \
            | grep -i "docker-content-digest" | awk '{print $2}' | tr -d '\r' || true)

        if [[ -n "$VANILLA_DIGEST" ]]; then
            echo "  Vanilla image digest: $VANILLA_DIGEST"
            echo ""
            echo "To test referrers API for this image:"
            echo "  ./quick-test.sh gar $VANILLA_DIGEST"
        else
            echo "  Could not resolve vanilla tag"
            echo ""
            echo "The provided reference 'ae7937418a56fdb730bda165d004908b' doesn't look like a full digest."
            echo "A valid SHA256 digest should be: sha256:<64 hex characters>"
            echo ""
            echo "Please provide the full image reference. You can get it with:"
            echo "  gcloud artifacts docker images describe ${GAR_REGISTRY}/${GAR_REPO}:TAG"
        fi
    else
        echo "Could not get GAR token. Please run: gcloud auth login"
    fi
else
    echo "gcloud not installed, skipping GAR"
fi

echo ""
echo "--- ECR Images ---"
echo ""

ECR_REGISTRY="992382529030.dkr.ecr.us-east-2.amazonaws.com"
ECR_REPO="union/dogfood"
ECR_TAG="flyte-4226d5b6bc1d573fe3264aeaa1792702"

if command -v aws &> /dev/null; then
    echo "Getting ECR token..."
    ECR_PASSWORD=$(aws ecr get-login-password --region us-east-2 2>/dev/null || true)

    if [[ -n "$ECR_PASSWORD" ]]; then
        ECR_TOKEN=$(echo -n "AWS:$ECR_PASSWORD" | base64)

        echo "Resolving ECR image: ${ECR_REPO}:${ECR_TAG}"

        ECR_DIGEST=$(curl -s -I \
            -H "Authorization: Basic $ECR_TOKEN" \
            -H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json" \
            "https://${ECR_REGISTRY}/v2/${ECR_REPO}/manifests/${ECR_TAG}" 2>/dev/null \
            | grep -i "docker-content-digest" | awk '{print $2}' | tr -d '\r' || true)

        if [[ -n "$ECR_DIGEST" ]]; then
            echo "  ECR image digest: $ECR_DIGEST"
            echo ""
            echo "To test referrers API for this image:"
            echo "  ./quick-test.sh ecr $ECR_DIGEST"
        else
            echo "  Could not resolve ECR image"
        fi
    else
        echo "Could not get ECR token. Please run: aws configure"
    fi
else
    echo "aws CLI not installed, skipping ECR"
fi

echo ""
echo "=== Summary ==="
echo ""
echo "Once you have the digests, run:"
echo "  cd tools/referrer-timing"
echo "  ./quick-test.sh gar <gar-digest>"
echo "  ./quick-test.sh ecr <ecr-digest>"
echo ""
echo "Compare the TTFB values to see if GAR is slower than ECR."
