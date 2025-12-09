#!/bin/bash
set -e

# Configuration
IMAGE_NAME="otelcol-ebpf-profiler-custom"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Verify we're using minikube with driver=none
if ! kubectl config current-context 2>/dev/null | grep -q "minikube"; then
    echo "❌ Error: Not using minikube context"
    echo "This script only supports minikube with driver=none"
    exit 1
fi

MINIKUBE_DRIVER=$(minikube profile list -o json 2>/dev/null | grep -o '"Driver":"[^"]*"' | cut -d'"' -f4 || echo "")
if [ "$MINIKUBE_DRIVER" != "none" ]; then
    echo "❌ Error: Minikube is using driver: $MINIKUBE_DRIVER"
    echo "This script only supports minikube with driver=none"
    echo ""
    echo "To use driver=none:"
    echo "  sudo minikube delete"
    echo "  sudo minikube start --driver=none"
    exit 1
fi

echo "Building custom OpenTelemetry Collector..."
echo "Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "Driver: none (using host Docker daemon)"
echo ""

# Build with host Docker daemon
docker build -f Dockerfile.custom -t ${IMAGE_NAME}:${IMAGE_TAG} .

echo ""
echo "✅ Build complete!"
echo ""
echo "Deploy with:"
echo "  kubectl create namespace otel  # if not exists"
echo "  kubectl apply -f k8s/opentelemetry-collector/"
