#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

COORDINATOR_IMAGE=${COORDINATOR_IMAGE:-styx-coordinator}
WORKER_IMAGE=${WORKER_IMAGE:-styx-worker}
TAG=${TAG:-dev}

if ! command -v minikube >/dev/null 2>&1; then
  echo "minikube is not installed or not in PATH." >&2
  exit 1
fi

STATUS=$(minikube status --format '{{.Host}}' 2>/dev/null || true)
if [ "$STATUS" != "Running" ]; then
  echo "minikube is not running. Start it with: minikube start" >&2
  exit 1
fi

"$ROOT_DIR/scripts/build_local_images.sh"

echo "Loading images into minikube..."
minikube  image load "${COORDINATOR_IMAGE}:${TAG}"
minikube  image load "${WORKER_IMAGE}:${TAG}"

echo "Done. Images available in minikube:"
minikube image list
