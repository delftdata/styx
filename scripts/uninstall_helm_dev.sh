#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
DELETE_NAMESPACE=${DELETE_NAMESPACE:-true}
COORDINATOR_IMAGE=${COORDINATOR_IMAGE:-styx-coordinator}
WORKER_IMAGE=${WORKER_IMAGE:-styx-worker}
TAG=${TAG:-dev}

echo "Uninstalling Helm release '$RELEASE_NAME' from namespace '$NAMESPACE'..."
helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" || true

if [[ "$DELETE_NAMESPACE" == "true" ]]; then
  echo "Deleting namespace '$NAMESPACE'..."
  kubectl delete namespace "$NAMESPACE" || true
fi

echo "Removing images from minikube..."
minikube image rm "${COORDINATOR_IMAGE}:${TAG}"
minikube image rm "${WORKER_IMAGE}:${TAG}"

echo "Done."
