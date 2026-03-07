#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
DELETE_NAMESPACE=${DELETE_NAMESPACE:-true}
COORDINATOR_IMAGE=${COORDINATOR_IMAGE:-styx-coordinator}
WORKER_IMAGE=${WORKER_IMAGE:-styx-worker}
TAG=${TAG:-dev}
DEPLOY_MODE=${DEPLOY_MODE:-k8s-minikube}   # k8s-minikube | k8s-cluster

echo "Uninstalling Helm release '$RELEASE_NAME' from namespace '$NAMESPACE'..."
helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" || true

if [[ "$DELETE_NAMESPACE" == "true" ]]; then
  echo "Deleting namespace '$NAMESPACE'..."
  kubectl delete namespace "$NAMESPACE" || true
fi

if [[ "$DEPLOY_MODE" == "k8s-minikube" ]]; then
  echo "Removing images from minikube..."
  minikube image rm "${COORDINATOR_IMAGE}:${TAG}"
  minikube image rm "${WORKER_IMAGE}:${TAG}"
fi

echo "Done."
