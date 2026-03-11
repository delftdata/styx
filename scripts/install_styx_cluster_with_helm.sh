#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
DEPLOY_MODE=${DEPLOY_MODE:-k8s-minikube}   # k8s-minikube | k8s-cluster

if [[ "$DEPLOY_MODE" == "k8s-minikube" ]]; then
  VALUES_FILE="$ROOT_DIR/charts/styx-cluster/dev_values.yaml"
  echo "Building current dev branch and loading the images to minikube..."
  "$ROOT_DIR/scripts/load_images_minikube.sh"
else
  VALUES_FILE="$ROOT_DIR/charts/styx-cluster/values.yaml"
fi

echo "Updating chart dependencies..."
helm dependency update "$ROOT_DIR/charts/styx-cluster"

echo "Installing/Upgrading Helm release '$RELEASE_NAME' in namespace '$NAMESPACE' using $(basename "$VALUES_FILE")..."
helm upgrade --install "$RELEASE_NAME" "$ROOT_DIR/charts/styx-cluster" \
  -n "$NAMESPACE" --create-namespace \
  -f "$VALUES_FILE"

echo "Done."
