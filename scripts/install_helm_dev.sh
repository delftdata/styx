#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}

echo "Building current dev branch and loading the images to minikube..."
"$ROOT_DIR/scripts/load_images_minikube.sh"

echo "Updating chart dependencies..."
helm dependency update "$ROOT_DIR/charts/styx-cluster"

echo "Installing/Upgrading Helm release '$RELEASE_NAME' in namespace '$NAMESPACE'..."
helm upgrade --install "$RELEASE_NAME" "$ROOT_DIR/charts/styx-cluster" \
  -n "$NAMESPACE" --create-namespace \
  -f "$ROOT_DIR/charts/styx-cluster/dev_values.yaml"

echo "Done."
