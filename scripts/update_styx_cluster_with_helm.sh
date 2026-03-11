#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
DEPLOY_MODE=${DEPLOY_MODE:-k8s-minikube}   # k8s-minikube | k8s-cluster
HASH_CM="${RELEASE_NAME}-values-hash"

if [[ "$DEPLOY_MODE" == "k8s-minikube" ]]; then
  VALUES_FILE="$ROOT_DIR/charts/styx-cluster/dev_values.yaml"
else
  VALUES_FILE="$ROOT_DIR/charts/styx-cluster/values.yaml"
fi

if ! helm status "$RELEASE_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
  echo "Release '$RELEASE_NAME' not found in namespace '$NAMESPACE'. Nothing to update."
  exit 0
fi

if [ ! -f "$VALUES_FILE" ]; then
  echo "Values file not found at $VALUES_FILE" >&2
  exit 1
fi

current_hash=$(sha256sum "$VALUES_FILE" | awk '{print $1}')
stored_hash=$(kubectl get configmap "$HASH_CM" -n "$NAMESPACE" -o jsonpath='{.data.sha256}' 2>/dev/null || true)

if [[ "$DEPLOY_MODE" == "k8s-minikube" ]]; then
  echo "Rebuilding current dev branch and loading the images to minikube..."
  "$ROOT_DIR/scripts/load_images_minikube.sh"
fi

if [ "$current_hash" = "$stored_hash" ]; then
  echo "$(basename "$VALUES_FILE") unchanged. Skipping Helm upgrade."
  exit 0
fi

echo "$(basename "$VALUES_FILE") changed (or hash missing). Upgrading release '$RELEASE_NAME'..."
helm upgrade "$RELEASE_NAME" "$ROOT_DIR/charts/styx-cluster" \
  -n "$NAMESPACE" \
  -f "$VALUES_FILE"

cat <<EOF | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${HASH_CM}
  labels:
    app.kubernetes.io/instance: ${RELEASE_NAME}
    app.kubernetes.io/managed-by: helm
data:
  sha256: "${current_hash}"
EOF

echo "Done."
