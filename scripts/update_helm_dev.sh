#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
DEV_VALUES="$ROOT_DIR/charts/styx-cluster/dev_values.yaml"
HASH_CM="${RELEASE_NAME}-dev-values-hash"

if ! helm status "$RELEASE_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
  echo "Release '$RELEASE_NAME' not found in namespace '$NAMESPACE'. Nothing to update."
  exit 0
fi

if [ ! -f "$DEV_VALUES" ]; then
  echo "dev_values.yaml not found at $DEV_VALUES" >&2
  exit 1
fi

current_hash=$(sha256sum "$DEV_VALUES" | awk '{print $1}')
stored_hash=$(kubectl get configmap "$HASH_CM" -n "$NAMESPACE" -o jsonpath='{.data.sha256}' 2>/dev/null || true)

echo "Rebuilding current dev branch and loading the images to minikube..."
"$ROOT_DIR/scripts/load_images_minikube.sh"

if [ "$current_hash" = "$stored_hash" ]; then
  echo "dev_values.yaml unchanged. Skipping Helm upgrade."
  exit 0
fi

echo "dev_values.yaml changed (or hash missing). Upgrading release '$RELEASE_NAME'..."
helm upgrade "$RELEASE_NAME" "$ROOT_DIR/charts/styx-cluster" \
  -n "$NAMESPACE" \
  -f "$DEV_VALUES"

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
