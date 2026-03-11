#!/usr/bin/env bash
set -euo pipefail

RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
TIMEOUT=${TIMEOUT:-600}

echo "Waiting for all pods to be Ready (release=$RELEASE_NAME, ns=$NAMESPACE, timeout=${TIMEOUT}s)..."
kubectl wait --for=condition=Ready pod --all \
  -n "${NAMESPACE}" --timeout="${TIMEOUT}s"

echo "Styx cluster is Ready."
