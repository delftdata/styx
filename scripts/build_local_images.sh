#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

COORDINATOR_IMAGE="styx-coordinator"
WORKER_IMAGE="styx-worker"
TAG="dev"

cd "$ROOT_DIR"

echo "Building coordinator image: ${COORDINATOR_IMAGE}:${TAG}"
docker build -f coordinator/coordinator.dockerfile -t "${COORDINATOR_IMAGE}:${TAG}" .

echo "Building worker image: ${WORKER_IMAGE}:${TAG}"
docker build -f worker/worker.dockerfile -t "${WORKER_IMAGE}:${TAG}" .

echo "Done."
