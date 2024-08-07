#!/bin/bash

# Run this script from the root project directory e.g.:
# ./scripts/stop_styx_dev_cluster.sh

# Enter dev-cluster build context
cd scripts/dev-cluster

# Silence empty env var warning. The actual value is not used.
export WORKER_THREADS=0

# DELETE PREVIOUS DEPLOYMENT
docker-compose down --volumes --remove-orphans
docker-compose -f docker-compose-kafka-dev.yml down --volumes --remove-orphans
docker-compose -f docker-compose-minio-dev.yml down --volumes --remove-orphans
