#!/bin/bash

# Run this script from the root project directory e.g.:
# ./scripts/start_styx_dev_cluster.sh 1 10

# Enter dev-cluster build context
cd scripts/dev-cluster

n_workers=$1
threads_per_worker=$2
epoch_size=$3

echo "

WORKERS = $n_workers
THREADS_PER_WORKER = $threads_per_worker
EPOCH_SIZE = $epoch_size

"

docker system prune -f --volumes

# APPLY NEW DEPLOYMENT in 3 steps.
# Step: 1
docker-compose -f docker-compose-kafka-dev.yml up -d
sleep 5

# Step: 2
# Ignore orphan containers warning:
# Tried to suppress with -p or top-level name but it was not respected.
# docker inspect <container name> | grep -i project
# should show that all containers share the same project and context
docker-compose -f docker-compose-minio-dev.yml up -d
sleep 10

# Step: 3
export WORKER_THREADS=$threads_per_worker
docker-compose build --build-arg epoch_size="$epoch_size"
# Ignore orphan containers warning. See previous comment.
# Pass env variable to docker container through the shell.
docker-compose -f docker-compose.yml up --scale worker="$n_workers" -d
sleep 5