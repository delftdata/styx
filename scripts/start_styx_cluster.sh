#!/bin/bash

scale_factor=$1
epoch_size=$2
max_operator_parallelism=$3
threads_per_worker=$4
minimum_amount_of_workers=1

threaded_scale_factor=$(( "$scale_factor" / "$threads_per_worker" ))
threaded_scale_factor=$(( "$minimum_amount_of_workers" > "$threaded_scale_factor" ? "$minimum_amount_of_workers" : "$threaded_scale_factor" ))

echo "============== Starting Styx Cluster ================"
echo "scale_factor: $scale_factor"
echo "epoch_size: $epoch_size"
echo "max_operator_parallelism: $max_operator_parallelism"
echo "threads_per_worker: $threads_per_worker"
echo "minimum_amount_of_workers: $minimum_amount_of_workers"
echo "threaded_scale_factor: $threaded_scale_factor"
echo "====================================================="

docker system prune -f --volumes >/dev/null
# START NEW DEPLOYMENT
docker compose -f docker-compose-kafka.yml up -d >/dev/null
sleep 5
docker compose -f docker-compose-minio.yml up -d >/dev/null
sleep 10
export STYX_WORKER_THREADS="$threads_per_worker"
docker compose build \
    --build-arg epoch_size="$epoch_size" \
    --build-arg max_operator_parallelism="$max_operator_parallelism" \
    --build-arg worker_threads="$threads_per_worker" \
    > /dev/null
docker compose up --scale worker="$threaded_scale_factor" -d >/dev/null
sleep 5