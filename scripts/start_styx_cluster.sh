#!/bin/bash

scale_factor=$1
epoch_size=$2
threads_per_worker=1
minimum_amount_of_workers=1

threaded_scale_factor=$(( scale_factor / threads_per_worker ))
threaded_scale_factor=$(( "$minimum_amount_of_workers" > "$threaded_scale_factor" ? "$minimum_amount_of_workers" : "$threaded_scale_factor" ))

docker system prune -f --volumes
# START NEW DEPLOYMENT
docker compose -f docker-compose-kafka.yml up -d
sleep 5
docker compose -f docker-compose-minio.yml up -d
sleep 10
docker compose build --build-arg epoch_size="$epoch_size"
docker compose up --scale worker="$threaded_scale_factor" -d
sleep 5