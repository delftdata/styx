#!/bin/bash

threads_per_worker=$1

TS=$(date +"%Y%m%d-%H%M%S")

export STYX_WORKER_THREADS="$threads_per_worker"
mkdir -p logs
docker compose logs worker | sort -t '|' -k1,1 -k2,2 > "logs/worker-logs-${TS}.log"
docker compose logs coordinator > "logs/coordinator-logs-${TS}.log"

# DELETE PREVIOUS DEPLOYMENT
docker compose down --volumes --remove-orphans
docker compose -f docker-compose-kafka.yml down --volumes --remove-orphans
docker compose -f docker-compose-minio.yml down --volumes --remove-orphans
