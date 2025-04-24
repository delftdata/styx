#!/bin/bash

docker compose logs worker | sort -t '|' -k1,1 -k2,2 > worker-logs.log
docker compose logs coordinator > coordinator-logs.log

# DELETE PREVIOUS DEPLOYMENT
docker compose down --volumes --remove-orphans
docker compose -f docker-compose-kafka.yml down --volumes --remove-orphans
docker compose -f docker-compose-minio.yml down --volumes --remove-orphans
