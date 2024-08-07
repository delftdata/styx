#!/bin/bash

docker compose logs worker > worker-logs.log
docker compose logs coordinator > worker-logs.log

# DELETE PREVIOUS DEPLOYMENT
docker compose down --volumes --remove-orphans
docker compose -f docker-compose-kafka.yml down --volumes --remove-orphans
docker compose -f docker-compose-minio.yml down --volumes --remove-orphans
