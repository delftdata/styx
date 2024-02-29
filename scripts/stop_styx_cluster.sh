#!/bin/bash

# DELETE PREVIOUS DEPLOYMENT
docker-compose down --volumes --remove-orphans
docker-compose -f docker-compose-kafka.yml down --volumes --remove-orphans
docker-compose -f docker-compose-minio.yml down --volumes --remove-orphans
