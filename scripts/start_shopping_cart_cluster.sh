#!/bin/bash

./scripts/start_styx_cluster.sh 4 100 1 "true" "true" "true"

docker compose -f docker-compose-shopping-cart-demo.yml up --build