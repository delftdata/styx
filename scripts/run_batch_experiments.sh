#!/bin/bash

input=$1
saving_dir=$2
styx_threads_per_worker=$3
partitions=$4
n_keys=$5
experiment_time=$6
warmup_time=$7
scenarios=$8

# Deployment mode is read from environment variables (not positional args).
#
# docker-compose mode (default):
#   No additional env vars needed. The script starts/stops the cluster per experiment.
#
# k8s-minikube mode:
#   export DEPLOY_MODE=k8s-minikube
#   export RELEASE_NAME=styx-cluster   # optional, default: styx-cluster
#   export NAMESPACE=styx              # optional, default: styx
#   kubefwd is used to forward all services in the namespace (requires sudo).
#
# k8s-cluster mode (generic k8s, no minikube):
#   export DEPLOY_MODE=k8s-cluster
#   export RELEASE_NAME=styx-cluster   # optional, default: styx-cluster
#   export NAMESPACE=styx              # optional, default: styx
#   kubefwd is used to forward all services in the namespace (requires sudo).
#
# For k8s modes, the cluster is installed and uninstalled automatically per experiment.

DEPLOY_MODE=${DEPLOY_MODE:-docker-compose}
echo "DEPLOY_MODE: $DEPLOY_MODE"
if [[ "$DEPLOY_MODE" == "k8s-minikube" || "$DEPLOY_MODE" == "k8s-cluster" ]]; then
    echo "RELEASE_NAME: ${RELEASE_NAME:-styx-cluster}"
    echo "NAMESPACE:    ${NAMESPACE:-styx}"
    echo "KAFKA_URL:    ${KAFKA_URL:-<not set>}"
fi

python scripts/create_config.py \
    --partitions "$partitions" \
    --n_keys "$n_keys" \
    --experiment_time "$experiment_time" \
    --warmup_time "$warmup_time" \
    --scenarios "$scenarios"

while IFS= read -r line
do
  printf 'Run experiment: %s\n' "$line"
  IFS=',' read -ra ss <<< "$line"
  workload_name="${ss[0]}"
  input_rate="${ss[1]}"
  n_keys="${ss[2]}"
  n_part="${ss[3]}"
  zipf_const="${ss[4]}"
  client_threads="${ss[5]}"
  total_time="${ss[6]}"
  warmup_seconds="${ss[7]}"
  epoch_size="${ss[8]}"
  enable_compression="${ss[9]}"
  use_composite_keys="${ss[10]}"

  ./scripts/run_experiment.sh "$workload_name" "$input_rate" "$n_keys" "$n_part" "$zipf_const" "$client_threads" \
                              "$total_time" "$saving_dir" "$warmup_seconds" "$epoch_size" "$styx_threads_per_worker" \
                              "$enable_compression" "$use_composite_keys"

done < "$input"
