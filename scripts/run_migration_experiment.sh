#!/bin/bash

set -euo pipefail

# Get the root directory of the project
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Read positional arguments
input_rate=$1
start_n_part=$2
end_n_part=$3
n_workers=$4
client_threads=$5
total_time=$6
saving_dir=$7
warmup_seconds=$8
epoch_size=$9
workload_name=${10}
n_keys=${11}
regenerate_tpcc_data=${12:-false}

# Determine the maximum number of partitions
if (( start_n_part > end_n_part )); then
    max_part=$start_n_part
else
    max_part=$end_n_part
fi

# Start the Styx cluster
bash "$ROOT_DIR/scripts/start_styx_cluster.sh" "$n_workers" "$epoch_size" "$max_part"
sleep 10

# Run workload
if [[ "$workload_name" == "ycsb" ]]; then

    python "$ROOT_DIR/demo/demo-migration-ycsb/client.py" \
        "$client_threads" "$start_n_part" "$end_n_part" \
        "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds" "$n_keys"

elif [[ "$workload_name" == "tpcc" ]]; then

    DATA_DIR="$ROOT_DIR/demo/demo-migration-tpc-c/data"
    GENERATOR_DIR="$ROOT_DIR/demo/demo-migration-tpc-c/tpcc-generator"
    GENERATOR_BIN="$GENERATOR_DIR/tpcc-generator"

    # Decide if we should regenerate data
    if [[ "$regenerate_tpcc_data" == true ]]; then
        echo "regenerate_tpcc_data is true — forcing data regeneration."
        regenerate=true
    elif [[ ! -d "$DATA_DIR" || $(find "$DATA_DIR" -type f | wc -l) -ne 9 ]]; then
        echo "Data directory missing or does not contain the exact TPC-C dataset."
        regenerate=true
    else
        echo "Skipping data generation: $DATA_DIR already contains the TPC-C dataset."
        regenerate=false
    fi

    if [[ "$regenerate" == true ]]; then
        make clean -C "$GENERATOR_DIR"
        make -C "$GENERATOR_DIR"
        rm -rf "$DATA_DIR"
        mkdir -p "$DATA_DIR"
        "$GENERATOR_BIN" "$n_keys" "$DATA_DIR"
    fi

    python "$ROOT_DIR/demo/demo-migration-tpc-c/pure_kafka_demo.py" \
        "$saving_dir" "$client_threads" "$start_n_part" "$end_n_part" \
        "$input_rate" "$total_time" "$warmup_seconds" "$n_keys"

else
    echo "Benchmark not supported: $workload_name"
    exit 1
fi

# Stop the Styx cluster
bash "$ROOT_DIR/scripts/stop_styx_cluster.sh"
