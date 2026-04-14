#!/bin/bash

set -euo pipefail

# Get the root directory of the project
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

styx_threads_per_worker=1
enable_compression=true
use_composite_keys=true
use_fallback_cache=true
regenerate_tpcc_data=false
workload_profile="constant"


# Read positional arguments
input_rate=$1
start_n_part=$2
end_n_part=$3
client_threads=$4
total_time=$5
saving_dir=$6
warmup_seconds=$7
epoch_size=$8
workload_name=$9
n_keys=${10}

[ -n "${11:-}" ] && workload_profile=${11}
[ -n "${12:-}" ] && regenerate_tpcc_data=${12}
# Optional overrides (minimal, but allows parity with start_experiment.sh style)
[ -n "${13:-}" ] && styx_threads_per_worker=${13}
[ -n "${14:-}" ] && enable_compression=${14}
[ -n "${15:-}" ] && use_composite_keys=${15}
[ -n "${16:-}" ] && use_fallback_cache=${16}

# Determine the maximum number of partitions
if (( start_n_part > end_n_part )); then
    max_part=$start_n_part
else
    max_part=$end_n_part
fi

echo "============= Running Migration Experiment ================="
echo "workload_name: $workload_name"
echo "input_rate: $input_rate"
echo "start_n_part: $start_n_part"
echo "end_n_part: $end_n_part"
echo "max_part: $max_part"
echo "client_threads: $client_threads"
echo "total_time: $total_time"
echo "saving_dir: $saving_dir"
echo "warmup_seconds: $warmup_seconds"
echo "epoch_size: $epoch_size"
echo "n_keys: $n_keys"
echo "styx_threads_per_worker: $styx_threads_per_worker"
echo "enable_compression: $enable_compression"
echo "use_composite_keys: $use_composite_keys"
echo "use_fallback_cache: $use_fallback_cache"
echo "regenerate_tpcc_data: $regenerate_tpcc_data"
echo "workload_profile: $workload_profile"
echo "============================================================"

case "$workload_profile" in
    constant|increasing|decreasing|random|cosine|step) ;;
    *)
        echo "ERROR: Unknown workload profile: $workload_profile"
        exit 1
        ;;
esac
load_config_path="demo/load_profiles/$workload_profile.yaml"

bash "$ROOT_DIR/scripts/start_styx_cluster.sh" \
  "$start_n_part" "$epoch_size" "$styx_threads_per_worker" \
  "$enable_compression" "$use_composite_keys" "$use_fallback_cache"

sleep 10

# Run workload
if [[ "$workload_name" == "ycsb" ]]; then

    python "$ROOT_DIR/demo/demo-migration-ycsb/client.py" \
        "$client_threads" "$start_n_part" "$end_n_part" \
        "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds" "$n_keys" "$load_config_path"

elif [[ "$workload_name" == "tpcc" ]]; then

    DATA_DIR="$ROOT_DIR/demo/demo-migration-tpc-c/data_${n_keys}"
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
        "$input_rate" "$total_time" "$warmup_seconds" "$n_keys" \
        "$enable_compression" "$use_composite_keys" "$use_fallback_cache" "$load_config_path"

else
    echo "Benchmark not supported: $workload_name"
    exit 1
fi

bash "$ROOT_DIR/scripts/stop_styx_cluster.sh" "$styx_threads_per_worker"
