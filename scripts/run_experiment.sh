#!/bin/bash

styx_threads_per_worker=1
enable_compression=true
use_composite_keys=true
use_fallback_cache=true
regenerate_tpcc_data=false

workload_name=$1
input_rate=$2
n_keys=$3
n_part=$4
zipf_const=$5
client_threads=$6
total_time=$7
saving_dir=$8
warmup_seconds=$9
epoch_size=${10}

[ -n "${11}" ] && styx_threads_per_worker=${11}
[ -n "${12}" ] && enable_compression=${12}
[ -n "${13}" ] && use_composite_keys=${13}
[ -n "${14}" ] && use_fallback_cache=${14}
[ -n "${15}" ] && regenerate_tpcc_data=${15}
kill_at="-1" # This means that we are not going to kill any containers using this script

echo "============= Running Experiment ================="
echo "workload_name: $workload_name"
echo "input_rate: $input_rate"
echo "n_keys: $n_keys"
echo "n_part: $n_part"
echo "zipf_const: $zipf_const"
echo "client_threads: $client_threads"
echo "total_time: $total_time"
echo "saving_dir: $saving_dir"
echo "warmup_seconds: $warmup_seconds"
echo "epoch_size: $epoch_size"
echo "styx_threads_per_worker: $styx_threads_per_worker"
echo "enable_compression: $enable_compression"
echo "use_composite_keys: $use_composite_keys"
echo "use_fallback_cache: $use_fallback_cache"
echo "regenerate_tpcc_data: $regenerate_tpcc_data"
echo "=================================================="

bash scripts/start_styx_cluster.sh "$n_part" "$epoch_size" "$n_part" "$styx_threads_per_worker" "$enable_compression" "$use_composite_keys" "$use_fallback_cache"

sleep 10

if [[ $workload_name == "ycsbt" ]]; then
    # YCSB-T
    # To check if the state is correct within Styx, expensive to run together with large scale experiments, use for debug
    # values true | false
    run_with_validation=false
    python demo/demo-ycsb/client.py "$client_threads" "$n_keys" "$n_part" "$zipf_const" "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds" "$run_with_validation" "$kill_at"
elif [[ $workload_name == "dhr" ]]; then
    # Deathstar Hotel Reservation
    python demo/demo-deathstar-hotel-reservation/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds" "$kill_at"
elif [[ $workload_name == "dmr" ]]; then
    # Deathstar Movie Review
    python demo/demo-deathstar-movie-review/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds" "$kill_at"
elif [[ $workload_name == "tpcc" ]]; then
    # TPC-C
    DATA_DIR="demo/demo-tpc-c/data_${n_keys}"
    GENERATOR_DIR="demo/demo-tpc-c/tpcc-generator"
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

    python demo/demo-tpc-c/pure_kafka_demo.py \
        "$saving_dir" "$client_threads" "$n_part" \
        "$input_rate" "$total_time" "$warmup_seconds" \
        "$n_keys" "$enable_compression" "$use_composite_keys" "$use_fallback_cache" "$kill_at"
else
    echo "Benchmark not supported!"
fi


bash scripts/stop_styx_cluster.sh "$styx_threads_per_worker"