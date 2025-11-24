#!/bin/bash

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
styx_threads_per_worker=${11}

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
echo "=================================================="

bash scripts/start_styx_cluster.sh "$n_part" "$epoch_size" "$n_part" "$styx_threads_per_worker"

sleep 10

if [[ $workload_name == "ycsbt" ]]; then
    # YCSB-T
    # To check if the state is correct within Styx, expensive to run together with large scale experiments, use for debug
    # values true | false
    run_with_validation=false
    python demo/demo-ycsb/client.py "$client_threads" "$n_keys" "$n_part" "$zipf_const" "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds" "$run_with_validation"
elif [[ $workload_name == "dhr" ]]; then
    # Deathstar Hotel Reservation
    python demo/demo-deathstar-hotel-reservation/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds"
elif [[ $workload_name == "dmr" ]]; then
    # Deathstar Movie Review
    python demo/demo-deathstar-movie-review/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds"
elif [[ $workload_name == "tpcc" ]]; then
    # TPC-C
    bash scripts/generate_tpcc_dataset.sh "$n_keys"
    python demo/demo-tpc-c/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds" "$n_keys"
else
    echo "Benchmark not supported!"
fi


bash scripts/stop_styx_cluster.sh "$styx_threads_per_worker"