#!/bin/bash

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

bash scripts/start_styx_cluster.sh "$n_workers" "$epoch_size"

sleep 10

if [[ $workload_name == "ycsb" ]]; then
    python demo/demo-migration-ycsb/client.py "$client_threads" "$start_n_part" "$end_n_part" "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds" "$n_keys"
elif [[ $workload_name == "tpcc" ]]; then
    # TPC-C
    make clean -C demo/demo-migration-tpc-c/tpcc-generator/
    make -C demo/demo-migration-tpc-c/tpcc-generator/
    rm -rf demo/demo-migration-tpc-c/data
    mkdir demo/demo-migration-tpc-c/data
    ./demo/demo-migration-tpc-c/tpcc-generator/tpcc-generator "$n_keys" demo/demo-migration-tpc-c/data
    python demo/demo-migration-tpc-c/pure_kafka_demo.py "$saving_dir" "$client_threads" "$start_n_part" "$end_n_part" "$input_rate" "$total_time" "$warmup_seconds" "$n_keys"
else
    echo "Benchmark not supported!"
fi

bash scripts/stop_styx_cluster.sh
