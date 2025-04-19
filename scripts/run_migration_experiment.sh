#!/bin/bash

input_rate=$1
n_part=$2
client_threads=$3
total_time=$4
saving_dir=$5
warmup_seconds=$6
epoch_size=$7

bash scripts/start_styx_cluster.sh "$n_part" "$epoch_size"

sleep 10

python demo/demo-migration-ycsb/client.py "$client_threads" "$n_part" "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds"

bash scripts/stop_styx_cluster.sh
