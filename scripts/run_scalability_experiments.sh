#!/bin/bash

input=$1
saving_dir=$2
styx_threads_per_worker=$3

python scripts/create_scalability_config.py

while IFS= read -r line
do
  printf 'Run experiment: %s\n' "$line"
  IFS=',' read -ra ss <<< "$line"
  input_rate="${ss[0]}"
  n_part="${ss[1]}"
  pm="${ss[2]}"
  client_threads="${ss[3]}"
  total_time="${ss[4]}"
  warmup_seconds="${ss[5]}"
  epoch_size="${ss[6]}"

  bash scripts/start_styx_cluster.sh "$n_part" "$epoch_size" "$n_part" "$styx_threads_per_worker"

  sleep 10

  python demo/demo-scalability/client.py "$client_threads" "$pm" "$n_part" "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds"

  bash scripts/stop_styx_cluster.sh "$styx_threads_per_worker"

done < "$input"