#!/bin/bash

input=$1
saving_dir=$2
styx_threads_per_worker=$3

python scripts/create_scalability_config.py

while IFS= read -r line
do
  IFS=',' read -ra ss <<< "$line"
  input_rate="${ss[0]}"
  n_part="${ss[1]}"
  pm="${ss[2]}"
  client_threads="${ss[3]}"
  total_time="${ss[4]}"
  warmup_seconds="${ss[5]}"
  epoch_size="${ss[6]}"

  echo "============= Running Scalability Experiment ============="
  echo "input_rate:               $input_rate"
  echo "n_part (workers):         $n_part"
  echo "multipartition (pm):      $pm"
  echo "client_threads:           $client_threads"
  echo "total_time:               $total_time"
  echo "warmup_seconds:           $warmup_seconds"
  echo "epoch_size:               $epoch_size"
  echo "threads_per_worker:       $styx_threads_per_worker"
  echo "=========================================================="

  bash scripts/start_styx_cluster.sh "$n_part" "$epoch_size" "$n_part" "$styx_threads_per_worker"

  sleep 10

  python demo/demo-scalability/client.py "$client_threads" "$pm" "$n_part" "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds"

  bash scripts/stop_styx_cluster.sh "$styx_threads_per_worker"

done < "$input"