#!/bin/bash

input=$1
saving_dir=$2
styx_threads_per_worker=$3
partitions=$4
n_keys=$5
experiment_time=$6
warmup_time=$7
scenarios=$8

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
  use_fallback_cache="${ss[11]}"

  ./scripts/run_experiment.sh "$workload_name" "$input_rate" "$n_keys" "$n_part" "$zipf_const" "$client_threads" \
                              "$total_time" "$saving_dir" "$warmup_seconds" "$epoch_size" "$styx_threads_per_worker" \
                              "$enable_compression" "$use_composite_keys" "$use_fallback_cache"

done < "$input"