#!/bin/bash

input=$1
saving_dir=$2

python scripts/create_config.py

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

  ./scripts/run_experiment.sh $workload_name $input_rate $n_keys $n_part $zipf_const $client_threads \
                              $total_time $saving_dir $warmup_seconds $epoch_size

done < "$input"