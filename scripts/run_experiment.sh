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

bash scripts/start_styx_cluster.sh $n_part $epoch_size

sleep 10

if [[ $workload_name == "ycsbt" ]]; then
    # YCSB-T
    python demo/demo-ycsb/client.py $client_threads $n_keys $n_part $zipf_const $input_rate $total_time $saving_dir
    python demo/demo-ycsb/kafka_output_consumer.py $saving_dir
    python demo/demo-ycsb/calculate_metrics.py $saving_dir $warmup_seconds $n_keys $n_part $input_rate $zipf_const $client_threads
elif [[ $workload_name == "dhr" ]]; then
    # Deathstar Hotel Reservation
    python demo/demo-deathstar-hotel-reservation/pure_kafka_demo.py $saving_dir $client_threads $n_part $input_rate $total_time
    python demo/demo-deathstar-hotel-reservation/kafka_output_consumer.py $saving_dir
    python demo/demo-deathstar-hotel-reservation/calculate_metrics.py $saving_dir $input_rate $warmup_seconds $client_threads
elif [[ $workload_name == "dmr" ]]; then
    # Deathstar Movie Review
    python demo/demo-deathstar-movie-review/pure_kafka_demo.py $saving_dir $client_threads $n_part $input_rate $total_time
    python demo/demo-deathstar-movie-review/kafka_output_consumer.py $saving_dir
    python demo/demo-deathstar-movie-review/calculate_metrics.py $saving_dir $input_rate $warmup_seconds $client_threads
elif [[ $workload_name == "tpcc" ]]; then
    # TPC-C
    bash scripts/generate_tpcc_dataset.sh $n_keys
    python demo/demo-tpc-c/pure_kafka_demo.py $saving_dir $client_threads $n_part $input_rate $total_time $n_keys
    python demo/demo-tpc-c/kafka_output_consumer.py $saving_dir
    python demo/demo-tpc-c/calculate_metrics.py $saving_dir $input_rate $warmup_seconds $client_threads $n_keys
else
    echo "Benchmark not supported!"
fi


bash scripts/stop_styx_cluster.sh