#!/bin/bash

styx_threads_per_worker=1
enable_compression=true
use_composite_keys=true
use_fallback_cache=true
regenerate_tpcc_data=false
workload_profile="constant"

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
[ -n "${11}" ] && workload_profile=${11}
[ -n "${12}" ] && styx_threads_per_worker=${12}
[ -n "${13}" ] && enable_compression=${13}
[ -n "${14}" ] && use_composite_keys=${14}
[ -n "${15}" ] && use_fallback_cache=${15}
[ -n "${16}" ] && regenerate_tpcc_data=${16}
kill_at="-1" # This means that we are not going to kill any containers using this script

# Deployment mode configuration (read from environment).
# Modes: docker-compose | k8s-minikube | k8s-cluster
DEPLOY_MODE=${DEPLOY_MODE:-docker-compose}
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}

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
echo "deploy_mode: $DEPLOY_MODE"
echo "workload_profile: $workload_profile"
echo "==================================================="

case "$workload_profile" in
    constant|increasing|decreasing|random|cosine|step) ;;
    *)
        echo "ERROR: Unknown workload profile: $workload_profile"
        exit 1
        ;;
esac
load_config_path="demo/load_profiles/$workload_profile.yaml"

# Use kubefwd to forward all services in the namespace.
# kubefwd adds /etc/hosts entries for service names and pod FQDNs, which is
# required for Kafka: kubectl port-forward alone causes advertised-listener
# redirects to internal cluster DNS names that the host cannot resolve.
_k8s_setup() {
    bash scripts/wait_for_styx_k8s.sh || { echo "ERROR: cluster readiness check failed, aborting." >&2; exit 1; }

    echo "Forwarding all services in namespace '$NAMESPACE' via kubefwd..."
    sudo KUBECONFIG="$HOME/.kube/config" kubefwd svc -n "$NAMESPACE" &
    KUBEFWD_PID=$!
    trap 'sudo kill "$KUBEFWD_PID" 2>/dev/null || true' EXIT
    sleep 5  # let kubefwd establish forwards and add /etc/hosts entries
    if ! kill -0 "$KUBEFWD_PID" 2>/dev/null; then
        echo "ERROR: kubefwd failed to start (check kubeconfig and cluster connectivity)." >&2
        exit 1
    fi

    export STYX_HOST="${RELEASE_NAME}-styx-coordinator"
    export STYX_PORT=8888
    export KAFKA_URL="${RELEASE_NAME}-kafka-headless:9092"
    export S3_ENDPOINT="http://${RELEASE_NAME}-rustfs:9000"
}

if [[ "$DEPLOY_MODE" == "k8s-minikube" || "$DEPLOY_MODE" == "k8s-cluster" ]]; then
    bash scripts/install_styx_cluster_with_helm.sh
    _k8s_setup

else
    # docker-compose mode
    bash scripts/start_styx_cluster.sh "$n_part" "$epoch_size" "$styx_threads_per_worker" "$enable_compression" "$use_composite_keys" "$use_fallback_cache"
    sleep 10
fi

if [[ $workload_name == "ycsbt" ]]; then
    # YCSB-T
    # To check if the state is correct within Styx, expensive to run together with large scale experiments, use for debug
    # values true | false
    run_with_validation=false
    python demo/demo-ycsb/client.py "$client_threads" "$n_keys" "$n_part" "$zipf_const" "$input_rate" "$total_time" "$saving_dir" "$warmup_seconds" "$run_with_validation" "$load_config_path" "$kill_at"
elif [[ $workload_name == "dhr" ]]; then
    # Deathstar Hotel Reservation
    python demo/demo-deathstar-hotel-reservation/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds" "$load_config_path" "$kill_at"
elif [[ $workload_name == "dmr" ]]; then
    # Deathstar Movie Review
    python demo/demo-deathstar-movie-review/pure_kafka_demo.py "$saving_dir" "$client_threads" "$n_part" "$input_rate" "$total_time" "$warmup_seconds" "$load_config_path" "$kill_at"
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
        "$n_keys" "$enable_compression" "$use_composite_keys" "$use_fallback_cache" "$load_config_path" "$kill_at"
else
    echo "Benchmark not supported!"
fi


if [[ "$DEPLOY_MODE" == "k8s-minikube" || "$DEPLOY_MODE" == "k8s-cluster" ]]; then
    if [[ -n "${KUBEFWD_PID:-}" ]]; then
        echo "Stopping kubefwd (pid $KUBEFWD_PID)..."
        sudo kill "$KUBEFWD_PID" 2>/dev/null || true
        unset KUBEFWD_PID
    fi
    bash scripts/uninstall_styx_cluster_with_helm.sh
else
    #bash scripts/stop_styx_cluster.sh "$styx_threads_per_worker"
    docker compose stop coordinator worker 
fi
