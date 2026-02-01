import multiprocessing
from multiprocessing import Pool, cpu_count
import os
import pickle
import random
import string
import sys
import time
from timeit import default_timer as timer

import calculate_metrics
import kafka_output_consumer
from minio import Minio
import pandas as pd
from styx.client.sync_client import SyncStyxClient
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.serialization import (
    cloudpickle_deserialization,
    cloudpickle_serialization,
)
from styx.common.stateflow_graph import StateflowGraph
from tqdm import tqdm
from ycsb import ycsb_operator


def ycsb_field(size=100, seed=0):
    "Translated from the original Java code"
    if seed is not None:
        random.seed(seed)
    charset = string.ascii_letters + string.digits  # 62 characters
    return "".join(random.choices(charset, k=size))


N_ENTITIES = int(sys.argv[8]) # 10 million entities
SECOND_TO_TAKE_MIGRATION = 60

threads = int(sys.argv[1])
START_N_PARTITIONS = int(sys.argv[2])
END_N_PARTITIONS = int(sys.argv[3])
messages_per_second = int(sys.argv[4])
seconds = int(sys.argv[5])
SAVE_DIR: str = sys.argv[6]
warmup_seconds: int = int(sys.argv[7])

BATCH_SIZE = 100_000

sleeps_per_second = 100
sleep_time = 0.0085
STYX_HOST: str = "localhost"
STYX_PORT: int = 8886
KAFKA_URL = "localhost:9092"
####################################################################################################################
g = StateflowGraph("ycsb-benchmark", operator_state_backend=LocalStateBackend.DICT)
ycsb_operator.set_n_partitions(START_N_PARTITIONS)
g.add_operators(ycsb_operator)

def submit_graph(styx: SyncStyxClient):
    print(f"Partitions: {list(g.nodes.values())[0].n_partitions}")
    styx.submit_dataflow(g)
    print("Graph submitted")

def generate_partition_batch_wrapper(args):
    batch_start, batch_end, operator_state = args
    current_active_graph, operator_name = operator_state
    current_active_graph = cloudpickle_deserialization(current_active_graph)
    local_partitions = {p: {} for p in range(START_N_PARTITIONS)}
    for i in range(batch_start, batch_end):
        partition = current_active_graph.get_operator_by_name(operator_name).which_partition(i)
        local_partitions[partition][i] = tuple(ycsb_field() for _ in range(10))
    return local_partitions

def merge_partitions(global_partitions, local_partitions):
    for part_id, entries in local_partitions.items():
        global_partitions[part_id].update(entries)

def ycsb_init(styx: SyncStyxClient, operator: Operator, num_workers: int = None):
    styx.set_graph(g)
    styx.init_metadata(g)
    partitions = {p: {} for p in range(START_N_PARTITIONS)}

    ycsb_dataset_path = f"ycsb_dataset_{START_N_PARTITIONS}p.pkl"

    if os.path.exists(ycsb_dataset_path):
        print("Loading YCSB dataset...")
        with open(ycsb_dataset_path, "rb") as f:
            partitions = pickle.load(f)
    else:
        print("Generating YCSB dataset...")
        batch_ranges = [
            (i, min(i + BATCH_SIZE, N_ENTITIES))
            for i in range(0, N_ENTITIES, BATCH_SIZE)
        ]
        operator_state = (cloudpickle_serialization(styx._current_active_graph), operator.name)

        tasks = [(start, end, operator_state) for start, end in batch_ranges]

        with Pool(processes=num_workers or cpu_count()) as pool:
            results = []
            for result in tqdm(pool.imap_unordered(generate_partition_batch_wrapper, tasks),
                               total=len(tasks)):
                results.append(result)

        for local_partitions in results:
            merge_partitions(partitions, local_partitions)
        with open(ycsb_dataset_path, "wb") as f:
            pickle.dump(partitions, f)
    print("Data ready")
    for partition, partition_data in partitions.items():
        styx.init_data(operator, partition, partition_data)
    print("Data loaded")
    time.sleep(5)
    submit_graph(styx)


def transactional_ycsb_generator(operator: Operator):
    while True:
        key = random.randint(0, N_ENTITIES - 1)
        op = "read" if random.random() < 0.85 else "update"
        yield operator, key, op


def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f"Generator: {proc_num} starting")
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    ycsb_generator = transactional_ycsb_generator(ycsb_operator)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    start = timer()
    for cur_sec in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name = next(ycsb_generator)
            future = styx.send_event(operator=operator,
                                     key=key,
                                     function=func_name)
            timestamp_futures[future.request_id] = {"op": f"{func_name} {key}"}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f"Latency per second: {sec_end2 - sec_start}")
        if cur_sec == SECOND_TO_TAKE_MIGRATION and proc_num == 0:
            new_g = StateflowGraph("ycsb-benchmark", operator_state_backend=LocalStateBackend.DICT)
            ycsb_operator.set_n_partitions(END_N_PARTITIONS)
            new_g.add_operators(ycsb_operator)
            styx.submit_dataflow(new_g)
            print("Migration request submitted")
    end = timer()
    print(f"Average latency per second: {(end - start) / seconds}")

    styx.close()

    for key, metadata in styx.delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def main():
    print("Generate and push workload to Styx")

    if N_ENTITIES < 3:
        print("Impossible to run this benchmark with one key")
        return

    minio = Minio("localhost:9000", access_key="minio", secret_key="minio123", secure=False)
    styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL, minio=minio)
    ycsb_init(styx_client, ycsb_operator)
    del styx_client
    time.sleep(5)

    with Pool(threads) as p:
        results = p.map(benchmark_runner, range(threads))

    results = {k: v for d in results for k, v in d.items()}
    assert len(results) == messages_per_second * seconds * threads

    pd.DataFrame({"request_id": list(results.keys()),
                  "timestamp": [res["timestamp"] for res in results.values()],
                  "op": [res["op"] for res in results.values()]
                  }).sort_values("timestamp").to_csv(f"{SAVE_DIR}/client_requests.csv", index=False)

    print("Workload completed")


if __name__ == "__main__":
    multiprocessing.set_start_method("fork")
    main()

    print()
    kafka_output_consumer.main(SAVE_DIR)

    print()
    calculate_metrics.main(
        messages_per_second,
        threads,
        warmup_seconds,
        SAVE_DIR
    )
