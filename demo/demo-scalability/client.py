from collections import defaultdict
import multiprocessing
import random
import sys
import time

multiprocessing.set_start_method("fork", force=True)
from multiprocessing import Pool
from timeit import default_timer as timer

import calculate_metrics
import kafka_output_consumer
from minio import Minio
import pandas as pd
from styx.client.sync_client import SyncStyxClient
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph
from tqdm import tqdm
from ycsb import ycsb_operator

threads = int(sys.argv[1])
barrier = multiprocessing.Barrier(threads)
PERC_MULT = float(sys.argv[2])
N_PARTITIONS = int(sys.argv[3])
N_ENTITIES = 1_000_000 * N_PARTITIONS
STARTING_MONEY = 1_000_000
messages_per_second = int(sys.argv[4])
sleeps_per_second = 100
sleep_time = 0.0085
seconds = int(sys.argv[5])
STYX_HOST: str = "localhost"
STYX_PORT: int = 8886
KAFKA_URL = "localhost:9092"
SAVE_DIR: str = sys.argv[6]
warmup_seconds: int = int(sys.argv[7])

g = StateflowGraph("ycsb-benchmark", operator_state_backend=LocalStateBackend.DICT)
ycsb_operator.set_n_partitions(N_PARTITIONS)
g.add_operators(ycsb_operator)

def submit_graph(styx: SyncStyxClient):
    print(f"Partitions: {list(g.nodes.values())[0].n_partitions}")
    styx.submit_dataflow(g)
    print("Graph submitted")


def ycsb_init(styx: SyncStyxClient, operator: Operator):
    styx.set_graph(g)
    styx.init_metadata(g)
    partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
    for i in tqdm(range(N_ENTITIES)):
        partition: int = styx.get_operator_partition(i, operator)
        partitions[partition][i] = STARTING_MONEY

    for partition, partition_data in partitions.items():
        styx.init_data(operator, partition, partition_data)
    time.sleep(5)
    submit_graph(styx)

def transactional_ycsb_generator(operator: Operator):
    # partition: list of keys
    key_per_partition: dict[int, list[int]] = defaultdict(list)
    for i in range(N_ENTITIES):
        part = i % N_PARTITIONS
        key_per_partition[part].append(i)
    while True:
        part_1 = random.randint(0, N_PARTITIONS - 1)
        if PERC_MULT > 0.0:
            coin = random.random()
            if coin > PERC_MULT:
                part_2 = part_1
            else:
                part_2 = random.choice([p for p in range(N_PARTITIONS) if p != part_1])
        else:
            part_2 = part_1
        key1 = random.choice(key_per_partition[part_1])
        key2 = random.choice(key_per_partition[part_2])
        while key2 == key1:
            key2 = random.choice(key_per_partition[part_2])
        yield operator, key1, "transfer", (key2, )


def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f"Generator: {proc_num} starting")
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    time.sleep(proc_num * 0.2)
    styx.open(consume=False)
    ycsb_generator = transactional_ycsb_generator(ycsb_operator)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    barrier.wait()
    start = timer()
    for sec in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(ycsb_generator)
            future = styx.send_event(operator=operator,
                                     key=key,
                                     function=func_name,
                                     params=params)
            timestamp_futures[future.request_id] = {"op": f"{func_name} {key}->{params[0]}"}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f"{sec} | Latency per second: {sec_end2 - sec_start}")
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
    main()

    print()
    kafka_output_consumer.main(SAVE_DIR)

    print()
    calculate_metrics.main(
        SAVE_DIR,
        warmup_seconds,
        PERC_MULT,
        N_PARTITIONS,
        messages_per_second,
        threads
    )
