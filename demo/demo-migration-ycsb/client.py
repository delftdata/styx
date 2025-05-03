import os
import pickle
import random
import string
import sys
import time
import multiprocessing
from multiprocessing import Pool

import pandas as pd
from timeit import default_timer as timer

from minio import Minio
from styx.client.sync_client import SyncStyxClient
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph

import kafka_output_consumer
import calculate_metrics

from tqdm import tqdm
from ycsb import ycsb_operator


def ycsb_field(size=100, seed=0):
    "Translated from the original Java code"
    if seed is not None:
        random.seed(seed)
    charset = string.ascii_letters + string.digits  # 62 characters
    return ''.join(random.choices(charset, k=size))


N_ENTITIES = int(sys.argv[8]) # 10 million entities
SECOND_TO_TAKE_MIGRATION = 60

threads = int(sys.argv[1])
START_N_PARTITIONS = int(sys.argv[2])
END_N_PARTITIONS = int(sys.argv[3])
messages_per_second = int(sys.argv[4])
seconds = int(sys.argv[5])
SAVE_DIR: str = sys.argv[6]
warmup_seconds: int = int(sys.argv[7])

sleeps_per_second = 100
sleep_time = 0.0085
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9092'
YCSB_DATASET_PATH = "ycsb_dataset.pkl"
####################################################################################################################
g = StateflowGraph('ycsb-benchmark', operator_state_backend=LocalStateBackend.DICT)
ycsb_operator.set_n_partitions(START_N_PARTITIONS)
g.add_operators(ycsb_operator)

def submit_graph(styx: SyncStyxClient):
    print(f'Partitions: {list(g.nodes.values())[0].n_partitions}')
    styx.submit_dataflow(g)
    print("Graph submitted")


def ycsb_init(styx: SyncStyxClient, operator: Operator):
    styx.set_graph(g)
    styx.init_metadata(g)
    partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
    if os.path.exists(YCSB_DATASET_PATH):
        print("Loading YCSB dataset...")
        with open(YCSB_DATASET_PATH, 'rb') as f:
            partitions = pickle.load(f)
    else:
        print("Generating YCSB dataset...")
        for i in tqdm(range(N_ENTITIES)):
            partition: int = styx.get_operator_partition(i, operator)
            partitions[partition][i] = (ycsb_field(), ycsb_field(),
                                        ycsb_field(), ycsb_field(),
                                        ycsb_field(), ycsb_field(),
                                        ycsb_field(), ycsb_field(),
                                        ycsb_field(), ycsb_field())
        with open(YCSB_DATASET_PATH, 'wb') as f:
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
    print(f'Generator: {proc_num} starting')
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
            timestamp_futures[future.request_id] = {"op": f'{func_name} {key}'}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
        if cur_sec == SECOND_TO_TAKE_MIGRATION:
            new_g = StateflowGraph('ycsb-benchmark', operator_state_backend=LocalStateBackend.DICT)
            ycsb_operator.set_n_partitions(END_N_PARTITIONS)
            new_g.add_operators(ycsb_operator)
            styx.submit_dataflow(new_g)
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')

    styx.close()

    for key, metadata in styx.delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def main():
    print('Generate and push workload to Styx')

    if N_ENTITIES < 3:
        print("Impossible to run this benchmark with one key")
        return

    minio = Minio('localhost:9000', access_key='minio', secret_key='minio123', secure=False)
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
                  }).sort_values("timestamp").to_csv(f'{SAVE_DIR}/client_requests.csv', index=False)

    print('Workload completed')


if __name__ == "__main__":
    multiprocessing.set_start_method('fork')
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
