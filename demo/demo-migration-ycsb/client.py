import os
import random
import sys
import time
import multiprocessing
from multiprocessing import Pool

import pandas as pd
from timeit import default_timer as timer

from styx.client.sync_client import SyncStyxClient
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph

import kafka_output_consumer
import calculate_metrics

from tqdm import tqdm
from ycsb import ycsb_operator


N_ENTITIES = 1_000_000 # 10 million entities
random_100_bytes_0: bytes = os.urandom(100)
random_100_bytes_1: bytes = os.urandom(100)
random_100_bytes_2: bytes = os.urandom(100)
random_100_bytes_3: bytes = os.urandom(100)
random_100_bytes_4: bytes = os.urandom(100)
random_100_bytes_5: bytes = os.urandom(100)
random_100_bytes_6: bytes = os.urandom(100)
random_100_bytes_7: bytes = os.urandom(100)
random_100_bytes_8: bytes = os.urandom(100)
random_100_bytes_9: bytes = os.urandom(100)


threads = int(sys.argv[1])
N_PARTITIONS = int(sys.argv[2])
messages_per_second = int(sys.argv[3])
seconds = int(sys.argv[4])
SAVE_DIR: str = sys.argv[5]
warmup_seconds: int = int(sys.argv[6])

sleeps_per_second = 100
sleep_time = 0.0085
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9092'
####################################################################################################################
g = StateflowGraph('ycsb-benchmark', operator_state_backend=LocalStateBackend.DICT)
ycsb_operator.set_n_partitions(N_PARTITIONS)
g.add_operators(ycsb_operator)

def submit_graph(styx: SyncStyxClient):
    print(f'Partitions: {list(g.nodes.values())[0].n_partitions}')
    styx.submit_dataflow(g)
    print("Graph submitted")


def ycsb_init(styx: SyncStyxClient, operator: Operator):
    submit_graph(styx)
    time.sleep(5)
    # INSERT
    partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
    for i in tqdm(range(N_ENTITIES)):
        partition: int = styx.get_operator_partition(i, operator)
        # 1 int PK + 10 random bytes columns of 100 bytes each
        partitions[partition] |= {i: (random_100_bytes_0, random_100_bytes_1,
                                      random_100_bytes_2, random_100_bytes_3,
                                      random_100_bytes_4, random_100_bytes_5,
                                      random_100_bytes_6, random_100_bytes_7,
                                      random_100_bytes_8, random_100_bytes_9)}
        if i % 2_000 == 0 or i == N_ENTITIES - 1:
            for partition, kv_pairs in partitions.items():
                styx.send_batch_insert(operator=operator,
                                       partition=partition,
                                       function='insert_batch',
                                       key_value_pairs=kv_pairs)
            partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
            styx.flush()


def transactional_ycsb_generator(operator: Operator) -> [Operator, int, str, tuple[int, ]]:
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
        if cur_sec == 150:
            new_g = StateflowGraph('ycsb-benchmark', operator_state_backend=LocalStateBackend.DICT)
            ycsb_operator.set_n_partitions(N_PARTITIONS-1)
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

    styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)

    styx_client.open(consume=False)

    ycsb_init(styx_client, ycsb_operator)

    styx_client.close()

    time.sleep(25)

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
        N_PARTITIONS,
        messages_per_second,
        threads,
        warmup_seconds,
        SAVE_DIR
    )
