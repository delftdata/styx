import sys
import time
import multiprocessing
multiprocessing.set_start_method("fork", force=True)
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
from zipfian_generator import ZipfGenerator


threads = int(sys.argv[1])
barrier = multiprocessing.Barrier(threads)
N_ENTITIES = int(sys.argv[2])
N_PARTITIONS = int(sys.argv[3])
STARTING_MONEY = 1_000_000
ZIPF_CONST = float(sys.argv[4])
messages_per_second = int(sys.argv[5])
sleeps_per_second = 100
sleep_time = 0.0085
seconds = int(sys.argv[6])
key_list: list[int] = list(range(N_ENTITIES))
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
# STYX_HOST: str = '35.229.80.128'
# STYX_PORT: int = 8888
KAFKA_URL = 'localhost:9092'
# KAFKA_URL = '35.229.114.18:9094'
SAVE_DIR: str = sys.argv[7]
warmup_seconds: int = int(sys.argv[8])
run_with_validation = sys.argv[9].lower() == "true"
####################################################################################################################
g = StateflowGraph('ycsb-benchmark', operator_state_backend=LocalStateBackend.DICT)
ycsb_operator.set_n_partitions(N_PARTITIONS)
g.add_operators(ycsb_operator)

def submit_graph(styx: SyncStyxClient):
    print(f'Partitions: {list(g.nodes.values())[0].n_partitions}')
    styx.submit_dataflow(g)
    print("Graph submitted")


def ycsb_init(styx: SyncStyxClient, operator: Operator, keys: list[int]):
    styx.set_graph(g)
    styx.init_metadata(g)
    partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
    for i in tqdm(keys):
        partition: int = styx.get_operator_partition(i, operator)
        partitions[partition][i] = STARTING_MONEY

    for partition, partition_data in partitions.items():
        styx.init_data(operator, partition, partition_data)
    time.sleep(5)
    submit_graph(styx)


def transactional_ycsb_generator(keys,
                                 operator: Operator,
                                 n: int,
                                 zipf_const: float):
    zipf_gen = ZipfGenerator(items=n, zipf_const=zipf_const)
    uniform_gen = ZipfGenerator(items=n, zipf_const=0.0)
    while True:
        key = keys[next(uniform_gen)]
        key2 = keys[next(zipf_gen)]
        while key2 == key:
            key2 = keys[next(zipf_gen)]
        yield operator, key, 'transfer', (key2, )


def read_only_ycsb_generator(keys, operator: Operator, n: int, zipf_const: float):
    zipf_gen = ZipfGenerator(items=n, zipf_const=zipf_const)
    while True:
        key = keys[next(zipf_gen)]
        yield operator, key, 'read', ()


def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    time.sleep(proc_num * 0.2)
    styx.open(consume=False)
    ycsb_generator = transactional_ycsb_generator(key_list, ycsb_operator, N_ENTITIES, zipf_const=ZIPF_CONST)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    barrier.wait()
    start = timer()
    for sec in range(seconds):
        sec_start = timer()
        step = max(1, messages_per_second // sleeps_per_second)
        for i in range(messages_per_second):
            if i % step == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(ycsb_generator)
            future = styx.send_event(operator=operator,
                                     key=key,
                                     function=func_name,
                                     params=params)
            timestamp_futures[future.request_id] = {"op": f'{func_name} {key}->{params[0]}'}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'{sec} | Latency per second: {sec_end2 - sec_start}')
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
    ycsb_init(styx_client, ycsb_operator, key_list)
    del styx_client
    time.sleep(5)

    with Pool(threads) as p:
        results = p.map(benchmark_runner, range(threads))

    results = {k: v for d in results for k, v in d.items()}
    assert len(results) == messages_per_second * seconds * threads

    if run_with_validation:
        # wait for system to stabilize
        time.sleep(30)

        styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)

        styx_client.open(consume=False)

        print('Starting Consistency measurement')

        validation_results = {}

        for key in key_list:
            future = styx_client.send_event(operator=ycsb_operator,
                                            key=key,
                                            function="read")
            validation_results[future.request_id] = {"op": f'read -> {key}'}

        styx_client.close()

        for request_id, metadata in styx_client.delivery_timestamps.items():
            validation_results[request_id]["timestamp"] = metadata

        assert len(validation_results) == N_ENTITIES

    pd.DataFrame({"request_id": list(results.keys()),
                  "timestamp": [res["timestamp"] for res in results.values()],
                  "op": [res["op"] for res in results.values()]
                  }).sort_values("timestamp").to_csv(f'{SAVE_DIR}/client_requests.csv', index=False)

    print('Workload completed')


if __name__ == "__main__":
    main()

    print()
    kafka_output_consumer.main(SAVE_DIR)

    print()
    calculate_metrics.main(
        N_ENTITIES,
        N_PARTITIONS,
        messages_per_second,
        ZIPF_CONST,
        threads,
        warmup_seconds,
        SAVE_DIR,
        run_with_validation
    )
