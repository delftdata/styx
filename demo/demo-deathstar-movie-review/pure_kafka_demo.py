import hashlib
import multiprocessing
import subprocess

from minio import Minio

multiprocessing.set_start_method("fork", force=True)
from multiprocessing import Pool
import random
import sys
import time
from timeit import default_timer as timer
import uuid

import calculate_metrics
from graph import (
    compose_review_operator,
    frontend_operator,
    movie_id_operator,
    movie_info_operator,
    plot_operator,
    rating_operator,
    text_operator,
    unique_id_operator,
    user_operator,
)
import kafka_output_consumer
from movie_data import movie_data
import pandas as pd
from styx.client import SyncStyxClient
from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph
from workload_data import charset, movie_titles

SAVE_DIR: str = sys.argv[1]
threads = int(sys.argv[2])
barrier = multiprocessing.Barrier(threads)
N_PARTITIONS = int(sys.argv[3])
messages_per_second = int(sys.argv[4])
sleeps_per_second = 100
sleep_time = 0.0085
seconds = int(sys.argv[5])
warmup_seconds = int(sys.argv[6])
STYX_HOST: str = "localhost"
STYX_PORT: int = 8886
KAFKA_URL = "localhost:9092"
kill_at = int(sys.argv[7]) if len(sys.argv) > 7 else -1

g = StateflowGraph("deathstar_movie_review", operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
compose_review_operator.set_n_partitions(N_PARTITIONS)
movie_id_operator.set_n_partitions(N_PARTITIONS)
movie_info_operator.set_n_partitions(N_PARTITIONS)
plot_operator.set_n_partitions(N_PARTITIONS)
rating_operator.set_n_partitions(N_PARTITIONS)
text_operator.set_n_partitions(N_PARTITIONS)
unique_id_operator.set_n_partitions(N_PARTITIONS)
user_operator.set_n_partitions(N_PARTITIONS)
frontend_operator.set_n_partitions(N_PARTITIONS)
g.add_operators(
    compose_review_operator,
    movie_id_operator,
    movie_info_operator,
    plot_operator,
    rating_operator,
    text_operator,
    unique_id_operator,
    user_operator,
    frontend_operator
)


# -------------------------------------------------------------------------------------
# init_data helpers
# -------------------------------------------------------------------------------------

def styx_hash(styx: SyncStyxClient, key, op) -> int:
    return styx.get_operator_partition(key, op)


def init_partitioned(styx: SyncStyxClient, op, items):
    """
    Bulk init operator state by partition:
      items: iterable[(key, value)]
      value must match what the operator stores via ctx.put(...)
    """
    parts = {p: {} for p in range(N_PARTITIONS)}
    for key, value in items:
        p = styx_hash(styx, key, op)
        parts[p][key] = value

    for p, d in parts.items():
        if d:
            styx.init_data(op, p, d)


def build_user_items():
    """
    user_operator.register_user(ctx, user_data) does: ctx.put(user_data)
    keyed by username.
    """
    user_items = []
    for i in range(1000):
        user_id = f"user{i}"
        username = f"username_{i}"
        password = f"password_{i}"

        hasher = hashlib.new("sha512")
        salt = uuid.uuid1().bytes
        hasher.update(password.encode())
        hasher.update(salt)
        password_hash = hasher.hexdigest()

        user_data = {
            "userId": user_id,
            "FirstName": "firstname",
            "LastName": "lastname",
            "Username": username,
            "Password": password_hash,
            "Salt": salt,
        }
        user_items.append((username, user_data))
    return user_items


def build_movie_items():
    """
    movie_info_operator.write(ctx, info): ctx.put(info)     keyed by movie_id
    plot_operator.write(ctx, plot):       ctx.put(plot)     keyed by movie_id
    movie_id_operator.register_movie_id(ctx, movie_id): ctx.put({"movieId": movie_id}) keyed by title
    """
    movie_info_items = []
    plot_items = []
    movie_id_items = []

    for movie in movie_data:
        movie_id = movie["MovieId"]
        title = movie["Title"]

        movie_info_items.append((movie_id, movie))
        plot_items.append((movie_id, "plot"))
        movie_id_items.append((title, {"movieId": movie_id}))

    return movie_info_items, plot_items, movie_id_items


def populate_with_init_data(styx: SyncStyxClient):
    # users
    user_items = build_user_items()
    init_partitioned(styx, user_operator, user_items)

    # movies
    movie_info_items, plot_items, movie_id_items = build_movie_items()
    init_partitioned(styx, movie_info_operator, movie_info_items)
    init_partitioned(styx, plot_operator, plot_items)
    init_partitioned(styx, movie_id_operator, movie_id_items)


def submit_graph(styx: SyncStyxClient):
    print(list(g.nodes.values())[0].n_partitions)
    styx.submit_dataflow(g)
    print("Graph submitted")


def deathstar_init(styx: SyncStyxClient):
    styx.set_graph(g)
    styx.init_metadata(g)

    populate_with_init_data(styx)
    styx.notify_init_data_complete()
    print("Data populated with init_data")
    time.sleep(1)

    styx.submit_dataflow(g)
    print("Graph submitted")


# -------------------------------------------------------------------------------------
# Workload
# -------------------------------------------------------------------------------------

def compose_review(c):
    user_index = random.randint(0, 999)
    username = f"username_{user_index}"
    title = random.choice(movie_titles)
    rating = random.randint(0, 10)
    text = "".join(random.choice(charset) for _ in range(256))
    return frontend_operator, c, "compose", (username, title, rating, text)


def deathstar_workload_generator():
    c = 0
    while True:
        yield compose_review(c)
        c += 1


def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f"Generator: {proc_num} starting")
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    deathstar_generator = deathstar_workload_generator()
    timestamp_futures: dict[bytes, dict] = {}
    barrier.wait()
    start = timer()
    for second in range(seconds):
        if proc_num == 0 and 0 <= kill_at == second:
            subprocess.run(["docker", "kill", "styx-worker-1"], check=False)
            print("KILL -> styx-worker-1 done")
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(deathstar_generator)
            future = styx.send_event(operator=operator,
                                     key=key,
                                     function=func_name,
                                     params=params)
            timestamp_futures[future.request_id] = {"op": f"{func_name} {key}->{params}"}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f"Latency per second: {sec_end2 - sec_start}")
    end = timer()
    print(f"Average latency per second: {(end - start) / seconds}")
    styx.close()
    for key, metadata in styx.delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def main():
    minio = Minio("localhost:9000", access_key="minio", secret_key="minio123", secure=False)
    styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL, minio=minio)
    styx_client.open(consume=False)

    deathstar_init(styx_client)

    styx_client.flush()
    time.sleep(10)

    with Pool(threads) as p:
        results = p.map(benchmark_runner, range(threads))

    styx_client.close()

    results = {k: v for d in results for k, v in d.items()}

    pd.DataFrame({
        "request_id": list(results.keys()),
        "timestamp": [res["timestamp"] for res in results.values()],
        "op": [res["op"] for res in results.values()],
    }).sort_values("timestamp").to_csv(f"{SAVE_DIR}/client_requests.csv", index=False)


if __name__ == "__main__":
    main()

    print()
    kafka_output_consumer.main(SAVE_DIR)

    print()
    calculate_metrics.main(
        SAVE_DIR,
        messages_per_second,
        warmup_seconds,
        threads
    )
