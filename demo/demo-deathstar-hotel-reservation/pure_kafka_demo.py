import multiprocessing
multiprocessing.set_start_method("fork", force=True)
from multiprocessing import Pool
import random
import sys
import time
from timeit import default_timer as timer

import calculate_metrics
from graph import (
    flight_operator,
    geo_operator,
    hotel_operator,
    order_operator,
    rate_operator,
    recommendation_operator,
    search_operator,
    user_operator,
)
import kafka_output_consumer
from minio import Minio
import pandas as pd
from styx.client import SyncStyxClient
from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph

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

g = StateflowGraph("deathstar_hotel_reservations", operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
flight_operator.set_n_partitions(N_PARTITIONS)
geo_operator.set_n_partitions(N_PARTITIONS)
hotel_operator.set_n_partitions(N_PARTITIONS)
order_operator.set_n_partitions(N_PARTITIONS)
rate_operator.set_n_partitions(N_PARTITIONS)
recommendation_operator.set_n_partitions(N_PARTITIONS)
search_operator.set_n_partitions(N_PARTITIONS)
user_operator.set_n_partitions(N_PARTITIONS)
g.add_operators(
    flight_operator,
    geo_operator,
    hotel_operator,
    order_operator,
    rate_operator,
    recommendation_operator,
    search_operator,
    user_operator,
)


def styx_hash(styx: SyncStyxClient, key, op) -> int:
    # IMPORTANT: op is the Operator object; Styx knows how to map it.
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


def populate_with_init_data(styx: SyncStyxClient):
    # GEO: ctx.put({"Plat": lat, "Plon": lon})
    geo_items = [
        (1, {"Plat": 37.7867, "Plon": 0}),
        (2, {"Plat": 37.7854, "Plon": -122.4005}),
        (3, {"Plat": 37.7867, "Plon": -122.4071}),
        (4, {"Plat": 37.7936, "Plon": -122.3930}),
        (5, {"Plat": 37.7831, "Plon": -122.4181}),
        (6, {"Plat": 37.7863, "Plon": -122.4015}),
    ]
    for i in range(7, 81):
        lat = 37.7835 + i / 500.0 * 3
        lon = -122.41 + i / 500.0 * 4
        geo_items.append((i, {"Plat": lat, "Plon": lon}))
    init_partitioned(styx, geo_operator, geo_items)

    # RATE: ctx.put({"code": code, "Indate": in_date, "Outdate": out_date, "RoomType": room_type})
    rate_items = [
        (1, {"code": "RACK", "Indate": "2015-04-09", "Outdate": "2015-04-10",
             "RoomType": {"BookableRate": 190.0, "Code": "KNG", "RoomDescription": "King sized bed",
                          "TotalRate": 109.0, "TotalRateInclusive": 123.17}}),
        (2, {"code": "RACK", "Indate": "2015-04-09", "Outdate": "2015-04-10",
             "RoomType": {"BookableRate": 139.0, "Code": "QN", "RoomDescription": "Queen sized bed",
                          "TotalRate": 139.0, "TotalRateInclusive": 153.09}}),
        (3, {"code": "RACK", "Indate": "2015-04-09", "Outdate": "2015-04-10",
             "RoomType": {"BookableRate": 109.0, "Code": "KNG", "RoomDescription": "King sized bed",
                          "TotalRate": 109.0, "TotalRateInclusive": 123.17}}),
    ]

    for i in range(4, 80):
        if i % 3 == 0:
            hotel_id = i
            end_date = "2015-04-" + ("17" if i % 2 == 0 else "24")
            rate = 109.0
            rate_inc = 123.17
            if i % 5 == 1:
                rate, rate_inc = 120.0, 140.0
            elif i % 5 == 2:
                rate, rate_inc = 124.0, 144.0
            elif i % 5 == 3:
                rate, rate_inc = 132.0, 158.0
            elif i % 5 == 4:
                rate, rate_inc = 232.0, 258.0

            rate_items.append((
                hotel_id,
                {"code": "RACK", "Indate": "2015-04-09", "Outdate": end_date,
                 "RoomType": {"BookableRate": rate, "Code": "KNG", "RoomDescription": "King sized bed",
                              "TotalRate": rate, "TotalRateInclusive": rate_inc}}
            ))
    init_partitioned(styx, rate_operator, rate_items)

    rec_items = [
        (1, {"HLat": 37.7867, "HLon": -122.4112, "HRate": 109.00, "HPrice": 150.00}),
        (2, {"HLat": 37.7854, "HLon": -122.4005, "HRate": 139.00, "HPrice": 120.00}),
        (3, {"HLat": 37.7834, "HLon": -122.4071, "HRate": 109.00, "HPrice": 190.00}),
        (4, {"HLat": 37.7936, "HLon": -122.3930, "HRate": 129.00, "HPrice": 160.00}),
        (5, {"HLat": 37.7831, "HLon": -122.4181, "HRate": 119.00, "HPrice": 140.00}),
        (6, {"HLat": 37.7863, "HLon": -122.4015, "HRate": 149.00, "HPrice": 200.00}),
    ]
    for i in range(7, 80):
        lat = 37.7835 + i / 500.0 * 3
        lon = -122.41 + i / 500.0 * 4
        rate = 135.00
        rate_inc = 179.00
        if i % 3 == 0:
            if i % 5 == 0:
                rate, rate_inc = 109.00, 123.17
            elif i % 5 == 1:
                rate, rate_inc = 120.00, 140.00
            elif i % 5 == 2:
                rate, rate_inc = 124.00, 144.00
            elif i % 5 == 3:
                rate, rate_inc = 132.00, 158.00
            elif i % 5 == 4:
                rate, rate_inc = 232.00, 258.00

        rec_items.append((i, {"HLat": lat, "HLon": lon, "HRate": rate, "HPrice": rate_inc}))
    init_partitioned(styx, recommendation_operator, rec_items)

    # USER: ctx.put(password)
    user_items = []
    for i in range(501):
        username = f"Cornell_{i}"
        password = str(i) * 10
        user_items.append((username, password))
    init_partitioned(styx, user_operator, user_items)

    # HOTEL: ctx.put({"Cap": cap, "Customers": []})
    hotel_items = [(i, {"Cap": 10, "Customers": []}) for i in range(100)]
    init_partitioned(styx, hotel_operator, hotel_items)

    # FLIGHT: ctx.put({"Cap": cap, "Customers": []})
    flight_items = [(i, {"Cap": 10, "Customers": []}) for i in range(100)]
    init_partitioned(styx, flight_operator, flight_items)


def submit_graph(styx: SyncStyxClient):
    print(f"Partitions: {list(g.nodes.values())[0].n_partitions}")
    styx.submit_dataflow(g)
    print("Graph submitted")


def deathstar_init(styx: SyncStyxClient):
    styx.set_graph(g)
    styx.init_metadata(g)

    populate_with_init_data(styx)
    print("Data populated with init_data")
    styx.notify_init_data_complete()

    styx.submit_dataflow(g)
    print("Graph submitted")

# -------------------------------------------------------------------------------------
# Workload generation (unchanged)
# -------------------------------------------------------------------------------------

def search_hotel(c):
    in_date = random.randint(9, 23)
    out_date = random.randint(in_date + 1, 24)
    if in_date < 10:
        in_date_str = f"2015-04-0{in_date}"
    else:
        in_date_str = f"2015-04-{in_date}"
    if out_date < 10:
        out_date_str = f"2015-04-0{in_date}"
    else:
        out_date_str = f"2015-04-{in_date}"
    lat = 38.0235 + (random.randint(0, 481) - 240.5) / 1000.0
    lon = -122.095 + (random.randint(0, 325) - 157.0) / 1000.0
    return search_operator, c, "nearby", (lat, lon, in_date_str, out_date_str)


def recommend(c):
    coin = random.random()
    if coin < 0.33:
        req_param = "dis"
    elif coin < 0.66:
        req_param = "rate"
    else:
        req_param = "price"
    lat = 38.0235 + (random.randint(0, 481) - 240.5) / 1000.0
    lon = -122.095 + (random.randint(0, 325) - 157.0) / 1000.0
    return recommendation_operator, c, "get_recommendations", (req_param, lat, lon)


def user_login():
    user_id = str(random.randint(0, 500))
    username = f"Cornell_{user_id}"
    password = user_id * 10
    return user_operator, username, "check_user", (password,)


def reserve_all(c):
    hotel_id = random.randint(0, 99)
    flight_id = random.randint(0, 99)
    user_id = "user1"
    return order_operator, c, "create", (hotel_id, flight_id, user_id)


def deathstar_workload_generator():
    search_ratio = 0.6
    recommend_ratio = 0.39
    user_ratio = 0.005
    reserve_ratio = 0.005
    c = 0
    while True:
        coin = random.random()
        if coin < search_ratio:
            yield search_hotel(c)
        elif coin < search_ratio + recommend_ratio:
            yield recommend(c)
        elif coin < search_ratio + recommend_ratio + user_ratio:
            yield user_login()
        else:
            yield reserve_all(c)
        c += 1


# -------------------------------------------------------------------------------------
# Benchmark runner (still uses send_event for workload traffic; only init changes)
# -------------------------------------------------------------------------------------

def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f"Generator: {proc_num} starting")
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    deathstar_generator = deathstar_workload_generator()
    timestamp_futures: dict[bytes, dict] = {}
    barrier.wait()
    start = timer()
    for _ in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(deathstar_generator)
            future = styx.send_event(
                operator=operator,
                key=key,
                function=func_name,
                params=params
            )
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
