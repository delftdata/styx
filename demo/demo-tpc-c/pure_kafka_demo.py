import subprocess
from collections import defaultdict
import csv
from datetime import datetime
import multiprocessing
from multiprocessing import Pool
import os
from pathlib import Path
import pickle
import random
import sys
import time
from timeit import default_timer as timer
from typing import Any

import calculate_metrics
from graph import (
    customer_idx_operator,
    customer_operator,
    district_operator,
    history_operator,
    item_operator,
    new_order_operator,
    new_order_txn_operator,
    order_line_operator,
    order_operator,
    payment_txn_operator,
    stock_operator,
    warehouse_operator,
)
import kafka_output_consumer
from minio import Minio
import pandas as pd
import rand
from setuptools._distutils.util import strtobool
from styx.client import SyncStyxClient
from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph

random.seed(42)

SAVE_DIR: str = sys.argv[1]
threads = int(sys.argv[2])
N_PARTITIONS = int(sys.argv[3])
messages_per_second = int(sys.argv[4])
sleeps_per_second = 100
sleep_time = 0.0085
seconds = int(sys.argv[5])
STYX_HOST: str = "localhost"
STYX_PORT: int = 8886
KAFKA_URL = "localhost:9092"
warmup_seconds = int(sys.argv[6])
N_W = int(sys.argv[7])
N_PARTITIONS = min(N_W, N_PARTITIONS)
C_Per_District = 3000
D_Per_Warehouse = 10
N_D = N_W * D_Per_Warehouse
N_I = 100_000

MIN_OL_CNT: int = 5
MAX_OL_CNT: int = 15
MAX_OL_QUANTITY: int = 10
MIN_PAYMENT = 1.0
MAX_PAYMENT = 5000.0
enable_compression: bool = bool(strtobool(sys.argv[8]))
use_composite_keys: bool = bool(strtobool(sys.argv[9]))
use_fallback_cache: bool = bool(strtobool(sys.argv[10]))
os.environ["ENABLE_COMPRESSION"] = str(enable_compression)
os.environ["USE_COMPOSITE_KEYS"] = str(use_composite_keys)
os.environ["USE_FALLBACK_CACHE"] = str(use_fallback_cache)
kill_at = int(sys.argv[11]) if len(sys.argv) > 11 else -1


customers_per_district: dict[tuple, list] = {}

script_path = os.path.dirname(os.path.realpath(__file__))


flush_interval = 100
data_folder = f"data_{N_W}"
DATA_DIR = os.path.join(script_path, data_folder)
tag = f"ck{int(use_composite_keys)}_p{N_PARTITIONS}"
CACHE_DIR = os.path.join(script_path, f"{data_folder}_cache_{tag}")
Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)

g = StateflowGraph("tpcc_benchmark", operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
customer_operator.set_n_partitions(N_PARTITIONS)
district_operator.set_n_partitions(N_PARTITIONS)
history_operator.set_n_partitions(N_PARTITIONS)
item_operator.set_n_partitions(N_PARTITIONS)
new_order_operator.set_n_partitions(N_PARTITIONS)
order_operator.set_n_partitions(N_PARTITIONS)
order_line_operator.set_n_partitions(N_PARTITIONS)
stock_operator.set_n_partitions(N_PARTITIONS)
warehouse_operator.set_n_partitions(N_PARTITIONS)
new_order_txn_operator.set_n_partitions(N_PARTITIONS)
customer_idx_operator.set_n_partitions(N_PARTITIONS)
payment_txn_operator.set_n_partitions(N_PARTITIONS)
g.add_operators(customer_operator, district_operator, history_operator, item_operator, new_order_operator,
                order_operator, order_line_operator, stock_operator, warehouse_operator,
                new_order_txn_operator, customer_idx_operator, payment_txn_operator)



# -------------------------------------------------------------------------------------
# Cache helper
# -------------------------------------------------------------------------------------

def load_or_build(name: str, builder):
    path = os.path.join(CACHE_DIR, f"{name}_p{N_PARTITIONS}.pkl")
    if os.path.exists(path):
        print(f"[CACHE HIT] {name}")
        with open(path, "rb") as f:
            return pickle.load(f)

    print(f"[CACHE MISS] Building {name}")
    obj = builder()
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)
    print(f"[CACHE WRITE] {name}")
    return obj

# -------------------------------------------------------------------------------------
# Populate helpers
# -------------------------------------------------------------------------------------

def build_simple(styx, csv_name, key_fn, op):
    partitions = {p: {} for p in range(N_PARTITIONS)}
    with open(os.path.join(DATA_DIR, csv_name)) as f:
        reader = csv.reader(f)
        for row in reader:
            key, value = key_fn(row)
            part = styx_hash(styx, key, op)
            partitions[part][key] = value
    return partitions


def styx_hash(styx, key, op):
    return styx.get_operator_partition(key, op)


# -------------------------------------------------------------------------------------
# Populate functions
# -------------------------------------------------------------------------------------

def populate_warehouse(styx):
    parts = load_or_build("warehouse", lambda: build_simple(styx,
        "warehouse.csv",
        lambda r: (
            int(r[0]),
            {
                "W_NAME": r[1], "W_STREET_1": r[2], "W_STREET_2": r[3],
                "W_CITY": r[4], "W_STATE": r[5], "W_ZIP": r[6],
                "W_TAX": float(r[7]), "W_YTD": float(r[8])
            }
        ),
        warehouse_operator
    ))
    for p, d in parts.items():
        styx.init_data(warehouse_operator, p, d)


def populate_district(styx):
    parts = load_or_build("district", lambda: build_simple(styx,
        "district.csv",
        lambda r: (
            f"{r[1]}:{r[0]}",
            {
                "D_ID": int(r[0]), "D_W_ID": int(r[1]), "D_NAME": r[2],
                "D_STREET_1": r[3], "D_STREET_2": r[4], "D_CITY": r[5],
                "D_STATE": r[6], "D_ZIP": r[7], "D_TAX": float(r[8]),
                "D_YTD": float(r[9]), "D_NEXT_O_ID": int(r[10])
            }
        ),
        district_operator
    ))
    for p, d in parts.items():
        styx.init_data(district_operator, p, d)


def populate_customer(styx):
    def builder():
        cust_parts = {p: {} for p in range(N_PARTITIONS)}
        idx_parts = {p: {} for p in range(N_PARTITIONS)}
        idx_buf = defaultdict(list)

        with open(os.path.join(DATA_DIR, "customer.csv")) as f:
            reader = csv.reader(f)
            for r in reader:
                key = f"{r[2]}:{r[1]}:{r[0]}"
                part = styx_hash(styx, key, customer_operator)
                cust_parts[part][key] = {
                    "C_ID": int(r[0]), "C_D_ID": int(r[1]), "C_W_ID": int(r[2]),
                    "C_FIRST": r[3], "C_MIDDLE": r[4], "C_LAST": r[5],
                    "C_STREET_1": r[6], "C_STREET_2": r[7], "C_CITY": r[8],
                    "C_STATE": r[9], "C_ZIP": r[10], "C_PHONE": r[11],
                    "C_SINCE": r[12], "C_CREDIT": r[13],
                    "C_CREDIT_LIM": float(r[14]), "C_DISCOUNT": float(r[15]),
                    "C_BALANCE": float(r[16]), "C_YTD_PAYMENT": float(r[17]),
                    "C_PAYMENT_CNT": int(r[18]), "C_DELIVERY_CNT": int(r[19]),
                    "C_DATA": r[20]
                }

                idx_key = f"{r[2]}:{r[1]}:{r[5]}"
                idx_buf[idx_key].append((r[3], f"{r[2]}:{r[1]}:{r[0]}"))

        for k, vals in idx_buf.items():
            part = styx_hash(styx, k, customer_idx_operator)
            idx_parts[part][k] = [v[1] for v in sorted(vals)]

        return cust_parts, idx_parts

    cust_parts, idx_parts = load_or_build("customer", builder)

    for p, d in cust_parts.items():
        styx.init_data(customer_operator, p, d)
    for p, d in idx_parts.items():
        styx.init_data(customer_idx_operator, p, d)


def submit_graph(styx: SyncStyxClient):
    print(list(g.nodes.values())[0].n_partitions)
    styx.submit_dataflow(g)
    print("Graph submitted")


def populate_simple(styx, name, csv_name, key_fn, op):
    parts = load_or_build(name, lambda: build_simple(styx, csv_name, key_fn, op))
    for p, d in parts.items():
        styx.init_data(op, p, d)


def populate_all(styx):
    populate_warehouse(styx)
    populate_district(styx)
    populate_customer(styx)

    populate_simple(styx, "history", "history.csv",
        lambda r: (f"{r[4]}:{r[3]}:{r[0]}", {
            "H_C_ID": int(r[0]), "H_C_D_ID": int(r[1]),
            "H_C_W_ID": int(r[2]), "H_D_ID": int(r[3]),
            "H_W_ID": int(r[4]), "H_DATE": r[5],
            "H_AMOUN": r[6], "H_DATA": r[7]
        }), history_operator)

    populate_simple(styx, "new_order", "new_order.csv",
        lambda r: (f"{r[2]}:{r[1]}:{r[0]}", {
            "NO_O_ID": int(r[0]), "NO_D_ID": int(r[1]), "NO_W_ID": int(r[2])
        }), new_order_operator)

    populate_simple(styx,"order", "order.csv",
        lambda r: (f"{r[2]}:{r[1]}:{r[0]}", {
            "O_ID": int(r[0]), "O_D_ID": int(r[1]), "O_W_ID": int(r[2]),
            "O_C_ID": int(r[3]), "O_ENTRY_D": r[4],
            "O_CARRIER_ID": r[5], "O_OL_CNT": int(r[6]),
            "O_ALL_LOCAL": bool(r[7])
        }), order_operator)

    populate_simple(styx, "order_line", "order_line.csv",
        lambda r: (f"{r[2]}:{r[1]}:{r[0]}:{r[3]}", {
            "OL_O_ID": int(r[0]), "OL_D_ID": int(r[1]),
            "OL_W_ID": int(r[2]), "OL_NUMBER": int(r[3]),
            "OL_I_ID": int(r[4]), "OL_SUPPLY_W_ID": int(r[5]),
            "OL_DELIVERY_D": r[6], "OL_QUANTITY": int(r[7]),
            "OL_AMOUNT": float(r[8]), "OL_DIST_INFO": r[9]
        }), order_line_operator)

    populate_simple(styx, "item", "item.csv",
        lambda r: (int(r[0]), {
            "I_IM_ID": int(r[1]), "I_NAME": r[2],
            "I_PRICE": float(r[3]), "I_DATA": r[4]
        }), item_operator)

    populate_simple(styx, "stock", "stock.csv",
        lambda r: (f"{r[1]}:{r[0]}", {
            "S_I_ID": int(r[0]), "S_W_ID": int(r[1]),
            "S_QUANTITY": int(r[2]), "S_DIST_01": r[3],
            "S_DIST_02": r[4], "S_DIST_03": r[5],
            "S_DIST_04": r[6], "S_DIST_05": r[7],
            "S_DIST_06": r[8], "S_DIST_07": r[9],
            "S_DIST_08": r[10], "S_DIST_09": r[11],
            "S_DIST_10": r[12], "S_YTD": int(r[13]),
            "S_ORDER_CNT": int(r[14]), "S_REMOTE_CNT": int(r[15]),
            "S_DATA": r[16]
        }), stock_operator)


def tpc_c_init(styx):
    styx.set_graph(g)
    styx.init_metadata(g)
    populate_all(styx)
    styx.notify_init_data_complete()
    time.sleep(5)
    styx.submit_dataflow(g)


def make_item_id() -> int:
    return rand.nu_rand(8191, 1, N_I)


def make_customer_id():
    return rand.nu_rand(1023, 1, C_Per_District)


def get_new_order_transaction(front_end_key):
    """Return parameters for NEW_ORDER"""
    params: dict[str, Any] = {
        "W_ID": random.randint(1, N_W),
        "D_ID": random.randint(1, D_Per_Warehouse),
        "C_ID": make_customer_id(),
        "O_ENTRY_D": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
    }

    rollback = random.randint(1, 100) == 1  # 1% rollback chance

    params["I_IDS"] = []
    params["I_W_IDS"] = []
    params["I_QTYS"] = []

    ol_cnt = random.randint(MIN_OL_CNT, MAX_OL_CNT)

    for i in range(ol_cnt):
        if rollback and i + 1 == ol_cnt:
            params["I_IDS"].append(N_I + 1)  # Invalid ID triggers rollback
        else:
            i_id = make_item_id()
            while i_id in params["I_IDS"]:
                i_id = make_item_id()
            params["I_IDS"].append(i_id)

        # Decide if this item comes from a remote warehouse
        remote = random.randint(1, 100) == 1  # 1% chance
        if N_W > 1 and remote:
            params["I_W_IDS"].append(
                rand.number_excluding(1, N_W, params["W_ID"])
            )
        else:
            params["I_W_IDS"].append(params["W_ID"])

        params["I_QTYS"].append(rand.number(1, MAX_OL_QUANTITY))

    # Ensure at least one item is from the local warehouse
    if not any(w_id == params["W_ID"] for w_id in params["I_W_IDS"]):
        params["I_W_IDS"][random.randint(0, ol_cnt - 1)] = params["W_ID"]

    return new_order_txn_operator, front_end_key, "new_order", (params,)


def get_payment_transaction(front_end_key):
    """Return parameters for PAYMENT"""
    x = rand.number(1, 100)
    y = rand.number(1, 100)

    w_id = rand.number(1, N_W)
    d_id = rand.number(1, D_Per_Warehouse)

    h_amount = rand.fixed_point(2, MIN_PAYMENT, MAX_PAYMENT)
    h_date = datetime.now()  # Keep as datetime for flexibility

    # 85% local payment
    if N_W == 1 or x <= 85:
        c_w_id = w_id
        c_d_id = d_id
    else:
        c_w_id = rand.number_excluding(1, N_W, w_id)
        assert c_w_id != w_id
        c_d_id = rand.number(1, D_Per_Warehouse)

    if y <= 60:
        # Payment by last name using TPC-C last name generation
        c_id = None
        c_last = rand.make_last_name(rand.nu_rand(255, 0, 999))
    else:
        c_id = make_customer_id()
        c_last = None

    params = {
        "W_ID": w_id,
        "D_ID": d_id,
        "H_AMOUNT": h_amount,
        "C_W_ID": c_w_id,
        "C_D_ID": c_d_id,
        "C_ID": c_id,
        "C_LAST": c_last,
        "H_DATE": h_date
    }
    return payment_txn_operator, front_end_key, "payment", (params,)


def tpc_c_workload_generator(proc_num):
    c = 0
    while True:
        front_end_key = f"{proc_num}:{c}"
        coin = rand.number(1, 100)
        if coin < 52:
            yield get_new_order_transaction(front_end_key)
        else:
            yield get_payment_transaction(front_end_key)
        c += 1


def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f'Generator: {proc_num} starting with: EC={os.environ["ENABLE_COMPRESSION"]} '
          f'CK={os.environ["USE_COMPOSITE_KEYS"]} FC={os.environ["USE_FALLBACK_CACHE"]}')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    tpc_c_generator = tpc_c_workload_generator(proc_num)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    start = timer()
    for second in range(seconds):
        if proc_num == 0 and 0 <= kill_at == second:
            subprocess.run(["docker", "kill", "styx-worker-1"], check=False)
            print("KILL -> styx-worker-1 done")
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(tpc_c_generator)
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
    tpc_c_init(styx_client)
    del styx_client
    print("Data populated waiting for 1 minute")
    # 1 min so that the init is surely done
    time.sleep(60)

    with Pool(threads) as p:
        results = p.map(benchmark_runner, range(threads))

    results = {k: v for d in results for k, v in d.items()}
    pd.DataFrame({"request_id": list(results.keys()),
                  "timestamp": [res["timestamp"] for res in results.values()],
                  "op": [res["op"] for res in results.values()]
                  }).sort_values(by="timestamp").to_csv(f"{SAVE_DIR}/client_requests.csv",
                                                        index=False)


if __name__ == "__main__":
    multiprocessing.set_start_method("fork")
    main()

    print()
    kafka_output_consumer.main(SAVE_DIR)

    print()
    calculate_metrics.main(
        SAVE_DIR,
        messages_per_second,
        warmup_seconds,
        threads,
        N_W,
        enable_compression,
        use_composite_keys,
        use_fallback_cache
    )
