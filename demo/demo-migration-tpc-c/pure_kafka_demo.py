import csv
import multiprocessing
import os
import sys
import random
from collections import defaultdict
from datetime import datetime
from typing import Any

from minio import Minio
from tqdm import tqdm

from timeit import default_timer as timer
import time
from multiprocessing import Pool

import pandas as pd

from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph
from styx.client import SyncStyxClient

from graph import (customer_operator, district_operator, history_operator, item_operator, new_order_operator,
                   order_operator, order_line_operator, stock_operator, warehouse_operator,
                   payment_txn_operator, customer_idx_operator, new_order_txn_operator)

import rand

import kafka_output_consumer
import calculate_metrics

SAVE_DIR: str = sys.argv[1]
threads = int(sys.argv[2])
START_N_PARTITIONS = int(sys.argv[3])
END_N_PARTITIONS = int(sys.argv[4])
messages_per_second = int(sys.argv[5])
SECOND_TO_TAKE_MIGRATION = 60
sleeps_per_second = 100
sleep_time = 0.0085
seconds = int(sys.argv[6])
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9092'
warmup_seconds = int(sys.argv[7])
N_W = int(sys.argv[8])
START_N_PARTITIONS = min(N_W, START_N_PARTITIONS)
END_N_PARTITIONS = min(N_W, END_N_PARTITIONS)
C_Per_District = 3000
D_Per_Warehouse = 10
N_D = N_W * D_Per_Warehouse
N_I = 100_000

MIN_OL_CNT: int = 5
MAX_OL_CNT: int = 15
MAX_OL_QUANTITY: int = 10
MIN_PAYMENT = 1.0
MAX_PAYMENT = 5000.0

customers_per_district: dict[tuple, list] = {}

script_path = os.path.dirname(os.path.realpath(__file__))


flush_interval = 1000

g = StateflowGraph('tpcc_benchmark', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
customer_operator.set_n_partitions(START_N_PARTITIONS)
district_operator.set_n_partitions(START_N_PARTITIONS)
history_operator.set_n_partitions(START_N_PARTITIONS)
item_operator.set_n_partitions(START_N_PARTITIONS)
new_order_operator.set_n_partitions(START_N_PARTITIONS)
order_operator.set_n_partitions(START_N_PARTITIONS)
order_line_operator.set_n_partitions(START_N_PARTITIONS)
stock_operator.set_n_partitions(START_N_PARTITIONS)
warehouse_operator.set_n_partitions(START_N_PARTITIONS)
new_order_txn_operator.set_n_partitions(START_N_PARTITIONS)
customer_idx_operator.set_n_partitions(START_N_PARTITIONS)
payment_txn_operator.set_n_partitions(START_N_PARTITIONS)
g.add_operators(customer_operator, district_operator, history_operator, item_operator, new_order_operator,
                order_operator, order_line_operator, stock_operator, warehouse_operator,
                new_order_txn_operator, customer_idx_operator, payment_txn_operator)



def populate_warehouse(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/warehouse.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
        for _, line in tqdm(enumerate(reader), desc="Populating Warehouse"):
            warehouse_key = int(line[0])
            partition: int = styx.get_operator_partition(warehouse_key, warehouse_operator)
            warehouse_data = {
                "W_NAME": line[1],
                "W_STREET_1": line[2],
                "W_STREET_2": line[3],
                "W_CITY": line[4],
                "W_STATE": line[5],
                "W_ZIP": line[6],
                "W_TAX": float(line[7]),
                "W_YTD": float(line[8])
            }
            partitions[partition][warehouse_key] = warehouse_data
    for partition, partition_data in partitions.items():
        print(f"Populating {warehouse_operator.name}:{partition}...")
        styx.init_data(warehouse_operator, partition, partition_data)


def populate_district(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/district.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
        for _, line in tqdm(enumerate(reader), desc="Populating District"):
            # Primary Key: (D_W_ID, D_ID)
            district_key = f'{line[1]}:{line[0]}'
            partition: int = styx.get_operator_partition_composite_key(district_key, district_operator, 0, ":")
            district_data = {
                "D_ID": int(line[0]),
                "D_W_ID": int(line[1]),
                "D_NAME": line[2],
                "D_STREET_1": line[3],
                "D_STREET_2": line[4],
                "D_CITY": line[5],
                "D_STATE": line[6],
                "D_ZIP": line[7],
                "D_TAX": float(line[8]),
                "D_YTD": float(line[9]),
                "D_NEXT_O_ID": int(line[10])
            }
            partitions[partition][district_key] = district_data
    for partition, partition_data in partitions.items():
        print(f"Populating {district_operator.name}:{partition}...")
        styx.init_data(district_operator, partition, partition_data)


def populate_customer(styx: SyncStyxClient):
    customer_index_data: dict[str, list[dict[str, str | int]]] = defaultdict(list)
    with open(os.path.join(script_path, "data/customer.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Customer"):
            # Primary Key: (C_W_ID, C_D_ID, C_ID)
            customer_key = f'{line[2]}:{line[1]}:{line[0]}'
            partition: int = styx.get_operator_partition_composite_key(customer_key, customer_operator, 0, ":")
            customer_data = {
                "C_ID": int(line[0]),
                "C_D_ID": int(line[1]),
                "C_W_ID": int(line[2]),
                "C_FIRST": line[3],
                "C_MIDDLE": line[4],
                "C_LAST": line[5],
                "C_STREET_1": line[6],
                "C_STREET_2": line[7],
                "C_CITY": line[8],
                "C_STATE": line[9],
                "C_ZIP": line[10],
                "C_PHONE": line[11],
                "C_SINCE": line[12],
                "C_CREDIT": line[13],
                "C_CREDIT_LIM": float(line[14]),
                "C_DISCOUNT": float(line[15]),
                "C_BALANCE": float(line[16]),
                "C_YTD_PAYMENT": float(line[17]),
                "C_PAYMENT_CNT": int(line[18]),
                "C_DELIVERY_CNT": int(line[19]),
                "C_DATA": line[20]
            }

            customers_per_district_key = (customer_data["C_W_ID"], customer_data["C_D_ID"])
            if customers_per_district_key in customers_per_district:
                customers_per_district[customers_per_district_key].append(customer_data["C_LAST"])
            else:
                customers_per_district[customers_per_district_key] = [customer_data["C_LAST"]]

            partitions[partition][customer_key] = customer_data

            # create index for 2.5.2.2  Case 2  C_W_ID:C_D_ID:C_LAST
            customer_idx_key = f'{line[2]}:{line[1]}:{line[5]}'
            customer_idx_value = {
                "C_FIRST": line[3],
                "C_ID": int(line[0]),
                "C_D_ID": int(line[1]),
                "C_W_ID": int(line[2])
            }
            customer_index_data[customer_idx_key].append(customer_idx_value)

        for partition, partition_data in partitions.items():
            print(f"Populating {customer_operator.name}:{partition}...")
            styx.init_data(customer_operator, partition, partition_data)

        # Final batch insert for customer index
        index_partitions: dict[int, dict[str, list[str]]] = {p: {} for p in range(START_N_PARTITIONS)}
        index_keys = list(customer_index_data.items())

        for i, (customer_idx_key, customer_idx_values) in enumerate(index_keys):
            partition: int = styx.get_operator_partition_composite_key(customer_idx_key, customer_idx_operator, 0, ":")
            sorted_customers = sorted(customer_idx_values, key=lambda x: x["C_FIRST"])
            customer_ids = [f"{cust['C_W_ID']}:{cust['C_D_ID']}:{cust['C_ID']}" for cust in sorted_customers]
            index_partitions[partition][customer_idx_key] = customer_ids

        for partition, partition_data in index_partitions.items():
            print(f"Populating {customer_idx_operator.name}:{partition}...")
            styx.init_data(customer_idx_operator, partition, partition_data)


def populate_history(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/history.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating History"):
            # Primary Key: (H_W_ID, H_D_ID, H_C_ID)
            history_key = f'{line[4]}:{line[3]}:{line[0]}'
            partition: int = styx.get_operator_partition_composite_key(history_key, history_operator, 0, ":")
            history_data = {
                "H_C_ID": int(line[0]),
                "H_C_D_ID": int(line[1]),
                "H_C_W_ID": int(line[2]),
                "H_D_ID": int(line[3]),
                "H_W_ID": int(line[4]),
                "H_DATE": line[5],
                "H_AMOUN": line[6],
                "H_DATA": line[7],
            }
            partitions[partition][history_key] = history_data
    for partition, partition_data in partitions.items():
        print(f"Populating {history_operator.name}:{partition}...")
        styx.init_data(history_operator, partition, partition_data)


def populate_new_order(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/new_order.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating New Order"):
            # Primary Key: (NO_W_ID, NO_D_ID, NO_O_ID)
            new_order_key = f'{line[2]}:{line[1]}:{line[0]}'
            partition: int = styx.get_operator_partition_composite_key(new_order_key, new_order_operator, 0, ":")
            new_order_data = {
                "NO_O_ID": int(line[0]),
                "NO_D_ID": int(line[1]),
                "NO_W_ID": int(line[2])
            }
            partitions[partition][new_order_key]= new_order_data
    for partition, partition_data in partitions.items():
        print(f"Populating {new_order_operator.name}:{partition}...")
        styx.init_data(new_order_operator, partition, partition_data)


def populate_order(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/order.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Order"):
            # Primary Key: (O_W_ID, O_D_ID, O_ID)
            order_key = f'{line[2]}:{line[1]}:{line[0]}'
            partition: int = styx.get_operator_partition_composite_key(order_key, order_operator, 0, ":")
            order_data = {
                "O_ID": int(line[0]),
                "O_D_ID": int(line[1]),
                "O_W_ID": int(line[2]),
                "O_C_ID": int(line[3]),
                "O_ENTRY_D": line[4],
                "O_CARRIER_ID": line[5],
                "O_OL_CNT": int(line[6]),
                "O_ALL_LOCAL": bool(line[7])
            }
            partitions[partition][order_key] = order_data
    for partition, partition_data in partitions.items():
        print(f"Populating {order_operator.name}:{partition}...")
        styx.init_data(order_operator, partition, partition_data)


def populate_order_line(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/order_line.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Order Line"):
            # Primary Key: (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
            order_line_key = f'{line[2]}:{line[1]}:{line[0]}:{line[3]}'
            partition: int = styx.get_operator_partition_composite_key(order_line_key, order_line_operator, 0, ":")
            order_line_data = {
                "OL_O_ID": int(line[0]),
                "OL_D_ID": int(line[1]),
                "OL_W_ID": int(line[2]),
                "OL_NUMBER": int(line[3]),
                "OL_I_ID": int(line[4]),
                "OL_SUPPLY_W_ID": int(line[5]),
                "OL_DELIVERY_D": line[6],
                "OL_QUANTITY": int(line[7]),
                "OL_AMOUNT": float(line[8]),
                "OL_DIST_INFO": line[9]
            }
            partitions[partition][order_line_key] = order_line_data
    for partition, partition_data in partitions.items():
        print(f"Populating {order_line_operator.name}:{partition}...")
        styx.init_data(order_line_operator, partition, partition_data)

def populate_item(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/item.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Item"):
            item_key = int(line[0])
            partition: int = styx.get_operator_partition(item_key, item_operator)
            item_data = {
                "I_IM_ID": int(line[1]),
                "I_NAME": line[2],
                "I_PRICE": float(line[3]),
                "I_DATA": line[4]
            }
            partitions[partition][item_key] = item_data
    for partition, partition_data in partitions.items():
        print(f"Populating {item_operator.name}:{partition}...")
        styx.init_data(item_operator, partition, partition_data)


def populate_stock(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/stock.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(START_N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Stock"):
            # Primary Key: (S_W_ID, S_I_ID)
            stock_key = f'{line[1]}:{line[0]}'
            partition: int = styx.get_operator_partition_composite_key(stock_key, stock_operator, 0, ":")
            stock_data = {
                "S_I_ID": int(line[0]),
                "S_W_ID": int(line[1]),
                "S_QUANTITY": int(line[2]),
                "S_DIST_01": line[3],
                "S_DIST_02": line[4],
                "S_DIST_03": line[5],
                "S_DIST_04": line[6],
                "S_DIST_05": line[7],
                "S_DIST_06": line[8],
                "S_DIST_07": line[9],
                "S_DIST_08": line[10],
                "S_DIST_09": line[11],
                "S_DIST_10": line[12],
                "S_YTD": int(line[13]),
                "S_ORDER_CNT": int(line[14]),
                "S_REMOTE_CNT": int(line[15]),
                "S_DATA": line[16]
            }
            partitions[partition][stock_key] = stock_data
    for partition, partition_data in partitions.items():
        print(f"Populating {stock_operator.name}:{partition}...")
        styx.init_data(stock_operator, partition, partition_data)


def submit_graph(styx: SyncStyxClient):
    print(list(g.nodes.values())[0].n_partitions)
    styx.submit_dataflow(g)
    print("Graph submitted")


def tpc_c_init(styx: SyncStyxClient):
    styx.set_graph(g)
    styx.init_metadata(g)
    populate_warehouse(styx)
    populate_district(styx)
    populate_customer(styx)
    populate_history(styx)
    populate_new_order(styx)
    populate_order(styx)
    populate_order_line(styx)
    populate_item(styx)
    populate_stock(styx)
    # Sleep for 5 seconds to be sure that all the data are in minio
    time.sleep(5)
    submit_graph(styx)


def make_item_id() -> int:
    return rand.nu_rand(8191, 1, N_I)


def make_customer_id():
    return rand.nu_rand(1023, 1, C_Per_District)


def get_new_order_transaction(front_end_key):
    """Return parameters for NEW_ORDER"""
    params: dict[str, Any] = {
        'W_ID': random.randint(1, N_W),
        'D_ID': random.randint(1, D_Per_Warehouse),
        'C_ID': make_customer_id(),
        'O_ENTRY_D': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
    }

    rollback = random.randint(1, 100) == 1  # 1% rollback chance

    params['I_IDS'] = []
    params['I_W_IDS'] = []
    params['I_QTYS'] = []

    ol_cnt = random.randint(MIN_OL_CNT, MAX_OL_CNT)

    for i in range(ol_cnt):
        if rollback and i + 1 == ol_cnt:
            params['I_IDS'].append(N_I + 1)  # Invalid ID triggers rollback
        else:
            i_id = make_item_id()
            while i_id in params['I_IDS']:
                i_id = make_item_id()
            params['I_IDS'].append(i_id)

        # Decide if this item comes from a remote warehouse
        remote = random.randint(1, 100) == 1  # 1% chance
        if N_W > 1 and remote:
            params['I_W_IDS'].append(
                rand.number_excluding(1, N_W, params['W_ID'])
            )
        else:
            params['I_W_IDS'].append(params['W_ID'])

        params['I_QTYS'].append(rand.number(1, MAX_OL_QUANTITY))

    # Ensure at least one item is from the local warehouse
    if not any(w_id == params['W_ID'] for w_id in params['I_W_IDS']):
        params['I_W_IDS'][random.randint(0, ol_cnt - 1)] = params['W_ID']

    return new_order_txn_operator, front_end_key, 'new_order', (params,)


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
        'W_ID': w_id,
        'D_ID': d_id,
        'H_AMOUNT': h_amount,
        'C_W_ID': c_w_id,
        'C_D_ID': c_d_id,
        'C_ID': c_id,
        'C_LAST': c_last,
        'H_DATE': h_date
    }
    return payment_txn_operator, front_end_key, 'payment', (params,)


def tpc_c_workload_generator(proc_num):
    c = 0
    while True:
        front_end_key = f'{proc_num}:{c}'
        coin = rand.number(1, 100)
        if coin < 52:
            yield get_new_order_transaction(front_end_key)
        else:
            yield get_payment_transaction(front_end_key)
        c += 1


def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    tpc_c_generator = tpc_c_workload_generator(proc_num)
    timestamp_futures: dict[bytes, dict] = {}
    time.sleep(5)
    start = timer()
    for cur_sec in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            operator, key, func_name, params = next(tpc_c_generator)
            future = styx.send_event(operator=operator,
                                     key=key,
                                     function=func_name,
                                     params=params)
            timestamp_futures[future.request_id] = {"op": f'{func_name} {key}->{params}'}
        styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
        if cur_sec == SECOND_TO_TAKE_MIGRATION:
            new_g = StateflowGraph('tpcc_benchmark', operator_state_backend=LocalStateBackend.DICT)
            ####################################################################################################################
            customer_operator.set_n_partitions(END_N_PARTITIONS)
            district_operator.set_n_partitions(END_N_PARTITIONS)
            history_operator.set_n_partitions(END_N_PARTITIONS)
            item_operator.set_n_partitions(END_N_PARTITIONS)
            new_order_operator.set_n_partitions(END_N_PARTITIONS)
            order_operator.set_n_partitions(END_N_PARTITIONS)
            order_line_operator.set_n_partitions(END_N_PARTITIONS)
            stock_operator.set_n_partitions(END_N_PARTITIONS)
            warehouse_operator.set_n_partitions(END_N_PARTITIONS)
            new_order_txn_operator.set_n_partitions(END_N_PARTITIONS)
            customer_idx_operator.set_n_partitions(END_N_PARTITIONS)
            payment_txn_operator.set_n_partitions(END_N_PARTITIONS)
            new_g.add_operators(customer_operator, district_operator, history_operator, item_operator, new_order_operator,
                            order_operator, order_line_operator, stock_operator, warehouse_operator,
                            new_order_txn_operator, customer_idx_operator, payment_txn_operator)
            styx.submit_dataflow(new_g)
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')
    styx.close()
    for key, metadata in styx.delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def main():
    minio = Minio('localhost:9000', access_key='minio', secret_key='minio123', secure=False)
    styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL, minio=minio)
    tpc_c_init(styx_client)
    del styx_client
    print('Data populated waiting for 1 minute')
    # 1 min so that the init is surely done
    time.sleep(60)

    with Pool(threads) as p:
        results = p.map(benchmark_runner, range(threads))

    results = {k: v for d in results for k, v in d.items()}
    pd.DataFrame({"request_id": list(results.keys()),
                  "timestamp": [res["timestamp"] for res in results.values()],
                  "op": [res["op"] for res in results.values()]
                  }).to_csv(f'{SAVE_DIR}/client_requests.csv',
                            index=False)


if __name__ == "__main__":
    multiprocessing.set_start_method('fork')
    main()

    print()
    kafka_output_consumer.main(SAVE_DIR)

    print()
    calculate_metrics.main(
        SAVE_DIR,
        messages_per_second,
        warmup_seconds,
        threads,
        N_W
    )
