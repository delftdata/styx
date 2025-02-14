import csv
import multiprocessing
import os
import sys
import random
from datetime import datetime

from styx.common.stateful_function import make_key_hashable
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
N_PARTITIONS = int(sys.argv[3])
messages_per_second = int(sys.argv[4])
sleeps_per_second = 100
sleep_time = 0.0085
seconds = int(sys.argv[5])
STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9092'
warmup_seconds = int(sys.argv[6])
N_W = int(sys.argv[7])
C_Per_District = 3000
D_Per_Warehouse = 10
N_D = N_W * D_Per_Warehouse
N_C = N_W * 30_000
N_I = 100_000
N_S = N_W * 100_000
N_H = N_W * 30_000
N_O = N_W * 30_000
N_NO = N_W * 9_000
N_OL = N_W * 300_000

MIN_OL_CNT: int = 5
MAX_OL_CNT: int = 15
MAX_OL_QUANTITY: int = 10
MIN_PAYMENT = 1.0
MAX_PAYMENT = 5000.0

customers_per_district: dict[tuple, list] = {}

script_path = os.path.dirname(os.path.realpath(__file__))


flush_interval = 100

g = StateflowGraph('tpcc_benchmark', operator_state_backend=LocalStateBackend.DICT)
####################################################################################################################
customer_operator.set_n_partitions(N_PARTITIONS)
district_operator.set_n_partitions(min(N_D, N_PARTITIONS))
history_operator.set_n_partitions(N_PARTITIONS)
item_operator.set_n_partitions(N_PARTITIONS)
new_order_operator.set_n_partitions(N_PARTITIONS)
order_operator.set_n_partitions(N_PARTITIONS)
order_line_operator.set_n_partitions(N_PARTITIONS)
stock_operator.set_n_partitions(N_PARTITIONS)
warehouse_operator.set_n_partitions(min(N_W, N_PARTITIONS))
new_order_txn_operator.set_n_partitions(N_PARTITIONS)
customer_idx_operator.set_n_partitions(N_PARTITIONS)
payment_txn_operator.set_n_partitions(N_PARTITIONS)
g.add_operators(customer_operator, district_operator, history_operator, item_operator, new_order_operator,
                order_operator, order_line_operator, stock_operator, warehouse_operator,
                new_order_txn_operator, customer_idx_operator, payment_txn_operator)



def populate_warehouse(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/warehouse.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        for _, line in tqdm(enumerate(reader), desc="Populating Warehouse"):
            warehouse_key = int(line[0])
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
            styx.send_event(operator=warehouse_operator,
                            key=warehouse_key,
                            function='insert',
                            params=(warehouse_data,))


def populate_district(styx: SyncStyxClient):
    with open(os.path.join(script_path, "data/district.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        for _, line in tqdm(enumerate(reader), desc="Populating District"):
            # Primary Key: (D_W_ID, D_ID)
            district_key = f'{line[1]}:{line[0]}'
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
            styx.send_event(operator=district_operator,
                            key=district_key,
                            function='insert',
                            params=(district_data,))


def populate_customer(styx: SyncStyxClient):
    c = 0
    customer_index_data = {}
    with open(os.path.join(script_path, "data/customer.csv"), "rb") as f:
        num_lines = sum(1 for _ in f)
    with open(os.path.join(script_path, "data/customer.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Customer"):
            # Primary Key: (C_W_ID, C_D_ID, C_ID)
            customer_key = f'{line[2]}:{line[1]}:{line[0]}'
            partition: int = make_key_hashable(customer_key) % customer_operator.n_partitions
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

            partitions[partition] |= {customer_key: customer_data}

            if i % 1_000 == 0 or i == num_lines - 1:
                for partition, kv_pairs in partitions.items():
                    styx.send_batch_insert(operator=customer_operator,
                                           partition=partition,
                                           function='insert_batch',
                                           key_value_pairs=kv_pairs)
                partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}

            # create index for 2.5.2.2  Case 2
            customer_idx_key = f'{line[2]}:{line[1]}:{line[5]}'
            customer_idx_data = {
                "C_FIRST": line[3],
                "C_ID": int(line[0]),
                "C_D_ID": int(line[1]),
                "C_W_ID": int(line[2])
            }
            if customer_idx_key in customer_idx_data:
                customer_idx_data[customer_idx_key].append(customer_idx_data)
            else:
                customer_idx_data[customer_idx_key] = [customer_idx_data]

        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for i, (customer_idx_key, customer_idx_data) in enumerate(customer_index_data.items()):
            partition = make_key_hashable(customer_idx_key) % customer_idx_operator.n_partitions
            partitions[partition] |= {customer_idx_key: customer_idx_data}
            if i % 1_000 == 0 or i == num_lines - 1:
                for partition, kv_pairs in partitions.items():
                    styx.send_batch_insert(operator=customer_idx_operator,
                                           partition=partition,
                                           function='insert_batch',
                                           key_value_pairs=kv_pairs)
                partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
                c += 1
                if c % flush_interval == 0:
                    styx.flush()


def populate_history(styx: SyncStyxClient):
    c = 0
    with open(os.path.join(script_path, "data/history.csv"), "rb") as f:
        num_lines = sum(1 for _ in f)
    with open(os.path.join(script_path, "data/history.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating History"):
            # Primary Key: (H_W_ID, H_D_ID, H_C_ID)
            history_key = f'{line[4]}:{line[3]}:{line[0]}'
            partition = make_key_hashable(history_key) % history_operator.n_partitions
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
            partitions[partition] |= {history_key: history_data}
            if i % 1_000 == 0 or i == num_lines - 1:
                for partition, kv_pairs in partitions.items():
                    styx.send_batch_insert(operator=history_operator,
                                           partition=partition,
                                           function='insert_batch',
                                           key_value_pairs=kv_pairs)
                partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
                c += 1
                if c % flush_interval == 0:
                    styx.flush()


def populate_new_order(styx: SyncStyxClient):
    c = 0
    with open(os.path.join(script_path, "data/new_order.csv"), "rb") as f:
        num_lines = sum(1 for _ in f)
    with open(os.path.join(script_path, "data/new_order.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating New Order"):
            # Primary Key: (NO_W_ID, NO_D_ID, NO_O_ID)
            new_order_key = f'{line[2]}:{line[1]}:{line[0]}'
            partition = make_key_hashable(new_order_key) % new_order_operator.n_partitions
            new_order_data = {
                "NO_O_ID": int(line[0]),
                "NO_D_ID": int(line[1]),
                "NO_W_ID": int(line[2])
            }
            partitions[partition] |= {new_order_key: new_order_data}
            if i % 1_000 == 0 or i == num_lines - 1:
                for partition, kv_pairs in partitions.items():
                    styx.send_batch_insert(operator=new_order_operator,
                                           partition=partition,
                                           function='insert_batch',
                                           key_value_pairs=kv_pairs)
                partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
                c += 1
                if c % flush_interval == 0:
                    styx.flush()


def populate_order(styx: SyncStyxClient):
    c = 0
    with open(os.path.join(script_path, "data/order.csv"), "rb") as f:
        num_lines = sum(1 for _ in f)
    with open(os.path.join(script_path, "data/order.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Order"):
            # Primary Key: (O_W_ID, O_D_ID, O_ID)
            order_key = f'{line[2]}:{line[1]}:{line[0]}'
            partition = make_key_hashable(order_key) % order_operator.n_partitions
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
            partitions[partition] |= {order_key: order_data}
            if i % 1_000 == 0 or i == num_lines - 1:
                for partition, kv_pairs in partitions.items():
                    styx.send_batch_insert(operator=order_operator,
                                           partition=partition,
                                           function='insert_batch',
                                           key_value_pairs=kv_pairs)
                partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
                c += 1
                if c % flush_interval == 0:
                    styx.flush()


def populate_order_line(styx: SyncStyxClient):
    c = 0
    with open(os.path.join(script_path, "data/order_line.csv"), "rb") as f:
        num_lines = sum(1 for _ in f)
    with open(os.path.join(script_path, "data/order_line.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Order Line"):
            # Primary Key: (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
            order_line_key = f'{line[2]}:{line[1]}:{line[0]}:{line[3]}'
            partition = make_key_hashable(order_line_key) % order_line_operator.n_partitions
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
            partitions[partition] |= {order_line_key: order_line_data}
            if i % 1_000 == 0 or i == num_lines - 1:
                for partition, kv_pairs in partitions.items():
                    styx.send_batch_insert(operator=order_line_operator,
                                           partition=partition,
                                           function='insert_batch',
                                           key_value_pairs=kv_pairs)
                partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
                c += 1
                if c % flush_interval == 0:
                    styx.flush()


def populate_item(styx: SyncStyxClient):
    c = 0
    with open(os.path.join(script_path, "data/item.csv"), "rb") as f:
        num_lines = sum(1 for _ in f)
    with open(os.path.join(script_path, "data/item.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Item"):
            item_key = int(line[0])
            partition = make_key_hashable(item_key) % item_operator.n_partitions
            item_data = {
                "I_IM_ID": int(line[1]),
                "I_NAME": line[2],
                "I_PRICE": float(line[3]),
                "I_DATA": line[4]
            }
            partitions[partition] |= {item_key: item_data}
            if i % 1_000 == 0 or i == num_lines - 1:
                for partition, kv_pairs in partitions.items():
                    styx.send_batch_insert(operator=item_operator,
                                           partition=partition,
                                           function='insert_batch',
                                           key_value_pairs=kv_pairs)
                partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
                c += 1
                if c % flush_interval == 0:
                    styx.flush()


def populate_stock(styx: SyncStyxClient):
    c = 0
    with open(os.path.join(script_path, "data/stock.csv"), "rb") as f:
        num_lines = sum(1 for _ in f)
    with open(os.path.join(script_path, "data/stock.csv"), "r") as f:
        reader = csv.reader(f, delimiter=",")
        partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
        for i, line in tqdm(enumerate(reader), desc="Populating Stock"):
            # Primary Key: (S_W_ID, S_I_ID)
            stock_key = f'{line[1]}:{line[0]}'
            partition = make_key_hashable(stock_key) % stock_operator.n_partitions
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
            partitions[partition] |= {stock_key: stock_data}
            if i % 1_000 == 0 or i == num_lines - 1:
                for partition, kv_pairs in partitions.items():
                    styx.send_batch_insert(operator=stock_operator,
                                           partition=partition,
                                           function='insert_batch',
                                           key_value_pairs=kv_pairs)
                partitions: dict[int, dict] = {p: {} for p in range(N_PARTITIONS)}
                c += 1
                if c % flush_interval == 0:
                    styx.flush()


def submit_graph(styx: SyncStyxClient):
    print(list(g.nodes.values())[0].n_partitions)
    styx.submit_dataflow(g)
    print("Graph submitted")


def tpc_c_init(styx: SyncStyxClient):
    submit_graph(styx)
    time.sleep(60)
    populate_warehouse(styx)
    styx.flush()
    populate_district(styx)
    styx.flush()
    populate_customer(styx)
    styx.flush()
    populate_history(styx)
    styx.flush()
    populate_new_order(styx)
    styx.flush()
    populate_order(styx)
    styx.flush()
    populate_order_line(styx)
    styx.flush()
    populate_item(styx)
    styx.flush()
    populate_stock(styx)
    styx.flush()
    print('Data populated waiting for 1 minute')
    # 1 min so that the init is surely done
    time.sleep(60)


def make_item_id() -> int:
    return rand.nu_rand(8191, 1, N_I)


def make_customer_id():
    return rand.nu_rand(1023, 1, C_Per_District)


def get_new_order_transaction(c):
    """Return parameters for NEW_ORDER"""
    params = {
        'W_ID': random.randint(1, N_W),
        'D_ID': random.randint(1, D_Per_Warehouse),
        'C_ID': make_customer_id(),
        'O_ENTRY_D': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
    }
    # 1% of transactions roll back
    rollback = random.randint(1, 100) == 1

    params['I_IDS'] = []
    params['I_W_IDS'] = []
    params['I_QTYS'] = []
    ol_cnt = random.randint(MIN_OL_CNT, MAX_OL_CNT)

    for i in range(ol_cnt):
        if rollback and i + 1 == ol_cnt:
            params['I_IDS'].append(N_I + 1)
        else:
            i_id = make_item_id()
            while i_id in params['I_IDS']:
                i_id = make_item_id()
            params['I_IDS'].append(i_id)

        # 1% of items are from a remote warehouse
        remote = (rand.number(1, 100) == 1)
        if N_W > 1 and remote:
            params['I_W_IDS'].append(
                rand.number_excluding(
                    1,
                    N_W,
                    params['W_ID']
                )
            )
        else:
            params['I_W_IDS'].append(params['W_ID'])
        params['I_QTYS'].append(rand.number(1, MAX_OL_QUANTITY))
    return new_order_txn_operator, c, 'new_order', (params,)


def get_payment_transaction(c):
    """Return parameters for PAYMENT"""
    x = rand.number(1, 100)
    y = rand.number(1, 100)

    w_id = rand.number(1, N_W)
    d_id = rand.number(1, D_Per_Warehouse)

    h_amount = rand.fixed_point(2, MIN_PAYMENT, MAX_PAYMENT)
    h_date = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

    # 85%: paying through own warehouse (or there is only 1 warehouse)
    if N_W == 1 or x <= 85:
        c_w_id = w_id
        c_d_id = d_id
    # 15%: paying through another warehouse:
    else:
        # select in range [1, num_warehouses] excluding w_id
        c_w_id = rand.number_excluding(1, N_W, w_id)
        assert c_w_id != w_id
        c_d_id = rand.number(1, D_Per_Warehouse)

    # 60%: payment by last name
    if y <= 60:
        c_id = None
        c_last = random.choice(customers_per_district[(c_w_id, c_d_id)])
    # 40%: payment by id
    else:
        assert y > 60
        c_id = make_customer_id()
        c_last = None
    params = {
        'W_ID': w_id,
        'D_ID': d_id,
        'H_AMOUNT': h_amount,
        "C_W_ID": c_w_id,
        "C_D_ID": c_d_id,
        "C_ID": c_id,
        "C_LAST": c_last,
        "H_DATE": h_date
    }
    return payment_txn_operator, c, 'payment', (params,)


def tpc_c_workload_generator():
    c = 0
    while True:
        coin = rand.number(1, 100)
        if coin < 52:
            yield get_new_order_transaction(c)
        else:
            yield get_payment_transaction(c)
        c += 1


def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f'Generator: {proc_num} starting')
    styx = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)
    styx.open(consume=False)
    tpc_c_generator = tpc_c_workload_generator()
    timestamp_futures: dict[bytes, dict] = {}
    start = timer()
    for _ in range(seconds):
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
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')
    styx.close()
    for key, metadata in styx.delivery_timestamps.items():
        timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def main():
    styx_client = SyncStyxClient(STYX_HOST, STYX_PORT, kafka_url=KAFKA_URL)

    styx_client.open(consume=False)
    tpc_c_init(styx_client)
    styx_client.close()

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
