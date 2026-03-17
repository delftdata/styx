"""Profile TPC-C workload through InMemoryOperatorState.

Simulates realistic TPC-C new_order and payment epochs using the actual
worker state classes, then profiles with cProfile.

TPC-C access patterns (from demo/demo-tpc-c/functions/):

  new_order_txn: touches 12 operators with deep chains
    - new_order_txn:  1 get + 5 puts  (coordinator state, 4 callback updates)
    - warehouse:      1 get
    - district:       1 get + 1 put   (increment D_NEXT_O_ID)
    - customer:       1 get
    - item x N:       N gets          (read-only, N=5..15, avg 10)
    - stock x N:      N gets + N puts (update quantity)
    - order:          1 put           (insert)
    - order_line x N: N puts          (insert)
    - new_order:      1 put           (insert)
    Total per txn: ~(N+4) gets + ~(N+9) puts, avg N=10 → 14 gets + 19 puts

  payment_txn: touches 6 operators
    - payment_txn:    1 get + 4 puts  (coordinator state, 3 callback updates)
    - warehouse:      1 get + 1 put   (update W_YTD)
    - district:       1 get + 1 put   (update D_YTD)
    - customer:       1 get + 1 put   (update balance)
    - history:        1 put           (insert)
    Total per txn: 4 gets + 8 puts

Standard TPC-C mix: 45% new_order, 43% payment, 12% other (simplified to 50/50 here).

Usage:
    PYTHONPATH=. python scripts/profile_tpcc.py
    PYTHONPATH=. python scripts/profile_tpcc.py --epoch-size 500
    PYTHONPATH=. python scripts/profile_tpcc.py --cprofile
"""

import argparse
import cProfile
import pstats
import random
import time

from styx.common.partitioning.hash_partitioner import HashPartitioner
from styx.common.run_func_payload import RunFuncPayload
from styx.common.serialization import msgpack_serialization
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.sequencer.sequencer import Sequencer

# ---------------------------------------------------------------------------
# TPC-C data generators
# ---------------------------------------------------------------------------

N_WAREHOUSES = 4
N_DISTRICTS_PER_WH = 10
N_CUSTOMERS_PER_DIST = 3000
N_ITEMS = 100_000
N_ITEMS_PER_ORDER_AVG = 10
N_ITEMS_PER_ORDER_MIN = 5
N_ITEMS_PER_ORDER_MAX = 15


def make_warehouse_data() -> dict:
    return {"W_TAX": 0.08, "W_YTD": 300_000.0, "W_NAME": "warehouse"}


def make_district_data(d_id: int) -> dict:
    return {"D_TAX": 0.05, "D_YTD": 30_000.0, "D_NEXT_O_ID": 3001, "D_ID": d_id}


def make_customer_data() -> dict:
    return {
        "C_BALANCE": -10.0,
        "C_YTD_PAYMENT": 10.0,
        "C_PAYMENT_CNT": 1,
        "C_CREDIT": "GC",
        "C_DISCOUNT": 0.05,
        "C_FIRST": "John",
        "C_LAST": "Doe",
        "C_DATA": "x" * 200,
    }


def make_stock_data() -> dict:
    return {
        "S_QUANTITY": random.randint(10, 100),
        "S_YTD": 0,
        "S_ORDER_CNT": 0,
        "S_REMOTE_CNT": 0,
        "S_DIST_01": "d" * 24,
    }


def make_item_data() -> dict:
    return {"I_NAME": "item_name", "I_PRICE": 25.0, "I_DATA": "original_data"}


# ---------------------------------------------------------------------------
# TPC-C operators
# ---------------------------------------------------------------------------

OPERATORS = [
    "new_order_txn",
    "payment_txn",
    "warehouse",
    "district",
    "customer",
    "stock",
    "item",
    "order",
    "order_line",
    "new_order",
    "history",
]

N_PARTITIONS_MAP = {
    "new_order_txn": 4,
    "payment_txn": 4,
    "warehouse": 4,
    "district": 4,
    "customer": 4,
    "stock": 4,
    "item": 4,
    "order": 4,
    "order_line": 4,
    "new_order": 4,
    "history": 4,
}


def build_state(n_partitions: int) -> tuple[InMemoryOperatorState, dict[str, HashPartitioner]]:
    """Build state and partitioners for all TPC-C operators."""
    op_partitions = set()
    for op in OPERATORS:
        for p in range(n_partitions):
            op_partitions.add((op, p))

    state = InMemoryOperatorState(op_partitions)
    partitioners = {op: HashPartitioner(n_partitions, None) for op in OPERATORS}

    # Populate warehouse
    for w_id in range(N_WAREHOUSES):
        p = partitioners["warehouse"].get_partition_no_cache(w_id)
        state.data[("warehouse", p)][w_id] = make_warehouse_data()

    # Populate districts
    for w_id in range(N_WAREHOUSES):
        for d_id in range(1, N_DISTRICTS_PER_WH + 1):
            key = f"{w_id}:{d_id}"
            p = partitioners["district"].get_partition_no_cache(key)
            state.data[("district", p)][key] = make_district_data(d_id)

    # Populate customers (small sample)
    for w_id in range(N_WAREHOUSES):
        for d_id in range(1, N_DISTRICTS_PER_WH + 1):
            for c_id in range(1, 101):  # 100 customers per district (sampled)
                key = f"{w_id}:{d_id}:{c_id}"
                p = partitioners["customer"].get_partition_no_cache(key)
                state.data[("customer", p)][key] = make_customer_data()

    # Populate items
    for i_id in range(1, 1001):  # 1000 items (sampled from 100k)
        p = partitioners["item"].get_partition_no_cache(i_id)
        state.data[("item", p)][i_id] = make_item_data()

    # Populate stock
    for w_id in range(N_WAREHOUSES):
        for i_id in range(1, 1001):
            key = f"{w_id}:{i_id}"
            p = partitioners["stock"].get_partition_no_cache(key)
            state.data[("stock", p)][key] = make_stock_data()

    return state, partitioners


# ---------------------------------------------------------------------------
# Transaction simulators
# ---------------------------------------------------------------------------


def sim_new_order(state, partitioners, t_id, w_id) -> None:
    """Simulate a TPC-C new_order transaction through InMemoryOperatorState.

    Follows the actual chain from demo/demo-tpc-c/functions/:
    new_order_txn → warehouse, district, customer → item(xN) → stock(xN) → order_line(xN), order, new_order
    """
    d_id = random.randint(1, N_DISTRICTS_PER_WH)
    c_id = random.randint(1, 100)
    n_items = random.randint(N_ITEMS_PER_ORDER_MIN, N_ITEMS_PER_ORDER_MAX)

    # new_order_txn.new_order: 1 put (initial state)
    txn_key = f"txn:{t_id}"
    txn_part = partitioners["new_order_txn"].get_partition_no_cache(txn_key)
    state.put(txn_key, {"status": "init", "n_items": n_items}, t_id, "new_order_txn", txn_part)

    # warehouse.get_warehouse: 1 get
    wh_part = partitioners["warehouse"].get_partition_no_cache(w_id)
    wh_data = state.get(w_id, t_id, "warehouse", wh_part)

    # new_order_txn.get_warehouse callback: 1 get + 1 put
    state.get(txn_key, t_id, "new_order_txn", txn_part)
    state.put(txn_key, {"status": "wh_done", "wh": wh_data}, t_id, "new_order_txn", txn_part)

    # district.get_district: 1 get + 1 put (increment D_NEXT_O_ID)
    dist_key = f"{w_id}:{d_id}"
    dist_part = partitioners["district"].get_partition_no_cache(dist_key)
    dist_data = state.get(dist_key, t_id, "district", dist_part)
    if dist_data:
        dist_data = dict(dist_data)  # copy before mutation
        o_id = dist_data.get("D_NEXT_O_ID", 3001)
        dist_data["D_NEXT_O_ID"] = o_id + 1
        state.put(dist_key, dist_data, t_id, "district", dist_part)

    # new_order_txn.get_district callback: 1 get + 1 put
    state.get(txn_key, t_id, "new_order_txn", txn_part)
    state.put(txn_key, {"status": "dist_done"}, t_id, "new_order_txn", txn_part)

    # customer.get_customer: 1 get
    cust_key = f"{w_id}:{d_id}:{c_id}"
    cust_part = partitioners["customer"].get_partition_no_cache(cust_key)
    cust_data = state.get(cust_key, t_id, "customer", cust_part)

    # new_order_txn.get_customer callback: 1 get + 1 put
    state.get(txn_key, t_id, "new_order_txn", txn_part)
    state.put(txn_key, {"status": "cust_done"}, t_id, "new_order_txn", txn_part)

    # For each item in order:
    for ol_num in range(n_items):
        i_id = random.randint(1, 1000)

        # item.get_item: 1 get (read-only)
        item_part = partitioners["item"].get_partition_no_cache(i_id)
        item_data = state.get(i_id, t_id, "item", item_part)

        # stock.update_stock: 1 get + 1 put
        stock_key = f"{w_id}:{i_id}"
        stock_part = partitioners["stock"].get_partition_no_cache(stock_key)
        stock_data = state.get(stock_key, t_id, "stock", stock_part)
        if stock_data:
            stock_data = dict(stock_data)
            stock_data["S_QUANTITY"] = max(10, stock_data.get("S_QUANTITY", 50) - random.randint(1, 10))
            stock_data["S_ORDER_CNT"] = stock_data.get("S_ORDER_CNT", 0) + 1
            state.put(stock_key, stock_data, t_id, "stock", stock_part)

        # order_line insert: 1 put
        ol_key = f"{w_id}:{d_id}:{t_id}:{ol_num}"
        ol_part = partitioners["order_line"].get_partition_no_cache(ol_key)
        state.put(ol_key, {"OL_I_ID": i_id, "OL_QTY": 5, "OL_AMOUNT": 25.0}, t_id, "order_line", ol_part)

        # new_order_txn.get_item_with_stock callback: 1 get + 1 put
        state.get(txn_key, t_id, "new_order_txn", txn_part)
        state.put(txn_key, {"status": f"item_{ol_num}_done"}, t_id, "new_order_txn", txn_part)

    # order insert: 1 put
    order_key = f"{w_id}:{d_id}:{t_id}"
    order_part = partitioners["order"].get_partition_no_cache(order_key)
    state.put(order_key, {"O_C_ID": c_id, "O_OL_CNT": n_items}, t_id, "order", order_part)

    # new_order insert: 1 put
    no_key = f"{w_id}:{d_id}:{t_id}"
    no_part = partitioners["new_order"].get_partition_no_cache(no_key)
    state.put(no_key, {"NO_W_ID": w_id, "NO_D_ID": d_id}, t_id, "new_order", no_part)


def sim_payment(state, partitioners, t_id, w_id) -> None:
    """Simulate a TPC-C payment transaction.

    Chain: payment_txn → warehouse, district, customer → history
    """
    d_id = random.randint(1, N_DISTRICTS_PER_WH)
    c_id = random.randint(1, 100)
    h_amount = random.uniform(1.0, 5000.0)

    # payment_txn.payment: 1 put (init)
    txn_key = f"pay:{t_id}"
    txn_part = partitioners["payment_txn"].get_partition_no_cache(txn_key)
    state.put(txn_key, {"status": "init", "h_amount": h_amount}, t_id, "payment_txn", txn_part)

    # warehouse.pay: 1 get + 1 put (update W_YTD)
    wh_part = partitioners["warehouse"].get_partition_no_cache(w_id)
    wh_data = state.get(w_id, t_id, "warehouse", wh_part)
    if wh_data:
        wh_data = dict(wh_data)
        wh_data["W_YTD"] = wh_data.get("W_YTD", 0) + h_amount
        state.put(w_id, wh_data, t_id, "warehouse", wh_part)

    # payment_txn.get_warehouse callback: 1 get + 1 put
    state.get(txn_key, t_id, "payment_txn", txn_part)
    state.put(txn_key, {"status": "wh_done"}, t_id, "payment_txn", txn_part)

    # district.pay: 1 get + 1 put (update D_YTD)
    dist_key = f"{w_id}:{d_id}"
    dist_part = partitioners["district"].get_partition_no_cache(dist_key)
    dist_data = state.get(dist_key, t_id, "district", dist_part)
    if dist_data:
        dist_data = dict(dist_data)
        dist_data["D_YTD"] = dist_data.get("D_YTD", 0) + h_amount
        state.put(dist_key, dist_data, t_id, "district", dist_part)

    # payment_txn.get_district callback: 1 get + 1 put
    state.get(txn_key, t_id, "payment_txn", txn_part)
    state.put(txn_key, {"status": "dist_done"}, t_id, "payment_txn", txn_part)

    # customer.pay: 1 get + 1 put
    cust_key = f"{w_id}:{d_id}:{c_id}"
    cust_part = partitioners["customer"].get_partition_no_cache(cust_key)
    cust_data = state.get(cust_key, t_id, "customer", cust_part)
    if cust_data:
        cust_data = dict(cust_data)
        cust_data["C_BALANCE"] = cust_data.get("C_BALANCE", 0) - h_amount
        cust_data["C_YTD_PAYMENT"] = cust_data.get("C_YTD_PAYMENT", 0) + h_amount
        cust_data["C_PAYMENT_CNT"] = cust_data.get("C_PAYMENT_CNT", 0) + 1
        state.put(cust_key, cust_data, t_id, "customer", cust_part)

    # payment_txn.get_customer callback: 1 get + 1 put
    state.get(txn_key, t_id, "payment_txn", txn_part)
    state.put(txn_key, {"status": "cust_done"}, t_id, "payment_txn", txn_part)

    # history.insert: 1 put
    hist_key = f"{w_id}:{d_id}:{c_id}:{t_id}"
    hist_part = partitioners["history"].get_partition_no_cache(hist_key)
    state.put(
        hist_key,
        {"H_AMOUNT": h_amount, "H_DATE": "2025-01-01"},
        t_id,
        "history",
        hist_part,
    )


# ---------------------------------------------------------------------------
# Full epoch simulation
# ---------------------------------------------------------------------------


def tpcc_epoch(epoch_size: int, n_partitions: int) -> dict:
    """Simulate one Aria epoch of mixed TPC-C transactions."""
    state, partitioners = build_state(n_partitions)
    seq = Sequencer(max_size=epoch_size)
    seq.sequencer_id = 1
    seq.n_workers = 4

    # Generate transaction mix: 50% new_order, 50% payment
    txn_types = []
    for i in range(epoch_size):
        w_id = random.randint(0, N_WAREHOUSES - 1)
        txn_type = "new_order" if random.random() < 0.5 else "payment"
        txn_types.append((txn_type, w_id))

        payload = RunFuncPayload(
            request_id=i.to_bytes(8, "big"),
            key=f"txn:{i}",
            operator_name="new_order_txn" if txn_type == "new_order" else "payment_txn",
            partition=0,
            function_name=txn_type,
            params=(),
        )
        seq.sequence(payload)

    seq.get_epoch()

    # PROCESSING
    total_gets = 0
    total_puts = 0
    for i, (txn_type, w_id) in enumerate(txn_types):
        t_id = i + 1
        if txn_type == "new_order":
            sim_new_order(state, partitioners, t_id, w_id)
        else:
            sim_payment(state, partitioners, t_id, w_id)

    # CONFLICT DETECTION
    aborted = state.check_conflicts()

    # REMOVE ABORTED
    if aborted:
        state.remove_aborted_from_rw_sets(aborted)

    # COMMIT
    committed = state.commit(aborted)

    # EGRESS
    for i, (txn_type, w_id) in enumerate(txn_types):
        t_id = i + 1
        if t_id not in aborted:
            msgpack_serialization((txn_type, t_id, "ok"))

    # CLEANUP
    state.cleanup()
    seq.increment_epoch(max_t_counter=epoch_size + 1, t_ids_to_reschedule=aborted)

    return {
        "epoch_size": epoch_size,
        "committed": len(committed),
        "aborted": len(aborted),
        "new_orders": sum(1 for t, _ in txn_types if t == "new_order"),
        "payments": sum(1 for t, _ in txn_types if t == "payment"),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile TPC-C workload")
    parser.add_argument("--epoch-size", type=int, default=500)
    parser.add_argument("--n-partitions", type=int, default=4)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--cprofile", action="store_true", help="Run cProfile")
    args = parser.parse_args()

    print("=" * 70)
    print("TPC-C MIXED WORKLOAD PROFILER")
    print("=" * 70)
    print(f"  epoch_size={args.epoch_size}, n_partitions={args.n_partitions}")
    print(f"  mix: 50% new_order (~14 gets + ~19 puts), 50% payment (4 gets + 8 puts)")
    print()

    if args.cprofile:
        print("--- cProfile ---\n")
        profiler = cProfile.Profile()
        profiler.enable()
        for _ in range(args.iterations):
            tpcc_epoch(args.epoch_size, args.n_partitions)
        profiler.disable()

        stats = pstats.Stats(profiler)
        stats.strip_dirs()

        print("--- sorted by tottime ---\n")
        stats.sort_stats("tottime")
        stats.print_stats(25)

        print("--- sorted by cumtime ---\n")
        stats.sort_stats("cumulative")
        stats.print_stats(25)
        return

    # Timing + phase breakdown
    times = []
    result = None
    for i in range(args.iterations):
        start = time.perf_counter_ns()
        result = tpcc_epoch(args.epoch_size, args.n_partitions)
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
        times.append(elapsed_ms)
        if i == 0:
            print(
                f"  First epoch: new_orders={result['new_orders']}, payments={result['payments']}, "
                f"committed={result['committed']}, aborted={result['aborted']}"
            )

    times.sort()
    median = times[len(times) // 2]
    p95 = times[int(len(times) * 0.95)]

    print()
    print(f"  Median epoch time:   {median:.1f} ms")
    print(f"  P95 epoch time:      {p95:.1f} ms")
    print(f"  Throughput (median): {args.epoch_size / (median / 1000):.0f} txn/s")
    print()

    # Per-phase breakdown
    print("--- Per-phase breakdown (median of %d) ---\n" % args.iterations)

    phase_times = {
        "build state": [],
        "ingress+sequencing": [],
        "processing (get/put)": [],
        "conflict detection": [],
        "remove_aborted": [],
        "commit": [],
        "egress (serialize)": [],
        "cleanup+advance": [],
    }

    for _ in range(args.iterations):
        t0 = time.perf_counter_ns()
        state, partitioners = build_state(args.n_partitions)
        seq = Sequencer(max_size=args.epoch_size)
        seq.sequencer_id = 1
        seq.n_workers = 4
        t1 = time.perf_counter_ns()
        phase_times["build state"].append(t1 - t0)

        txn_types = []
        for i in range(args.epoch_size):
            w_id = random.randint(0, N_WAREHOUSES - 1)
            txn_type = "new_order" if random.random() < 0.5 else "payment"
            txn_types.append((txn_type, w_id))
            payload = RunFuncPayload(
                request_id=i.to_bytes(8, "big"),
                key=f"txn:{i}",
                operator_name="new_order_txn" if txn_type == "new_order" else "payment_txn",
                partition=0,
                function_name=txn_type,
                params=(),
            )
            seq.sequence(payload)
        seq.get_epoch()
        t2 = time.perf_counter_ns()
        phase_times["ingress+sequencing"].append(t2 - t1)

        for i, (txn_type, w_id) in enumerate(txn_types):
            t_id = i + 1
            if txn_type == "new_order":
                sim_new_order(state, partitioners, t_id, w_id)
            else:
                sim_payment(state, partitioners, t_id, w_id)
        t3 = time.perf_counter_ns()
        phase_times["processing (get/put)"].append(t3 - t2)

        aborted = state.check_conflicts()
        t4 = time.perf_counter_ns()
        phase_times["conflict detection"].append(t4 - t3)

        if aborted:
            state.remove_aborted_from_rw_sets(aborted)
        t5 = time.perf_counter_ns()
        phase_times["remove_aborted"].append(t5 - t4)

        state.commit(aborted)
        t6 = time.perf_counter_ns()
        phase_times["commit"].append(t6 - t5)

        for i, (txn_type, w_id) in enumerate(txn_types):
            t_id = i + 1
            if t_id not in aborted:
                msgpack_serialization((txn_type, t_id, "ok"))
        t7 = time.perf_counter_ns()
        phase_times["egress (serialize)"].append(t7 - t6)

        state.cleanup()
        seq.increment_epoch(max_t_counter=args.epoch_size + 1, t_ids_to_reschedule=aborted)
        t8 = time.perf_counter_ns()
        phase_times["cleanup+advance"].append(t8 - t7)

    total_median = 0
    for phase, t_list in phase_times.items():
        t_list.sort()
        med = t_list[len(t_list) // 2] / 1_000_000
        total_median += med
        bar = "#" * int(med / median * 40)
        print(f"  {phase:.<35s} {med:>7.1f} ms  ({med / median * 100:>4.1f}%)  {bar}")
    print(f"  {'TOTAL':.<35s} {total_median:>7.1f} ms")


if __name__ == "__main__":
    main()
