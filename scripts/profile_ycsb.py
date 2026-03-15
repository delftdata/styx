"""Profile YCSB workload through InMemoryOperatorState.

Simulates realistic YCSB transfer epochs using the actual worker
state classes, then profiles with cProfile to show where time goes.

Usage:
    PYTHONPATH=. python scripts/profile_ycsb.py
    PYTHONPATH=. python scripts/profile_ycsb.py --epoch-size 2000 --n-keys 10000
    PYTHONPATH=. python scripts/profile_ycsb.py --cprofile
"""

import argparse
import cProfile
import pstats
import random
import time

from msgspec import msgpack

from styx.common.partitioning.hash_partitioner import HashPartitioner
from styx.common.run_func_payload import RunFuncPayload
from styx.common.serialization import msgpack_serialization
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.sequencer.sequencer import Sequencer

# ---------------------------------------------------------------------------
# YCSB epoch simulation
# ---------------------------------------------------------------------------

STARTING_BALANCE = 1_000_000


def ycsb_epoch(
    epoch_size: int,
    n_keys: int,
    n_partitions: int,
) -> dict:
    """Simulate one Aria epoch of YCSB transfer transactions.

    Each transfer:
      - get(key_a)          → 1 read
      - put(key_a, val-1)   → 1 write
      - get(key_b)          → 1 read  (chained update_t)
      - put(key_b, val+1)   → 1 write (chained update_t)

    So each transaction touches 2 keys with 2 gets + 2 puts.
    """
    op_partitions = {("ycsb", p) for p in range(n_partitions)}
    state = InMemoryOperatorState(op_partitions)
    partitioner = HashPartitioner(n_partitions, None)
    seq = Sequencer(max_size=epoch_size)
    seq.sequencer_id = 1
    seq.n_workers = 4

    # Pre-populate state
    for key in range(n_keys):
        partition = partitioner.get_partition_no_cache(key)
        state.data[("ycsb", partition)][key] = STARTING_BALANCE

    # --- INGRESS + SEQUENCING ---
    transactions = []
    for i in range(epoch_size):
        key_a = random.randint(0, n_keys - 1)
        key_b = random.randint(0, n_keys - 1)
        while key_b == key_a:
            key_b = random.randint(0, n_keys - 1)
        part_a = partitioner.get_partition_no_cache(key_a)
        part_b = partitioner.get_partition_no_cache(key_b)

        payload = RunFuncPayload(
            request_id=i.to_bytes(8, "big"),
            key=key_a,
            operator_name="ycsb",
            partition=part_a,
            function_name="transfer",
            params=(key_b,),
        )
        seq.sequence(payload)
        transactions.append((key_a, part_a, key_b, part_b))

    epoch = seq.get_epoch()

    # --- PROCESSING: simulate transfer + update_t ---
    for idx, (key_a, part_a, key_b, part_b) in enumerate(transactions):
        t_id = idx + 1

        # transfer: get(key_a)
        val_a = state.get(key_a, t_id, "ycsb", part_a)

        # transfer: put(key_a, val-1)
        state.put(key_a, val_a - 1, t_id, "ycsb", part_a)

        # update_t (chained): get(key_b)
        val_b = state.get(key_b, t_id, "ycsb", part_b)

        # update_t (chained): put(key_b, val+1)
        state.put(key_b, val_b + 1, t_id, "ycsb", part_b)

    # --- CONFLICT DETECTION ---
    aborted = state.check_conflicts()

    # --- REMOVE ABORTED FROM RW SETS ---
    if aborted:
        state.remove_aborted_from_rw_sets(aborted)

    # --- COMMIT ---
    committed = state.commit(aborted)

    # --- EGRESS: serialize responses ---
    for idx, (key_a, part_a, key_b, part_b) in enumerate(transactions):
        t_id = idx + 1
        if t_id not in aborted:
            msgpack_serialization((key_a, val_a - 1))

    # --- CLEANUP ---
    state.cleanup()

    # --- SEQUENCER EPOCH ADVANCE ---
    seq.increment_epoch(
        max_t_counter=epoch_size + 1,
        t_ids_to_reschedule=aborted,
    )

    return {
        "epoch_size": epoch_size,
        "committed": len(committed),
        "aborted": len(aborted),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile YCSB workload")
    parser.add_argument("--epoch-size", type=int, default=1000)
    parser.add_argument("--n-keys", type=int, default=10_000)
    parser.add_argument("--n-partitions", type=int, default=4)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--cprofile", action="store_true", help="Run cProfile")
    args = parser.parse_args()

    print("=" * 70)
    print("YCSB TRANSFER WORKLOAD PROFILER")
    print("=" * 70)
    print(f"  epoch_size={args.epoch_size}, n_keys={args.n_keys}")
    print(f"  n_partitions={args.n_partitions}, iterations={args.iterations}")
    print(f"  Access pattern: transfer → 2 gets + 2 puts per txn")
    print()

    if args.cprofile:
        print("--- cProfile ---\n")
        profiler = cProfile.Profile()
        profiler.enable()
        for _ in range(args.iterations):
            ycsb_epoch(args.epoch_size, args.n_keys, args.n_partitions)
        profiler.disable()

        stats = pstats.Stats(profiler)
        stats.strip_dirs()

        print("--- sorted by tottime (where CPU actually spends time) ---\n")
        stats.sort_stats("tottime")
        stats.print_stats(25)

        print("--- sorted by cumtime (including callees) ---\n")
        stats.sort_stats("cumulative")
        stats.print_stats(25)
        return

    # Timing runs
    times = []
    result = None
    for i in range(args.iterations):
        start = time.perf_counter_ns()
        result = ycsb_epoch(args.epoch_size, args.n_keys, args.n_partitions)
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
        times.append(elapsed_ms)
        if i == 0:
            print(f"  First epoch: committed={result['committed']}, aborted={result['aborted']}")

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
        "ingress+sequencing": [],
        "processing (get/put)": [],
        "conflict detection": [],
        "remove_aborted": [],
        "commit": [],
        "egress (serialize)": [],
        "cleanup+advance": [],
    }

    for _ in range(args.iterations):
        op_partitions = {("ycsb", p) for p in range(args.n_partitions)}
        state = InMemoryOperatorState(op_partitions)
        partitioner = HashPartitioner(args.n_partitions, None)
        seq = Sequencer(max_size=args.epoch_size)
        seq.sequencer_id = 1
        seq.n_workers = 4

        for key in range(args.n_keys):
            partition = partitioner.get_partition_no_cache(key)
            state.data[("ycsb", partition)][key] = STARTING_BALANCE

        # INGRESS
        t0 = time.perf_counter_ns()
        transactions = []
        for i in range(args.epoch_size):
            key_a = random.randint(0, args.n_keys - 1)
            key_b = random.randint(0, args.n_keys - 1)
            while key_b == key_a:
                key_b = random.randint(0, args.n_keys - 1)
            part_a = partitioner.get_partition_no_cache(key_a)
            part_b = partitioner.get_partition_no_cache(key_b)
            payload = RunFuncPayload(
                request_id=i.to_bytes(8, "big"),
                key=key_a,
                operator_name="ycsb",
                partition=part_a,
                function_name="transfer",
                params=(key_b,),
            )
            seq.sequence(payload)
            transactions.append((key_a, part_a, key_b, part_b))
        epoch = seq.get_epoch()
        t1 = time.perf_counter_ns()
        phase_times["ingress+sequencing"].append(t1 - t0)

        # PROCESSING
        for idx, (key_a, part_a, key_b, part_b) in enumerate(transactions):
            t_id = idx + 1
            val_a = state.get(key_a, t_id, "ycsb", part_a)
            state.put(key_a, val_a - 1, t_id, "ycsb", part_a)
            val_b = state.get(key_b, t_id, "ycsb", part_b)
            state.put(key_b, val_b + 1, t_id, "ycsb", part_b)
        t2 = time.perf_counter_ns()
        phase_times["processing (get/put)"].append(t2 - t1)

        # CONFLICT DETECTION
        aborted = state.check_conflicts()
        t3 = time.perf_counter_ns()
        phase_times["conflict detection"].append(t3 - t2)

        # REMOVE ABORTED
        if aborted:
            state.remove_aborted_from_rw_sets(aborted)
        t4 = time.perf_counter_ns()
        phase_times["remove_aborted"].append(t4 - t3)

        # COMMIT
        state.commit(aborted)
        t5 = time.perf_counter_ns()
        phase_times["commit"].append(t5 - t4)

        # EGRESS
        for idx, (key_a, part_a, key_b, part_b) in enumerate(transactions):
            t_id = idx + 1
            if t_id not in aborted:
                msgpack_serialization((key_a, STARTING_BALANCE))
        t6 = time.perf_counter_ns()
        phase_times["egress (serialize)"].append(t6 - t5)

        # CLEANUP
        state.cleanup()
        seq.increment_epoch(max_t_counter=args.epoch_size + 1, t_ids_to_reschedule=aborted)
        t7 = time.perf_counter_ns()
        phase_times["cleanup+advance"].append(t7 - t6)

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
