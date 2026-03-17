"""Profiling harness for Styx hot paths.

Microbenchmarks each performance-critical component in isolation,
then simulates a full Aria epoch to show relative costs.

Usage:
    python scripts/profile_hotpaths.py
    python scripts/profile_hotpaths.py --epoch-size 2000 --keys-per-txn 10
    python scripts/profile_hotpaths.py --profile-epoch   # cProfile on simulated epoch
"""

import argparse
import copy
import cProfile
import pstats
import random
import struct
import time
from collections import defaultdict

import cityhash
import msgspec
import zstandard as zstd

from styx.common.partitioning.hash_partitioner import HashPartitioner
from styx.common.serialization import (
    msgpack_deserialization,
    msgpack_serialization,
    zstd_msgpack_deserialization,
    zstd_msgpack_serialization,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_EPOCH_SIZE = 1000
DEFAULT_KEYS_PER_TXN = 5
DEFAULT_N_PARTITIONS = 4
DEFAULT_N_OPERATORS = 2
DEFAULT_VALUE_SIZE = 100  # bytes (simulated state value)
DEFAULT_ITERATIONS = 10  # repeat each microbenchmark


def make_state_value(size: int) -> dict:
    """Simulate a realistic state value (dict with string/int fields)."""
    return {"balance": random.randint(0, 1_000_000), "name": "user_" + "x" * size, "active": True}


def make_state_value_simple(size: int) -> int:
    """Simple integer value for minimal-overhead baseline."""
    return random.randint(0, 1_000_000)


# ---------------------------------------------------------------------------
# Benchmark utilities
# ---------------------------------------------------------------------------


def bench(label: str, func, iterations: int = DEFAULT_ITERATIONS, ops_per_iter: int = 1) -> float:
    """Run func() `iterations` times, print stats, return median ns/op."""
    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        func()
        elapsed = time.perf_counter_ns() - start
        times.append(elapsed)

    times.sort()
    median_ns = times[len(times) // 2]
    total_ops = ops_per_iter
    ns_per_op = median_ns / total_ops

    if ns_per_op > 1_000_000:
        unit, divisor = "ms", 1_000_000
    elif ns_per_op > 1_000:
        unit, divisor = "us", 1_000
    else:
        unit, divisor = "ns", 1

    print(f"  {label:.<55s} {ns_per_op / divisor:>10.1f} {unit}/op  (median of {iterations})")
    return ns_per_op


# ---------------------------------------------------------------------------
# 1. State get() — msgpack deep copy vs alternatives
# ---------------------------------------------------------------------------


def bench_state_get(n_ops: int, value_size: int, iterations: int) -> None:
    print("\n=== 1. STATE GET — deep copy via msgpack encode/decode ===")
    print(f"    ({n_ops} ops per iteration, value_size={value_size})\n")

    value = make_state_value(value_size)

    # What InMemoryOperatorState.get() actually does
    def msgpack_roundtrip():
        for _ in range(n_ops):
            msgspec.msgpack.decode(msgspec.msgpack.encode(value))

    # Alternative: copy.deepcopy
    def deepcopy_roundtrip():
        for _ in range(n_ops):
            copy.deepcopy(value)

    # Alternative: copy.copy (shallow)
    def shallow_copy():
        for _ in range(n_ops):
            copy.copy(value)

    # Alternative: dict unpacking (only works for flat dicts)
    def dict_copy():
        for _ in range(n_ops):
            dict(value)

    # Baseline: just the encode
    def msgpack_encode_only():
        for _ in range(n_ops):
            msgspec.msgpack.encode(value)

    # Baseline: just the decode
    encoded = msgspec.msgpack.encode(value)

    def msgpack_decode_only():
        for _ in range(n_ops):
            msgspec.msgpack.decode(encoded)

    # Also test with a simple integer value (common in YCSB)
    int_value = 1_000_000

    def msgpack_roundtrip_int():
        for _ in range(n_ops):
            msgspec.msgpack.decode(msgspec.msgpack.encode(int_value))

    bench("msgpack encode+decode (dict value)", msgpack_roundtrip, iterations, n_ops)
    bench("msgpack encode only (dict value)", msgpack_encode_only, iterations, n_ops)
    bench("msgpack decode only (dict value)", msgpack_decode_only, iterations, n_ops)
    bench("msgpack encode+decode (int value)", msgpack_roundtrip_int, iterations, n_ops)
    bench("copy.deepcopy (dict value)", deepcopy_roundtrip, iterations, n_ops)
    bench("copy.copy / shallow (dict value)", shallow_copy, iterations, n_ops)
    bench("dict() copy (dict value)", dict_copy, iterations, n_ops)


# ---------------------------------------------------------------------------
# 2. Serialization — msgpack and ZSTD
# ---------------------------------------------------------------------------


def bench_serialization(n_ops: int, iterations: int) -> None:
    print("\n=== 2. SERIALIZATION — msgpack + ZSTD ===")
    print(f"    ({n_ops} ops per iteration)\n")

    # Typical Aria protocol message
    msg_small = (42, b"\x01\x02\x03\x04", "ycsb", "transfer", 7, 3, False, ("key_b",))
    msg_medium = {
        ("op", i): {j: random.randint(0, 100) for j in range(50)} for i in range(4)
    }  # ~read/write sets
    msg_large_bytes = msgpack_serialization(
        {i: make_state_value(200) for i in range(100)},
    )  # snapshot-like

    def ser_small():
        for _ in range(n_ops):
            msgpack_serialization(msg_small)

    def deser_small():
        encoded = msgpack_serialization(msg_small)
        for _ in range(n_ops):
            msgpack_deserialization(encoded)

    def ser_medium():
        for _ in range(n_ops):
            msgpack_serialization(msg_medium)

    def zstd_compress():
        for _ in range(n_ops):
            zstd_msgpack_serialization(msg_large_bytes, already_ser=True)

    def zstd_decompress():
        compressed = zstd_msgpack_serialization(msg_large_bytes, already_ser=True)
        for _ in range(n_ops):
            zstd_msgpack_deserialization(compressed)

    # struct packing (TCP framing)
    def struct_pack():
        for _ in range(n_ops):
            struct.pack(">B", 1) + struct.pack(">B", 1)

    def struct_pack_header():
        payload = b"\x00" * 200
        for _ in range(n_ops):
            struct.pack(">Q", len(payload)) + payload

    bench("msgpack encode (small tuple)", ser_small, iterations, n_ops)
    bench("msgpack decode (small tuple)", deser_small, iterations, n_ops)
    bench("msgpack encode (medium dict — rw sets)", ser_medium, iterations, n_ops)
    bench("zstd compress (pre-serialized ~30KB)", zstd_compress, iterations, n_ops)
    bench("zstd decompress (~30KB)", zstd_decompress, iterations, n_ops)
    bench("struct.pack 2x >B (msg type + ser id)", struct_pack, iterations, n_ops)
    bench("struct.pack >Q + concat (TCP header)", struct_pack_header, iterations, n_ops)


# ---------------------------------------------------------------------------
# 3. Hashing / Partitioning
# ---------------------------------------------------------------------------


def bench_hashing(n_ops: int, iterations: int) -> None:
    print("\n=== 3. HASHING / PARTITIONING ===")
    print(f"    ({n_ops} ops per iteration)\n")

    int_keys = list(range(n_ops))
    str_keys = [f"user_{i}" for i in range(n_ops)]
    composite_keys = [f"field1|field2|{i}" for i in range(n_ops)]

    partitioner = HashPartitioner(4, None)
    partitioner_composite = HashPartitioner(4, (2, "|"))

    def hash_int_keys():
        for k in int_keys:
            k % 4  # what happens for int keys (bypass hash)

    def hash_str_keys():
        for k in str_keys:
            cityhash.CityHash64(k) % 4

    def partitioner_int_no_cache():
        for k in int_keys:
            partitioner.get_partition_no_cache(k)

    def partitioner_str_no_cache():
        for k in str_keys:
            partitioner.get_partition_no_cache(k)

    def partitioner_composite_no_cache():
        for k in composite_keys:
            partitioner_composite.get_partition_no_cache(k)

    # LRU cache hit rate simulation
    HashPartitioner._get_partition_cached.cache_clear()

    def partitioner_cached_warmup():
        for k in int_keys:
            partitioner.get_partition(k)

    def partitioner_cached_hits():
        for k in int_keys:
            partitioner.get_partition(k)

    bench("int key mod (baseline)", hash_int_keys, iterations, n_ops)
    bench("CityHash64 (str key)", hash_str_keys, iterations, n_ops)
    bench("partitioner no_cache (int keys)", partitioner_int_no_cache, iterations, n_ops)
    bench("partitioner no_cache (str keys)", partitioner_str_no_cache, iterations, n_ops)
    bench("partitioner no_cache (composite keys)", partitioner_composite_no_cache, iterations, n_ops)
    bench("partitioner cached (cold — cache misses)", partitioner_cached_warmup, iterations, n_ops)
    bench("partitioner cached (warm — cache hits)", partitioner_cached_hits, iterations, n_ops)


# ---------------------------------------------------------------------------
# 4. Conflict Detection
# ---------------------------------------------------------------------------


def bench_conflict_detection(epoch_size: int, keys_per_txn: int, n_partitions: int, iterations: int) -> None:
    print("\n=== 4. CONFLICT DETECTION ===")
    print(f"    (epoch_size={epoch_size}, keys_per_txn={keys_per_txn})\n")

    # Import the actual class
    from worker.operator_state.aria.in_memory_state import InMemoryOperatorState

    key_space = 10_000
    op_partitions = {("op", p) for p in range(n_partitions)}

    def build_and_check():
        state = InMemoryOperatorState(op_partitions)
        # Simulate epoch: each txn reads and writes keys_per_txn keys
        for t_id in range(1, epoch_size + 1):
            partition = t_id % n_partitions
            op_part = ("op", partition)
            for _ in range(keys_per_txn):
                key = random.randint(0, key_space - 1)
                # Simulate get (read tracking)
                state.deal_with_reads(key, t_id, op_part)
                # Simulate put (write tracking)
                state.put(key, t_id * 100, t_id, "op", partition)
        # Run conflict detection
        state.check_conflicts()

    # Separate: just the check_conflicts part with pre-built sets
    state_prebuilt = InMemoryOperatorState(op_partitions)
    for t_id in range(1, epoch_size + 1):
        partition = t_id % n_partitions
        for _ in range(keys_per_txn):
            key = random.randint(0, key_space - 1)
            state_prebuilt.deal_with_reads(key, t_id, ("op", partition))
            state_prebuilt.put(key, t_id * 100, t_id, "op", partition)

    def check_only():
        state_prebuilt.check_conflicts()

    # Also benchmark deal_with_reads and put in isolation
    state_rw = InMemoryOperatorState(op_partitions)
    op_part = ("op", 0)

    def deal_with_reads_bench():
        for t_id in range(1, epoch_size + 1):
            for k in range(keys_per_txn):
                state_rw.deal_with_reads(k, t_id, op_part)

    state_put = InMemoryOperatorState(op_partitions)

    def put_bench():
        for t_id in range(1, epoch_size + 1):
            for k in range(keys_per_txn):
                state_put.put(k, t_id, t_id, "op", 0)

    # Benchmark commit
    state_commit = InMemoryOperatorState(op_partitions)
    for t_id in range(1, epoch_size + 1):
        partition = t_id % n_partitions
        for _ in range(keys_per_txn):
            key = random.randint(0, key_space - 1)
            state_commit.put(key, t_id * 100, t_id, "op", partition)

    def commit_bench():
        state_commit.commit(set())

    # Benchmark cleanup
    state_cleanup = InMemoryOperatorState(op_partitions)
    for t_id in range(1, epoch_size + 1):
        partition = t_id % n_partitions
        for _ in range(keys_per_txn):
            key = random.randint(0, key_space - 1)
            state_cleanup.deal_with_reads(key, t_id, ("op", partition))
            state_cleanup.put(key, t_id * 100, t_id, "op", partition)

    def cleanup_bench():
        state_cleanup.cleanup()

    n_total = epoch_size * keys_per_txn
    bench("deal_with_reads (read tracking)", deal_with_reads_bench, iterations, n_total)
    bench("put (write set tracking)", put_bench, iterations, n_total)
    bench(f"check_conflicts (pre-built, {epoch_size} txns)", check_only, iterations, 1)
    bench(f"build rw-sets + check_conflicts ({epoch_size} txns)", build_and_check, iterations, 1)
    bench(f"commit ({epoch_size} txns)", commit_bench, iterations, 1)
    bench(f"cleanup ({epoch_size} txns)", cleanup_bench, iterations, 1)


# ---------------------------------------------------------------------------
# 5. Sequencer
# ---------------------------------------------------------------------------


def bench_sequencer(epoch_size: int, iterations: int) -> None:
    print("\n=== 5. SEQUENCER ===")
    print(f"    (epoch_size={epoch_size})\n")

    from styx.common.run_func_payload import RunFuncPayload

    from worker.sequencer.sequencer import Sequencer

    def sequence_epoch():
        seq = Sequencer(max_size=epoch_size)
        seq.sequencer_id = 1
        seq.n_workers = 4
        for i in range(epoch_size):
            payload = RunFuncPayload(
                request_id=i.to_bytes(8, "big"),
                key=i,
                operator_name="op",
                partition=i % 4,
                function_name="func",
                params=(),
            )
            seq.sequence(payload)
        seq.get_epoch()

    def increment_with_aborts():
        seq = Sequencer(max_size=epoch_size)
        seq.sequencer_id = 1
        seq.n_workers = 4
        for i in range(epoch_size):
            payload = RunFuncPayload(
                request_id=i.to_bytes(8, "big"),
                key=i,
                operator_name="op",
                partition=i % 4,
                function_name="func",
                params=(),
            )
            seq.sequence(payload)
        seq.get_epoch()
        # 10% abort rate
        aborts = set(random.sample(range(epoch_size), epoch_size // 10))
        seq.increment_epoch(max_t_counter=epoch_size, t_ids_to_reschedule=aborts)

    bench(f"sequence + get_epoch ({epoch_size} msgs)", sequence_epoch, iterations, epoch_size)
    bench(f"increment_epoch (10% abort rate)", increment_with_aborts, iterations, 1)


# ---------------------------------------------------------------------------
# 6. Simulated Full Epoch (for cProfile)
# ---------------------------------------------------------------------------


def simulate_epoch(epoch_size: int, keys_per_txn: int, n_partitions: int, value_size: int) -> None:
    """Simulate one full Aria epoch for profiling.

    Models: ingress → hashing → sequencing → function exec (get/put) →
    conflict detection → commit → cleanup.
    Excludes: networking I/O, Kafka, actual user functions.
    """
    from styx.common.run_func_payload import RunFuncPayload

    from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
    from worker.sequencer.sequencer import Sequencer

    op_partitions = {("op", p) for p in range(n_partitions)}
    state = InMemoryOperatorState(op_partitions)
    partitioner = HashPartitioner(n_partitions, None)
    seq = Sequencer(max_size=epoch_size)
    seq.sequencer_id = 1
    seq.n_workers = 4

    key_space = 10_000
    # Pre-populate state
    for p in range(n_partitions):
        batch = {k: make_state_value(value_size) for k in range(p * (key_space // n_partitions), (p + 1) * (key_space // n_partitions))}
        state.batch_insert(batch, "op", p)

    # --- INGRESS: hash + sequence ---
    messages = []
    for i in range(epoch_size):
        key = random.randint(0, key_space - 1)
        partition = partitioner.get_partition_no_cache(key)
        payload = RunFuncPayload(
            request_id=i.to_bytes(8, "big"),
            key=key,
            operator_name="op",
            partition=partition,
            function_name="transfer",
            params=(random.randint(0, key_space - 1),),
        )
        seq.sequence(payload)
        messages.append((key, partition, i + 1))  # (key, partition, t_id)

    epoch = seq.get_epoch()

    # --- PROCESSING: get + put per transaction ---
    for key, partition, t_id in messages:
        # Simulate reading keys_per_txn keys
        for _ in range(keys_per_txn):
            read_key = random.randint(0, key_space - 1)
            read_partition = partitioner.get_partition_no_cache(read_key)
            state.get(read_key, t_id, "op", read_partition)
        # Simulate writing 1 key
        state.put(key, make_state_value(value_size), t_id, "op", partition)

    # --- CONFLICT DETECTION ---
    aborted = state.check_conflicts()

    # --- COMMIT ---
    state.commit(aborted)

    # --- SERIALIZE RESPONSES (egress) ---
    for key, partition, t_id in messages:
        if t_id not in aborted:
            msgpack_serialization((key, t_id, "ok"))

    # --- CLEANUP ---
    state.cleanup()

    # --- SEQUENCER EPOCH INCREMENT ---
    seq.increment_epoch(
        max_t_counter=epoch_size + 1,
        t_ids_to_reschedule=aborted,
    )


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def print_summary(epoch_size: int, keys_per_txn: int, value_size: int, iterations: int) -> None:
    print("\n" + "=" * 70)
    print("SIMULATED EPOCH TIMING")
    print("=" * 70)
    print(f"  epoch_size={epoch_size}, keys_per_txn={keys_per_txn}, value_size={value_size}")
    print()

    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        simulate_epoch(epoch_size, keys_per_txn, 4, value_size)
        elapsed = time.perf_counter_ns() - start
        times.append(elapsed)

    times.sort()
    median_ms = times[len(times) // 2] / 1_000_000
    print(f"  Full epoch (median of {iterations})............ {median_ms:.1f} ms")
    print(f"  Throughput estimate.............. {epoch_size / (median_ms / 1000):.0f} txn/s (single thread, CPU only)")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile Styx hot paths")
    parser.add_argument("--epoch-size", type=int, default=DEFAULT_EPOCH_SIZE)
    parser.add_argument("--keys-per-txn", type=int, default=DEFAULT_KEYS_PER_TXN)
    parser.add_argument("--n-partitions", type=int, default=DEFAULT_N_PARTITIONS)
    parser.add_argument("--value-size", type=int, default=DEFAULT_VALUE_SIZE)
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--profile-epoch", action="store_true", help="Run cProfile on simulated epoch")
    args = parser.parse_args()

    print("=" * 70)
    print("STYX HOT PATH PROFILER")
    print("=" * 70)
    print(f"  epoch_size={args.epoch_size}, keys_per_txn={args.keys_per_txn}")
    print(f"  n_partitions={args.n_partitions}, value_size={args.value_size}")
    print(f"  iterations={args.iterations}")

    if args.profile_epoch:
        print("\n--- cProfile: simulated epoch ---\n")
        profiler = cProfile.Profile()
        profiler.enable()
        for _ in range(args.iterations):
            simulate_epoch(args.epoch_size, args.keys_per_txn, args.n_partitions, args.value_size)
        profiler.disable()
        stats = pstats.Stats(profiler)
        stats.strip_dirs()
        stats.sort_stats("cumulative")
        stats.print_stats(30)
        print("\n--- sorted by tottime ---\n")
        stats.sort_stats("tottime")
        stats.print_stats(30)
        return

    # Microbenchmarks
    bench_state_get(args.epoch_size, args.value_size, args.iterations)
    bench_serialization(args.epoch_size, args.iterations)
    bench_hashing(args.epoch_size, args.iterations)
    bench_conflict_detection(args.epoch_size, args.keys_per_txn, args.n_partitions, args.iterations)
    bench_sequencer(args.epoch_size, args.iterations)
    print_summary(args.epoch_size, args.keys_per_txn, args.value_size, args.iterations)


if __name__ == "__main__":
    main()
