"""Local runner demo — runs the YCSB workload with zero infrastructure.

Usage:
    python demo/demo-local-runner/client.py

This demo uses the same YCSB operator functions as demo-ycsb but executes
them locally via LocalStyxRunner instead of the distributed Styx cluster.
No Kafka, S3, coordinator, or workers are required.
"""

import asyncio
import random
import time

from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph
from styx.local_runner import LocalStyxRunner

from ycsb import NotEnoughCredit, ycsb_operator

# Configuration
N_ENTITIES = 100
N_PARTITIONS = 4
STARTING_MONEY = 1_000_000
N_TRANSFERS = 500
CONCURRENCY = 50  # number of concurrent transfers in flight


async def run_transfer(runner, key_a, key_b):
    """Run a single transfer, returning 1 if aborted."""
    try:
        await runner.send_event(
            "ycsb",
            key=key_a,
            function="transfer",
            params=(key_b,),
        )
    except NotEnoughCredit:
        return 1
    return 0


async def main() -> None:
    # 1. Build graph — identical to production setup
    g = StateflowGraph(
        "ycsb-benchmark",
        operator_state_backend=LocalStateBackend.DICT,
        max_operator_parallelism=N_PARTITIONS,
    )
    ycsb_operator.set_n_partitions(N_PARTITIONS)
    g.add_operators(ycsb_operator)

    # 2. Create runner and seed data
    runner = LocalStyxRunner(graph=g)
    runner.init_data("ycsb", {i: STARTING_MONEY for i in range(N_ENTITIES)})

    print(f"Initialized {N_ENTITIES} accounts with {STARTING_MONEY} each")
    print(f"Total money supply: {N_ENTITIES * STARTING_MONEY}")
    print()

    # 3. Run concurrent transfers in batches
    print(f"Running {N_TRANSFERS} transfers (concurrency={CONCURRENCY})...")
    start = time.perf_counter()
    aborted = 0

    # Generate all transfer pairs upfront
    pairs = []
    for _ in range(N_TRANSFERS):
        key_a = random.randint(0, N_ENTITIES - 1)
        key_b = random.randint(0, N_ENTITIES - 1)
        while key_b == key_a:
            key_b = random.randint(0, N_ENTITIES - 1)
        pairs.append((key_a, key_b))

    # Execute in concurrent batches
    for batch_start in range(0, N_TRANSFERS, CONCURRENCY):
        batch = pairs[batch_start : batch_start + CONCURRENCY]
        results = await asyncio.gather(
            *(run_transfer(runner, a, b) for a, b in batch),
        )
        aborted += sum(results)

    elapsed = time.perf_counter() - start
    print(f"Completed in {elapsed:.3f}s ({N_TRANSFERS / elapsed:.0f} txn/s)")
    print(f"Aborted (insufficient credit): {aborted}")
    print()

    # 4. Validate: total money supply is conserved
    state = runner.get_state("ycsb")
    total = sum(state.values())
    expected = N_ENTITIES * STARTING_MONEY
    print(f"Final money supply: {total}")
    print(f"Expected:           {expected}")
    assert total == expected, f"Money supply mismatch: {total} != {expected}"
    print("Conservation check PASSED")
    print()

    # 5. Spot-check reads
    print("Spot-checking 5 random accounts:")
    for key in random.sample(range(N_ENTITIES), 5):
        result = await runner.send_event("ycsb", key=key, function="read")
        print(f"  account {result[0]:>3d}: balance = {result[1]}")


if __name__ == "__main__":
    asyncio.run(main())
