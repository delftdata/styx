# Styx Local Runner

An in-process executor for Styx stateful functions.
Runs your functions against a plain dict-backed state with **zero infrastructure** — no Kafka, S3, coordinator, or workers required.

Use it to validate function logic (get/put/chaining) before deploying to a full Styx cluster.

## Installation

The local runner ships with the `styx-package`:

```bash
pip install -e styx-package/
```

No additional dependencies are needed.

## Quick Start

```python
import asyncio
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph
from styx.local_runner import LocalStyxRunner

# 1. Define your graph — identical to production
g = StateflowGraph("my-app", operator_state_backend=LocalStateBackend.DICT)
op = Operator("accounts", n_partitions=4)

@op.register
async def read(ctx):
    return ctx.key, ctx.get()

@op.register
async def deposit(ctx, amount):
    balance = ctx.get() or 0
    balance += amount
    ctx.put(balance)
    return ctx.key, balance

g.add_operator(op)

# 2. Create runner and seed state
runner = LocalStyxRunner(graph=g)
runner.init_data("accounts", {0: 1000, 1: 2000})

# 3. Execute functions
async def main():
    result = await runner.send_event("accounts", key=0, function="read")
    print(result)  # (0, 1000)

    result = await runner.send_event("accounts", key=0, function="deposit", params=(500,))
    print(result)  # (0, 1500)

asyncio.run(main())
```

## API Reference

### `LocalStyxRunner(graph)`

Create a runner from a `StateflowGraph`. Initializes all operator partitions.

### `runner.init_data(operator_name, key_value_pairs)`

Pre-populate state for an operator. Keys are automatically routed to the correct partition using the same `HashPartitioner` as production.

```python
runner.init_data("accounts", {0: 1000, 1: 2000, 2: 500})
```

### `await runner.send_event(operator_name, key, function, params=())`

Execute a single function. Returns whatever the user function returns. Chained `call_remote_async` calls are executed depth-first after the function completes.

```python
result = await runner.send_event("accounts", key=0, function="deposit", params=(100,))
```

### `runner.send_event_sync(operator_name, key, function, params=())`

Synchronous wrapper that calls `asyncio.run()` internally. Convenient for scripts and REPLs.

```python
result = runner.send_event_sync("accounts", key=0, function="read")
```

**Caveat:** Cannot be called from within an already-running event loop (Jupyter notebooks, pytest-asyncio). Use `await send_event()` in those contexts.

### `await runner.send_batch(events)`

Execute a list of events sequentially. Each event is a tuple of `(operator_name, key, function, params)`.

```python
results = await runner.send_batch([
    ("accounts", 0, "read", ()),
    ("accounts", 1, "deposit", (100,)),
])
```

### `runner.get_state(operator_name)`

Return a merged dict of all state across all partitions for an operator. Useful for assertions and debugging.

```python
state = runner.get_state("accounts")
# {0: 1500, 1: 2000, 2: 500}
```

## Concurrent Execution

`send_event` is safe to call concurrently with `asyncio.gather()`. The runner uses per-key `asyncio.Lock` to serialize access to the same key, while allowing full parallelism across different keys.

```python
# Run 50 transfers concurrently
await asyncio.gather(
    runner.send_event("accounts", key=0, function="deposit", params=(10,)),
    runner.send_event("accounts", key=1, function="deposit", params=(20,)),
    runner.send_event("accounts", key=2, function="deposit", params=(30,)),
    # ...
)
```

### How locking works

1. Before executing a function, the runner acquires the lock for `(operator_name, key)`.
2. The user function runs while the lock is held — `get()` and `put()` are protected.
3. The lock is **released before** any chained `call_remote_async` calls execute.
4. Each chained call then acquires its own key lock independently.

This design prevents deadlocks on bidirectional chains (e.g. concurrent `transfer(A→B)` and `transfer(B→A)`).

### Concurrency caveats

- Concurrent calls to the **same key** are serialized (one at a time). This is correct but means throughput on a single hot key is limited.
- Chained calls execute **after** the parent's lock is released. Between the parent's `put()` and the child's execution, another concurrent task could modify the parent's key. This differs from production where the entire transaction (parent + chains) is atomic within an epoch.

## Function Chaining

`call_remote_async` works as expected. The runner resolves chained calls depth-first:

```python
@op.register
async def transfer(ctx, to_key, amount):
    balance = ctx.get()
    balance -= amount
    ctx.put(balance)
    ctx.call_remote_async("accounts", "credit", key=to_key, params=(amount,))
    return ctx.key, balance

@op.register
async def credit(ctx, amount):
    balance = ctx.get() or 0
    balance += amount
    ctx.put(balance)
```

After `transfer` returns, the runner immediately executes `credit` on the target key. If `credit` itself calls `call_remote_async`, those are executed next (depth-first recursion).

Cross-operator chains work the same way:

```python
ctx.call_remote_async("inventory", "deduct", key=item_id, params=(qty,))
```

## Partitioning

The runner uses the real `HashPartitioner` (via `operator.which_partition(key)`) so keys land in the same partitions as they would in production. This matters when using `ctx.data` which returns all state in the current partition.

## What the Local Runner Provides

- Validate that `get`/`put`/`call_remote_async` calls are wired correctly
- Test function return values and state mutations
- Verify chaining logic across operators
- Catch exceptions raised by user functions
- Debug with `get_state()` to inspect all state at any point
- Concurrent execution with per-key safety via `asyncio.gather()`
- Same partitioning behavior as production

## Limitations and Differences from Production

Understanding these differences is critical. Passing local tests does **not** guarantee production correctness.

### No transactional semantics

In production, Styx uses the Aria protocol to execute functions in epoch-based batches with optimistic concurrency control. Conflicting transactions are detected and aborted. The local runner has **none of this** — `put()` writes are immediate and permanent, there is no abort/retry mechanism, and there are no epochs.

**Impact:** Code that relies on transaction atomicity across multiple keys (beyond what per-key locking provides) may behave differently in production.

### No conflict detection

In production, concurrent writes to the same key within an epoch trigger conflict detection, and one transaction is aborted and retried. Locally, concurrent writes to the same key are serialized by the per-key lock — they always succeed but the "last writer wins" based on lock acquisition order.

**Impact:** Write-write conflicts that would cause aborts in production are invisible locally.

### DFS chain execution

In production, `call_remote_async` chains execute in the next epoch batch or concurrently across workers. Locally, chains execute synchronously and depth-first after the parent function returns, and **after** the parent key's lock is released.

**Impact:**
- Code that depends on chain execution order relative to other concurrent transactions may behave differently.
- The window between the parent's `put()` and the child's execution is a point where concurrent tasks can interleave.

### No serialization round-trip

In production, function parameters and state values are serialized (msgpack) when sent across the network. The local runner passes Python objects directly. Values that are not msgpack-serializable will work locally but fail in production.

**Impact:** Test with production-representative data types (strings, numbers, lists, dicts) rather than arbitrary Python objects.

### API drift risk

`LocalStatefulFunction` duck-types the real `StatefulFunction`. If `StatefulFunction` gains new methods (or changes signatures), the local runner will silently diverge. Code that works locally may call a method that doesn't exist in the local context, or vice versa.

The user-facing methods that are mirrored:
- `ctx.key` (property)
- `ctx.data` (property)
- `ctx.get()`
- `ctx.put(value)`
- `ctx.batch_insert(kv_pairs)`
- `ctx.call_remote_async(operator_name, function_name, key, params)`

### `send_event_sync` and event loops

`send_event_sync` uses `asyncio.run()` internally, which creates a new event loop. This fails if called from within an already-running loop:

- **Jupyter notebooks**: Use `await runner.send_event(...)` directly in cells
- **pytest-asyncio**: Use `async def test_...` with `await runner.send_event(...)`
- **Scripts**: `send_event_sync` works fine

### Python recursion limit

Chains are executed via recursive calls to `_execute_function`. Python's default recursion limit (1000) acts as a guard against infinite chains. Deeply nested chains (>500 levels) may hit this limit.

## Demo

A full working demo is at `demo/demo-local-runner/`:

```bash
python demo/demo-local-runner/client.py
```

It runs 500 concurrent YCSB transfers across 100 accounts and validates that the total money supply is conserved. See `demo/demo-local-runner/client.py` for the source.

## Running Tests

```bash
pytest tests/unit/styx_package/test_local_runner.py -v
```

The test suite covers state basics, function execution, chaining, cross-operator chains, error handling, batch execution, sync wrapper, and concurrent execution (including bidirectional transfer deadlock safety).
