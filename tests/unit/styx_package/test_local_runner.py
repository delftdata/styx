import pytest
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph
from styx.local_runner import LocalStyxRunner
from styx.local_runner.local_context import LocalStatefulFunction
from styx.local_runner.local_state import LocalOperatorState

# ---------------------------------------------------------------------------
# Helpers: reusable graph + function definitions
# ---------------------------------------------------------------------------


def _make_kv_graph():
    """Single-operator graph with read/write/insert functions."""
    g = StateflowGraph("test", operator_state_backend=LocalStateBackend.DICT)
    op = Operator("kv", n_partitions=4)

    @op.register
    async def read(ctx):
        return ctx.key, ctx.get()

    @op.register
    async def write(ctx, value):
        ctx.put(value)
        return ctx.key, value

    @op.register
    async def insert(ctx, value):
        ctx.put(value)

    @op.register
    async def read_data(ctx):
        return ctx.data

    g.add_operator(op)
    return g


def _make_transfer_graph():
    """Single-operator graph with transfer chain."""
    g = StateflowGraph("transfer-test", operator_state_backend=LocalStateBackend.DICT)
    op = Operator("accounts", n_partitions=2)

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
        return ctx.key, balance

    g.add_operator(op)
    return g


def _make_multi_operator_graph():
    """Two-operator graph for cross-operator chaining."""
    g = StateflowGraph("multi-op", operator_state_backend=LocalStateBackend.DICT)

    orders = Operator("orders", n_partitions=2)
    inventory = Operator("inventory", n_partitions=2)

    @orders.register
    async def place_order(ctx, item_key, quantity):
        ctx.put({"item": item_key, "qty": quantity, "status": "placed"})
        ctx.call_remote_async("inventory", "deduct", key=item_key, params=(quantity,))
        return ctx.key

    @inventory.register
    async def deduct(ctx, quantity):
        stock = ctx.get() or 0
        stock -= quantity
        ctx.put(stock)

    g.add_operator(orders)
    g.add_operator(inventory)
    return g


# ---------------------------------------------------------------------------
# LocalOperatorState unit tests
# ---------------------------------------------------------------------------


class TestLocalOperatorState:
    def test_init_partition_and_get(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        assert state.get("missing", "op", 0) is None

    def test_put_and_get(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        state.put("k1", 42, "op", 0)
        assert state.get("k1", "op", 0) == 42

    def test_get_all(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        state.put("a", 1, "op", 0)
        state.put("b", 2, "op", 0)
        assert state.get_all("op", 0) == {"a": 1, "b": 2}

    def test_get_all_returns_copy(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        state.put("a", 1, "op", 0)
        copy = state.get_all("op", 0)
        copy["a"] = 999
        assert state.get("a", "op", 0) == 1

    def test_batch_insert(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        state.batch_insert({"x": 10, "y": 20}, "op", 0)
        assert state.get("x", "op", 0) == 10
        assert state.get("y", "op", 0) == 20

    def test_separate_partitions(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        state.init_partition("op", 1)
        state.put("k", "val0", "op", 0)
        state.put("k", "val1", "op", 1)
        assert state.get("k", "op", 0) == "val0"
        assert state.get("k", "op", 1) == "val1"

    def test_separate_operators(self):
        state = LocalOperatorState()
        state.init_partition("a", 0)
        state.init_partition("b", 0)
        state.put("k", "from_a", "a", 0)
        state.put("k", "from_b", "b", 0)
        assert state.get("k", "a", 0) == "from_a"
        assert state.get("k", "b", 0) == "from_b"


# ---------------------------------------------------------------------------
# LocalStatefulFunction unit tests
# ---------------------------------------------------------------------------


class TestLocalStatefulFunction:
    def test_key_property(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        ctx = LocalStatefulFunction(key=42, operator_name="op", partition=0, state=state)
        assert ctx.key == 42

    def test_get_put(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        ctx = LocalStatefulFunction(key="k", operator_name="op", partition=0, state=state)
        assert ctx.get() is None
        ctx.put("hello")
        assert ctx.get() == "hello"

    def test_data_property(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        state.put("a", 1, "op", 0)
        state.put("b", 2, "op", 0)
        ctx = LocalStatefulFunction(key="a", operator_name="op", partition=0, state=state)
        assert ctx.data == {"a": 1, "b": 2}

    def test_batch_insert(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        ctx = LocalStatefulFunction(key="k", operator_name="op", partition=0, state=state)
        ctx.batch_insert({"x": 10, "y": 20})
        assert state.get("x", "op", 0) == 10

    def test_call_remote_async_and_drain(self):
        state = LocalOperatorState()
        state.init_partition("op", 0)
        ctx = LocalStatefulFunction(key="k", operator_name="op", partition=0, state=state)
        ctx.call_remote_async("target_op", "func_a", key=1, params=(42,))
        ctx.call_remote_async("target_op", "func_b", key=2)
        calls = ctx.drain_remote_calls()
        assert len(calls) == 2
        assert calls[0] == ("target_op", "func_a", 1, (42,))
        assert calls[1] == ("target_op", "func_b", 2, ())
        # drain clears the queue
        assert ctx.drain_remote_calls() == []

    def test_call_remote_async_type_function_name(self):
        """When function_name is a type, its __name__ should be used."""
        state = LocalOperatorState()
        state.init_partition("op", 0)
        ctx = LocalStatefulFunction(key="k", operator_name="op", partition=0, state=state)

        class MyFunc:
            pass

        ctx.call_remote_async("op", MyFunc, key=1)
        calls = ctx.drain_remote_calls()
        assert calls[0][1] == "MyFunc"


# ---------------------------------------------------------------------------
# LocalStyxRunner integration tests
# ---------------------------------------------------------------------------


class TestLocalStyxRunnerBasics:
    async def test_init_data_and_read(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)
        runner.init_data("kv", {0: "a", 1: "b", 2: "c"})

        result = await runner.send_event("kv", key=0, function="read")
        assert result == (0, "a")

    async def test_write_and_read(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)

        await runner.send_event("kv", key=5, function="write", params=(100,))
        result = await runner.send_event("kv", key=5, function="read")
        assert result == (5, 100)

    async def test_insert_no_return(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)

        result = await runner.send_event("kv", key=10, function="insert", params=("val",))
        assert result is None
        state = runner.get_state("kv")
        assert state[10] == "val"

    async def test_read_data_property(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)
        # Put two keys that land in the same partition
        # We use key 0 and check its partition data
        runner.init_data("kv", {0: "zero"})
        result = await runner.send_event("kv", key=0, function="read_data")
        assert 0 in result
        assert result[0] == "zero"


class TestLocalStyxRunnerChaining:
    async def test_transfer_chain(self):
        g = _make_transfer_graph()
        runner = LocalStyxRunner(graph=g)
        runner.init_data("accounts", {0: 1000, 1: 500})

        result = await runner.send_event("accounts", key=0, function="transfer", params=(1, 100))
        assert result == (0, 900)

        state = runner.get_state("accounts")
        assert state[0] == 900
        assert state[1] == 600

    async def test_multi_transfer(self):
        g = _make_transfer_graph()
        runner = LocalStyxRunner(graph=g)
        runner.init_data("accounts", {0: 1000, 1: 500})

        await runner.send_event("accounts", key=0, function="transfer", params=(1, 100))
        await runner.send_event("accounts", key=1, function="transfer", params=(0, 50))

        state = runner.get_state("accounts")
        assert state[0] == 950  # 1000 - 100 + 50
        assert state[1] == 550  # 500 + 100 - 50


class TestLocalStyxRunnerMultiOperator:
    async def test_cross_operator_chain(self):
        g = _make_multi_operator_graph()
        runner = LocalStyxRunner(graph=g)
        runner.init_data("inventory", {42: 100})

        order_key = await runner.send_event("orders", key=1, function="place_order", params=(42, 5))
        assert order_key == 1

        # Check order was placed
        order_state = runner.get_state("orders")
        assert order_state[1] == {"item": 42, "qty": 5, "status": "placed"}

        # Check inventory was deducted
        inv_state = runner.get_state("inventory")
        assert inv_state[42] == 95


class TestLocalStyxRunnerErrorHandling:
    async def test_user_function_raises(self):
        g = StateflowGraph("err", operator_state_backend=LocalStateBackend.DICT)
        op = Operator("fail", n_partitions=1)

        @op.register
        async def boom(ctx):
            msg = "something went wrong"
            raise ValueError(msg)

        g.add_operator(op)
        runner = LocalStyxRunner(graph=g)

        with pytest.raises(ValueError, match="something went wrong"):
            await runner.send_event("fail", key=0, function="boom")

    async def test_unknown_function_raises(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)

        with pytest.raises(KeyError):
            await runner.send_event("kv", key=0, function="nonexistent")

    async def test_unknown_operator_raises(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)

        with pytest.raises(KeyError):
            await runner.send_event("nonexistent", key=0, function="read")


class TestLocalStyxRunnerGetState:
    async def test_get_state_merges_partitions(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)
        runner.init_data("kv", {0: "a", 1: "b", 2: "c", 3: "d"})

        state = runner.get_state("kv")
        assert state == {0: "a", 1: "b", 2: "c", 3: "d"}

    async def test_get_state_empty(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)
        assert runner.get_state("kv") == {}


class TestLocalStyxRunnerBatch:
    async def test_send_batch(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)
        runner.init_data("kv", {0: "x", 1: "y"})

        results = await runner.send_batch(
            [
                ("kv", 0, "read", ()),
                ("kv", 1, "read", ()),
                ("kv", 2, "write", (99,)),
            ]
        )

        assert results[0] == (0, "x")
        assert results[1] == (1, "y")
        assert results[2] == (2, 99)
        assert runner.get_state("kv")[2] == 99


class TestLocalStyxRunnerSync:
    def test_send_event_sync(self):
        g = _make_kv_graph()
        runner = LocalStyxRunner(graph=g)
        runner.init_data("kv", {42: "hello"})

        result = runner.send_event_sync("kv", key=42, function="read")
        assert result == (42, "hello")
