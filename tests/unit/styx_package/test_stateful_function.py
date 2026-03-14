"""Unit tests for styx/common/stateful_function.py"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from styx.common.stateful_function import StatefulFunction

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sf(
    key="k1",
    function_name="my_func",
    partition=0,
    operator_name="users",
    fallback_mode=False,
    use_fallback_cache=False,
):
    """Creates a StatefulFunction with mocked dependencies."""
    state = MagicMock()
    networking = MagicMock()
    networking.host_name = "localhost"
    networking.host_port = 6000
    networking.worker_id = 0
    networking.in_the_same_network = MagicMock(return_value=False)
    dns = {"users": {0: ("host1", 5000, 6000), 1: ("host2", 5001, 6001)}}
    t_id = 42
    request_id = b"req123"

    graph = MagicMock()
    graph.nodes = {"users": MagicMock()}
    graph.nodes["users"].which_partition = MagicMock(return_value=0)

    lock = asyncio.Lock()
    protocol = MagicMock()

    sf = StatefulFunction(
        key=key,
        function_name=function_name,
        partition=partition,
        operator_name=operator_name,
        operator_state=state,
        networking=networking,
        dns=dns,
        t_id=t_id,
        request_id=request_id,
        fallback_mode=fallback_mode,
        use_fallback_cache=use_fallback_cache,
        deployed_graph=graph,
        operator_lock=lock,
        protocol=protocol,
    )
    return sf, state, networking, graph, protocol


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------


class TestStatefulFunctionProperties:
    def test_key_property(self):
        sf, *_ = _make_sf(key="mykey")
        assert sf.key == "mykey"

    def test_name_from_function_class(self):
        sf, *_ = _make_sf(function_name="calc")
        assert sf.name == "calc"

    def test_data_property_calls_get_all(self):
        sf, state, *_ = _make_sf()
        state.get_all.return_value = {"a": 1, "b": 2}
        result = sf.data
        state.get_all.assert_called_once_with(42, "users", 0)
        assert result == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# get / put
# ---------------------------------------------------------------------------


class TestStatefulFunctionGetPut:
    def test_get_normal_mode(self):
        sf, state, *_ = _make_sf(fallback_mode=False)
        state.get.return_value = "value1"
        result = sf.get()
        state.get.assert_called_once_with("k1", 42, "users", 0)
        assert result == "value1"

    def test_get_fallback_mode(self):
        sf, state, *_ = _make_sf(fallback_mode=True)
        state.get_immediate.return_value = "fb_value"
        result = sf.get()
        state.get_immediate.assert_called_once_with("k1", 42, "users", 0)
        assert result == "fb_value"

    def test_put_normal_mode(self):
        sf, state, *_ = _make_sf(fallback_mode=False)
        sf.put("new_value")
        state.put.assert_called_once_with("k1", "new_value", 42, "users", 0)

    def test_put_fallback_mode(self):
        sf, state, *_ = _make_sf(fallback_mode=True)
        sf.put("fb_new")
        state.put_immediate.assert_called_once_with("k1", "fb_new", 42, "users", 0)


# ---------------------------------------------------------------------------
# batch_insert
# ---------------------------------------------------------------------------


class TestBatchInsert:
    def test_batch_insert_with_data(self):
        sf, state, *_ = _make_sf()
        kv = {"a": 1, "b": 2}
        sf.batch_insert(kv)
        state.batch_insert.assert_called_once_with(kv, "users", 0)

    def test_batch_insert_empty_does_nothing(self):
        sf, state, *_ = _make_sf()
        sf.batch_insert({})
        state.batch_insert.assert_not_called()


# ---------------------------------------------------------------------------
# call_remote_async
# ---------------------------------------------------------------------------


class TestCallRemoteAsync:
    def test_queues_remote_call_string_name(self):
        sf, _state, _networking, _graph, _ = _make_sf()
        sf.call_remote_async("users", "my_func", "key1", (1, 2))
        # Check that internal list was populated
        assert len(sf._StatefulFunction__async_remote_calls) == 1
        entry = sf._StatefulFunction__async_remote_calls[0]
        assert entry[0] == "users"
        assert entry[1] == "my_func"

    def test_queues_remote_call_type_name(self):
        sf, _state, _networking, _graph, _ = _make_sf()

        class MyFunc:
            pass

        sf.call_remote_async("users", MyFunc, "key1")
        entry = sf._StatefulFunction__async_remote_calls[0]
        assert entry[1] == "MyFunc"

    def test_multiple_remote_calls(self):
        sf, *_ = _make_sf()
        sf.call_remote_async("users", "func1", "k1")
        sf.call_remote_async("users", "func2", "k2")
        assert len(sf._StatefulFunction__async_remote_calls) == 2


# ---------------------------------------------------------------------------
# __call__
# ---------------------------------------------------------------------------


class TestStatefulFunctionCall:
    @pytest.mark.asyncio
    async def test_call_returns_exception_on_error(self):
        sf, state, *_ = _make_sf()
        state.in_remote_keys.return_value = False
        # run() is not implemented on the base class
        result, n_calls, partial = await sf()
        assert isinstance(result, NotImplementedError)
        assert n_calls == -1
        assert partial == -1

    @pytest.mark.asyncio
    async def test_call_with_no_remote_keys(self):
        sf, state, *_ = _make_sf()
        state.in_remote_keys.return_value = False

        # Patch run to return a value
        async def mock_run(*args):
            return "success"

        sf.run = mock_run
        result, n_calls, partial = await sf()
        assert result == "success"
        assert n_calls == 0
        assert partial == 0

    @pytest.mark.asyncio
    async def test_call_local_migration(self):
        sf, state, _networking, *_ = _make_sf()
        state.in_remote_keys.return_value = True
        state.get_worker_id_old_partition.return_value = (0, 1)
        state.operator_partitions = {("users", 1)}  # old partition is local

        async def mock_run(*args):
            return "migrated"

        sf.run = mock_run
        result, _n_calls, _partial = await sf()
        state.migrate_within_the_same_worker.assert_called_once_with("users", 0, "k1", 1)
        assert result == "migrated"

    @pytest.mark.asyncio
    async def test_call_remote_migration(self):
        sf, state, networking, *_ = _make_sf()
        state.in_remote_keys.return_value = True
        state.get_worker_id_old_partition.return_value = (1, 2)
        state.operator_partitions = set()  # not local

        networking.request_key = AsyncMock()
        networking.wait_for_remote_key_event = AsyncMock()

        async def mock_run(*args):
            return "remote_migrated"

        sf.run = mock_run
        result, _n_calls, _partial = await sf()
        networking.request_key.assert_called_once()
        networking.wait_for_remote_key_event.assert_called_once()
        assert result == "remote_migrated"

    @pytest.mark.asyncio
    async def test_fallback_cache_shortcircuit(self):
        sf, state, *_ = _make_sf(fallback_mode=True, use_fallback_cache=True)
        state.in_remote_keys.return_value = False

        async def mock_run(*args):
            return "cached"

        sf.run = mock_run
        result, _n_calls, partial = await sf()
        assert result == "cached"
        assert partial == -1
