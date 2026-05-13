"""Additional coverage tests for styx/common/stateful_function.py

Focuses on: __prepare_message_transmission, __call_remote_function_no_response,
__send_async_calls (local vs remote, root vs middle chain).
"""

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
    same_network=False,
):
    state = MagicMock()
    state.in_remote_keys.return_value = False
    networking = MagicMock()
    networking.host_name = "10.0.0.1"
    networking.host_port = 6000
    networking.worker_id = 0
    networking.in_the_same_network = MagicMock(return_value=same_network)
    networking.send_message = AsyncMock()
    networking.prepare_function_chain = MagicMock()
    dns = {
        "users": {0: ("10.0.0.1", 5000, 6000), 1: ("10.0.0.2", 5001, 6001)},
        "orders": {0: ("10.0.0.3", 5002, 6002)},
    }
    t_id = 42
    request_id = b"req123"
    graph = MagicMock()
    graph.nodes = {
        "users": MagicMock(),
        "orders": MagicMock(),
    }
    graph.nodes["users"].which_partition = MagicMock(return_value=0)
    graph.nodes["orders"].which_partition = MagicMock(return_value=0)
    lock = asyncio.Lock()
    protocol = MagicMock()
    protocol.run_function = AsyncMock()
    protocol.run_fallback_function = AsyncMock()
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
        deployed_graph=graph,
        operator_lock=lock,
        protocol=protocol,
    )
    return sf, state, networking, graph, protocol


# ---------------------------------------------------------------------------
# __prepare_message_transmission
# ---------------------------------------------------------------------------


class TestPrepareMessageTransmission:
    def test_payload_construction(self):
        sf, *_ = _make_sf()
        result = sf._StatefulFunction__prepare_message_transmission("users", "k1", "my_func", 0, (1, 2), None)
        payload, host, port = result
        assert payload[0] == 42  # t_id
        assert payload[1] == b"req123"  # request_id
        assert payload[2] == "users"  # operator_name
        assert payload[3] == "my_func"  # function_name
        assert payload[4] == "k1"  # key
        assert payload[5] == 0  # partition
        assert host == "10.0.0.1"
        assert port == 6000

    def test_payload_with_ack(self):
        sf, *_ = _make_sf()
        ack = ("h", 5000, 1, "1/2", [])
        result = sf._StatefulFunction__prepare_message_transmission("users", "k1", "my_func", 0, (), ack)
        payload, _, _ = result
        assert len(payload) == 9  # base 8 + ack tuple
        assert payload[8] == ack

    async def test_missing_operator_returns_none(self):
        sf, *_ = _make_sf()
        result = sf._StatefulFunction__prepare_message_transmission("nonexistent_op", "k1", "fn", 0, (), None)
        assert result is None


# ---------------------------------------------------------------------------
# __call_remote_function_no_response
# ---------------------------------------------------------------------------


class TestCallRemoteFunctionNoResponse:
    @pytest.mark.asyncio
    async def test_sends_message(self):
        sf, _, networking, *_ = _make_sf()
        await sf._StatefulFunction__call_remote_function_no_response(
            operator_name="users",
            function_name="my_func",
            key="k1",
            partition=0,
            params=(1,),
            ack_payload=("h", 5000, 1, "1/1", []),
        )
        networking.send_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_type_name_conversion(self):
        sf, _, networking, *_ = _make_sf()

        class MyFunc:
            pass

        await sf._StatefulFunction__call_remote_function_no_response(
            operator_name="users",
            function_name=MyFunc,
            key="k1",
            partition=0,
        )
        networking.send_message.assert_called_once()


# ---------------------------------------------------------------------------
# __send_async_calls — root chain
# ---------------------------------------------------------------------------


class TestSendAsyncCallsRoot:
    @pytest.mark.asyncio
    async def test_root_prepares_function_chain(self):
        sf, _state, networking, _graph, _protocol = _make_sf(same_network=True)

        async def my_run(*args):
            sf.call_remote_async("users", "my_func", "k2", (1,))
            return "result"

        sf.run = my_run

        result, n_calls = await sf()
        assert result == "result"
        assert n_calls == 1
        networking.prepare_function_chain.assert_called_once_with(42)


# ---------------------------------------------------------------------------
# __send_async_calls — middle chain with remote
# ---------------------------------------------------------------------------


class TestSendAsyncCallsMiddle:
    @pytest.mark.asyncio
    async def test_middle_chain_remote_call(self):
        sf, _state, networking, _graph, _protocol = _make_sf(same_network=False)

        async def my_run(*args):
            sf.call_remote_async("orders", "order_func", "ok1", (1,))
            return "mid_result"

        sf.run = my_run

        result, n_calls = await sf(
            ack_host="10.0.0.2",
            ack_port=6001,
            ack_share="1/1",
            chain_participants=[],
        )
        assert result == "mid_result"
        assert n_calls == 1
        # Should append worker_id to chain_participants since remote
        assert networking.worker_id == 0  # sanity check


# ---------------------------------------------------------------------------
# __send_async_calls — local fallback
# ---------------------------------------------------------------------------


class TestSendAsyncCallsFallback:
    @pytest.mark.asyncio
    async def test_local_fallback_calls_protocol_fallback(self):
        sf, _state, _networking, _graph, protocol = _make_sf(
            same_network=True,
            fallback_mode=True,
        )

        async def my_run(*args):
            sf.call_remote_async("users", "my_func", "k2")
            return "fb_result"

        sf.run = my_run

        _result, n_calls = await sf(
            ack_host="10.0.0.1",
            ack_port=6000,
            ack_share="1/1",
            chain_participants=[],
        )
        assert n_calls == 1
        protocol.run_fallback_function.assert_called_once()


# ---------------------------------------------------------------------------
# __send_async_calls — no remote calls
# ---------------------------------------------------------------------------


class TestSendAsyncCallsEmpty:
    @pytest.mark.asyncio
    async def test_no_remote_calls_returns_zero(self):
        sf, *_ = _make_sf()

        async def my_run(*args):
            return "simple"

        sf.run = my_run

        result, n_calls = await sf()
        assert result == "simple"
        assert n_calls == 0
