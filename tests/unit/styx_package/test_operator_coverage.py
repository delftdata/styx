"""Additional coverage tests for styx/common/operator.py

Focuses on paths not covered by test_operator.py: materialize_function error,
attach_state_networking, run_function success/error paths, ack routing.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from styx.common.exceptions import OperatorDoesNotContainFunctionError
from styx.common.message_types import MessageType
from styx.common.operator import Operator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _operator(name="users", n_partitions=2):
    return Operator(name, n_partitions)


def _mock_networking(same_network=False):
    n = MagicMock()
    n.host_name = "10.0.0.1"
    n.host_port = 6000
    n.worker_id = 0
    n.in_the_same_network = MagicMock(return_value=same_network)
    n.send_message = AsyncMock()
    n.add_response = MagicMock()
    n.abort_chain = MagicMock()
    n.add_ack_fraction_str = MagicMock()
    n.add_ack_cnt = MagicMock()
    n.prepare_function_chain = MagicMock()
    n.add_remote_function_call = MagicMock()
    return n


def _mock_state():
    s = MagicMock()
    s.in_remote_keys.return_value = False
    return s


def _mock_graph():
    return MagicMock()


def _mock_protocol():
    p = MagicMock()
    p.run_function = AsyncMock()
    p.run_fallback_function = AsyncMock()
    return p


def _setup_operator(same_network=False):
    """Creates an operator with a registered function and attached dependencies."""
    op = _operator()

    async def my_func(ctx, *args):
        return "result"

    op.register(my_func)
    state = _mock_state()
    networking = _mock_networking(same_network)
    dns = {"users": {0: ("10.0.0.1", 5000, 6000), 1: ("10.0.0.2", 5001, 6001)}}
    graph = _mock_graph()
    op.attach_state_networking(state, networking, dns, graph)
    return op, state, networking, graph


# ---------------------------------------------------------------------------
# __init__ with USE_COMPOSITE_KEYS=false
# ---------------------------------------------------------------------------


class TestOperatorInitCompositeKeys:
    def test_init_with_composite_keys_false(self):
        with patch.dict("os.environ", {"USE_COMPOSITE_KEYS": "false"}):
            import importlib

            import styx.common.operator as op_mod

            importlib.reload(op_mod)
            try:
                o = op_mod.Operator("test", 4)
                # Partitioner should be created with None for composite params
                assert o.get_partitioner() is not None
            finally:
                # Restore
                import os

                os.environ["USE_COMPOSITE_KEYS"] = "true"
                importlib.reload(op_mod)


# ---------------------------------------------------------------------------
# __materialize_function error
# ---------------------------------------------------------------------------


class TestMaterializeFunctionError:
    @pytest.mark.asyncio
    async def test_missing_function_raises(self):
        op, *_ = _setup_operator()
        protocol = _mock_protocol()
        with pytest.raises(OperatorDoesNotContainFunctionError):
            await op.run_function(
                key="k1",
                t_id=1,
                request_id=b"req",
                function_name="nonexistent",
                partition=0,
                ack_payload=None,
                fallback_mode=False,
                use_fallback_cache=False,
                params=(),
                protocol=protocol,
            )


# ---------------------------------------------------------------------------
# attach_state_networking
# ---------------------------------------------------------------------------


class TestAttachStateNetworking:
    def test_attach_sets_internal_state(self):
        op = _operator()
        state = _mock_state()
        networking = _mock_networking()
        dns = {"users": {0: ("h", 5000, 6000)}}
        graph = _mock_graph()
        op.attach_state_networking(state, networking, dns, graph)
        assert op.dns == dns


# ---------------------------------------------------------------------------
# run_function — root call, success
# ---------------------------------------------------------------------------


class TestRunFunctionRoot:
    @pytest.mark.asyncio
    async def test_root_success_stores_response(self):
        op, _state, networking, _graph = _setup_operator()
        protocol = _mock_protocol()
        result = await op.run_function(
            key="k1",
            t_id=1,
            request_id=b"req",
            function_name="my_func",
            partition=0,
            ack_payload=None,
            fallback_mode=False,
            use_fallback_cache=False,
            params=(),
            protocol=protocol,
        )
        assert result is True
        networking.add_response.assert_called_once_with(1, "result")

    @pytest.mark.asyncio
    async def test_root_exception_aborts_chain(self):
        op, _state, networking, _graph = _setup_operator()
        protocol = _mock_protocol()

        async def bad_func(ctx, *args):
            return ValueError("boom")

        op.register(bad_func)
        result = await op.run_function(
            key="k1",
            t_id=1,
            request_id=b"req",
            function_name="bad_func",
            partition=0,
            ack_payload=None,
            fallback_mode=False,
            use_fallback_cache=False,
            params=(),
            protocol=protocol,
        )
        assert result is False
        networking.abort_chain.assert_called_once()

    @pytest.mark.asyncio
    async def test_root_none_response_no_add_response(self):
        op = _operator()

        async def no_return_func(ctx, *args):
            return None

        op.register(no_return_func)
        state = _mock_state()
        networking = _mock_networking()
        dns = {"users": {0: ("10.0.0.1", 5000, 6000)}}
        op.attach_state_networking(state, networking, dns, _mock_graph())
        protocol = _mock_protocol()
        result = await op.run_function(
            key="k1",
            t_id=1,
            request_id=b"req",
            function_name="no_return_func",
            partition=0,
            ack_payload=None,
            fallback_mode=False,
            use_fallback_cache=False,
            params=(),
            protocol=protocol,
        )
        assert result is True
        networking.add_response.assert_not_called()


# ---------------------------------------------------------------------------
# run_function — chain call (ack_payload is not None)
# ---------------------------------------------------------------------------


class TestRunFunctionChain:
    @pytest.mark.asyncio
    async def test_chain_success_sends_ack_local(self):
        op, _state, networking, _graph = _setup_operator(same_network=True)
        protocol = _mock_protocol()
        ack_payload = ("10.0.0.1", 6000, 1, "1/1", [], 0)
        result = await op.run_function(
            key="k1",
            t_id=1,
            request_id=b"req",
            function_name="my_func",
            partition=0,
            ack_payload=ack_payload,
            fallback_mode=False,
            use_fallback_cache=False,
            params=(),
            protocol=protocol,
        )
        assert result is True
        networking.add_ack_fraction_str.assert_called_once()

    @pytest.mark.asyncio
    async def test_chain_success_sends_ack_remote(self):
        op, _state, networking, _graph = _setup_operator(same_network=False)
        protocol = _mock_protocol()
        ack_payload = ("10.0.0.2", 6001, 1, "1/1", [], 0)
        result = await op.run_function(
            key="k1",
            t_id=1,
            request_id=b"req",
            function_name="my_func",
            partition=0,
            ack_payload=ack_payload,
            fallback_mode=False,
            use_fallback_cache=False,
            params=(),
            protocol=protocol,
        )
        assert result is True
        networking.send_message.assert_called()

    @pytest.mark.asyncio
    async def test_chain_exception_sends_abort(self):
        op, _state, networking, _graph = _setup_operator(same_network=True)
        protocol = _mock_protocol()

        async def bad_func(ctx, *args):
            return ValueError("chain_error")

        op.register(bad_func)
        ack_payload = ("10.0.0.1", 6000, 1, "1/1", [], 0)
        result = await op.run_function(
            key="k1",
            t_id=1,
            request_id=b"req",
            function_name="bad_func",
            partition=0,
            ack_payload=ack_payload,
            fallback_mode=False,
            use_fallback_cache=False,
            params=(),
            protocol=protocol,
        )
        assert result is False
        networking.abort_chain.assert_called_once()


# ---------------------------------------------------------------------------
# _send_response_to_root
# ---------------------------------------------------------------------------


class TestSendResponseToRoot:
    @pytest.mark.asyncio
    async def test_local_response(self):
        op, _, networking, _ = _setup_operator(same_network=True)
        await op._send_response_to_root("resp", "10.0.0.1", 6000, 1)
        networking.add_response.assert_called_once_with(1, "resp")

    @pytest.mark.asyncio
    async def test_remote_response(self):
        op, _, networking, _ = _setup_operator(same_network=False)
        await op._send_response_to_root("resp", "10.0.0.2", 6001, 1)
        networking.send_message.assert_called_once()
        call_kwargs = networking.send_message.call_args
        assert call_kwargs[1]["msg_type"] == MessageType.ResponseToRoot


# ---------------------------------------------------------------------------
# _send_chain_abort
# ---------------------------------------------------------------------------


class TestSendChainAbort:
    @pytest.mark.asyncio
    async def test_local_abort(self):
        op, _, networking, _ = _setup_operator(same_network=True)
        await op._send_chain_abort("error", "10.0.0.1", 6000, 1)
        networking.abort_chain.assert_called_once_with(1, "error")

    @pytest.mark.asyncio
    async def test_remote_abort(self):
        op, _, networking, _ = _setup_operator(same_network=False)
        await op._send_chain_abort("error", "10.0.0.2", 6001, 1)
        networking.send_message.assert_called_once()
        call_kwargs = networking.send_message.call_args
        assert call_kwargs[1]["msg_type"] == MessageType.ChainAbort


# ---------------------------------------------------------------------------
# set_n_partitions
# ---------------------------------------------------------------------------


class TestSetNPartitions:
    def test_updates_partitioner(self):
        op = _operator(n_partitions=2)
        op.set_n_partitions(4)
        assert op.n_partitions == 4


# ---------------------------------------------------------------------------
# Fallback cache ack
# ---------------------------------------------------------------------------


class TestFallbackCacheAck:
    @pytest.mark.asyncio
    async def test_chain_fallback_cache_sends_cache_ack_local(self):
        op, _state, networking, _graph = _setup_operator(same_network=True)
        protocol = _mock_protocol()
        ack_payload = ("10.0.0.1", 6000, 1, "1/1", [], 0)
        result = await op.run_function(
            key="k1",
            t_id=1,
            request_id=b"req",
            function_name="my_func",
            partition=0,
            ack_payload=ack_payload,
            fallback_mode=True,
            use_fallback_cache=True,
            params=(),
            protocol=protocol,
        )
        assert result is True
        networking.add_ack_cnt.assert_called_once()

    @pytest.mark.asyncio
    async def test_chain_fallback_cache_sends_cache_ack_remote(self):
        op, _state, networking, _graph = _setup_operator(same_network=False)
        protocol = _mock_protocol()
        ack_payload = ("10.0.0.2", 6001, 1, "1/1", [], 0)
        result = await op.run_function(
            key="k1",
            t_id=1,
            request_id=b"req",
            function_name="my_func",
            partition=0,
            ack_payload=ack_payload,
            fallback_mode=True,
            use_fallback_cache=True,
            params=(),
            protocol=protocol,
        )
        assert result is True
        # Should send both AckCache and ResponseToRoot (because resp is not None)
        assert networking.send_message.call_count >= 1
        msg_types = [c[1]["msg_type"] for c in networking.send_message.call_args_list]
        assert MessageType.AckCache in msg_types
