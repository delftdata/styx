"""Unit tests for styx/common/base_networking.py

Note: many BaseNetworking methods use asyncio.Event or trigger aiologger,
which require a running event loop.  We make those tests ``async`` so that
pytest-asyncio (asyncio_mode = auto) provides a loop automatically.
"""

import asyncio
import fractions
from unittest.mock import MagicMock, patch

import pytest
from styx.common.base_networking import BaseNetworking, MessagingMode
from styx.common.exceptions import SerializerNotSupportedError
from styx.common.message_types import MessageType
from styx.common.run_func_payload import RunFuncPayload
from styx.common.serialization import Serializer

# ---------------------------------------------------------------------------
# Concrete subclass for testing abstract methods
# ---------------------------------------------------------------------------


class ConcreteNetworking(BaseNetworking):
    async def close_all_connections(self):
        pass

    async def send_message(self, host, port, msg, msg_type, serializer=Serializer.CLOUDPICKLE):
        pass

    async def send_message_request_response(self, host, port, msg, msg_type, serializer=Serializer.CLOUDPICKLE):
        pass


def _net(port=6000, mode=MessagingMode.WORKER_COR):
    with patch("socket.gethostbyname", return_value="10.0.0.1"), patch("socket.gethostname", return_value="worker-0"):
        return ConcreteNetworking(port, mode)


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestBaseNetworkingInit:
    def test_host_port_stored(self):
        n = _net(port=7000)
        assert n.host_port == 7000

    def test_host_name_resolved(self):
        n = _net()
        assert n.host_name == "10.0.0.1"

    def test_default_worker_id(self):
        n = _net()
        assert n.worker_id == -1

    def test_messaging_mode(self):
        n = _net(mode=MessagingMode.PROTOCOL_PROTOCOL)
        assert n.messaging_mode == MessagingMode.PROTOCOL_PROTOCOL

    def test_empty_collections(self):
        n = _net()
        assert n.waited_ack_events == {}
        assert n.ack_fraction == {}
        assert n.ack_cnts == {}
        assert len(n.aborted_events) == 0
        assert len(n.logic_aborts_everywhere) == 0


# ---------------------------------------------------------------------------
# in_the_same_network
# ---------------------------------------------------------------------------


class TestInTheSameNetwork:
    def test_same_host_and_port(self):
        n = _net(port=6000)
        assert n.in_the_same_network("10.0.0.1", 6000) is True

    def test_different_host(self):
        n = _net(port=6000)
        assert n.in_the_same_network("10.0.0.2", 6000) is False

    def test_different_port(self):
        n = _net(port=6000)
        assert n.in_the_same_network("10.0.0.1", 7000) is False


# ---------------------------------------------------------------------------
# set_worker_id
# ---------------------------------------------------------------------------


class TestSetWorkerId:
    def test_sets_worker_id(self):
        n = _net()
        n.set_worker_id(5)
        assert n.worker_id == 5


# ---------------------------------------------------------------------------
# cleanup_after_epoch  (needs loop for asyncio.Event)
# ---------------------------------------------------------------------------


class TestCleanupAfterEpoch:
    async def test_clears_all_dicts(self):
        n = _net()
        n.waited_ack_events[1] = asyncio.Event()
        n.ack_fraction[1] = fractions.Fraction(1, 2)
        n.ack_cnts[1] = (1, 2)
        n.aborted_events[1] = "err"
        n.remote_function_calls[1] = [MagicMock()]
        n.logic_aborts_everywhere.add(1)
        n.chain_participants[1] = [2]
        n.client_responses[1] = "resp"

        n.cleanup_after_epoch()

        assert len(n.waited_ack_events) == 0
        assert len(n.ack_fraction) == 0
        assert len(n.ack_cnts) == 0
        assert len(n.aborted_events) == 0
        assert len(n.remote_function_calls) == 0
        assert len(n.logic_aborts_everywhere) == 0
        assert len(n.chain_participants) == 0
        assert len(n.client_responses) == 0


# ---------------------------------------------------------------------------
# add_remote_function_call
# ---------------------------------------------------------------------------


class TestAddRemoteFunctionCall:
    def test_appends_payload(self):
        n = _net()
        p = RunFuncPayload(request_id=b"r", key="k", operator_name="op", partition=0, function_name="fn", params=())
        n.add_remote_function_call(1, p)
        assert len(n.remote_function_calls[1]) == 1
        assert n.remote_function_calls[1][0] is p

    def test_multiple_payloads(self):
        n = _net()
        p1 = RunFuncPayload(request_id=b"r1", key="k1", operator_name="op", partition=0, function_name="fn", params=())
        p2 = RunFuncPayload(request_id=b"r2", key="k2", operator_name="op", partition=0, function_name="fn", params=())
        n.add_remote_function_call(1, p1)
        n.add_remote_function_call(1, p2)
        assert len(n.remote_function_calls[1]) == 2


# ---------------------------------------------------------------------------
# prepare_function_chain / ack_fraction / ack_cnt  (needs loop)
# ---------------------------------------------------------------------------


class TestFunctionChain:
    async def test_prepare_creates_event_and_fraction(self):
        n = _net()
        n.prepare_function_chain(10)
        assert 10 in n.waited_ack_events
        assert isinstance(n.waited_ack_events[10], asyncio.Event)
        assert n.ack_fraction[10] == fractions.Fraction(0)
        assert n.ack_cnts[10] == (0, 0)

    async def test_add_ack_fraction_completes_at_1(self):
        n = _net()
        n.prepare_function_chain(10)
        n.add_ack_fraction_str(10, "1/2", [], 0)
        assert not n.waited_ack_events[10].is_set()
        n.add_ack_fraction_str(10, "1/2", [], 0)
        assert n.waited_ack_events[10].is_set()

    async def test_add_ack_fraction_skips_aborted(self):
        n = _net()
        n.prepare_function_chain(10)
        n.aborted_events[10] = "err"
        n.add_ack_fraction_str(10, "1", [], 0)
        # Should not set the event (aborted early return)
        assert n.ack_fraction[10] == fractions.Fraction(0)

    async def test_add_ack_cnt(self):
        n = _net()
        n.prepare_function_chain(10)
        n.ack_cnts[10] = (0, 2)
        n.add_ack_cnt(10, 1)
        assert n.ack_cnts[10] == (1, 2)
        assert not n.waited_ack_events[10].is_set()
        n.add_ack_cnt(10, 1)
        assert n.waited_ack_events[10].is_set()

    async def test_add_ack_cnt_skips_aborted(self):
        n = _net()
        n.prepare_function_chain(10)
        n.aborted_events[10] = "err"
        n.add_ack_cnt(10, 1)
        assert n.ack_cnts[10] == (0, 0)


# ---------------------------------------------------------------------------
# reset_ack_for_fallback  (needs loop)
# ---------------------------------------------------------------------------


class TestResetAck:
    async def test_reset_for_fallback(self):
        n = _net()
        n.prepare_function_chain(10)
        n.waited_ack_events[10].set()
        n.ack_fraction[10] = fractions.Fraction(1)
        n.reset_ack_for_fallback(10)
        assert not n.waited_ack_events[10].is_set()
        assert n.ack_fraction[10] == fractions.Fraction(0)

    def test_reset_for_fallback_missing_id(self):
        n = _net()
        n.reset_ack_for_fallback(999)  # should not raise

    async def test_reset_for_fallback_cache(self):
        n = _net()
        n.prepare_function_chain(10)
        n.waited_ack_events[10].set()
        n.reset_ack_for_fallback_cache(10)
        assert not n.waited_ack_events[10].is_set()
        # fraction NOT reset in cache mode
        assert n.ack_fraction[10] == fractions.Fraction(0)


# ---------------------------------------------------------------------------
# abort_chain  (needs loop)
# ---------------------------------------------------------------------------


class TestAbortChain:
    async def test_abort_sets_event(self):
        n = _net()
        n.prepare_function_chain(10)
        n.abort_chain(10, "error_msg")
        assert n.waited_ack_events[10].is_set()
        assert n.aborted_events[10] == "error_msg"
        assert 10 in n.logic_aborts_everywhere

    async def test_abort_idempotent(self):
        n = _net()
        n.prepare_function_chain(10)
        n.abort_chain(10, "first")
        n.abort_chain(10, "second")
        assert n.aborted_events[10] == "first"

    def test_abort_without_prepared_chain(self):
        n = _net()
        n.abort_chain(10, "err")
        assert n.aborted_events[10] == "err"
        assert 10 in n.logic_aborts_everywhere


# ---------------------------------------------------------------------------
# add_response  (needs loop for aiologger)
# ---------------------------------------------------------------------------


class TestAddResponse:
    async def test_stores_response(self):
        n = _net()
        n.add_response(1, "result")
        assert n.client_responses[1] == "result"


# ---------------------------------------------------------------------------
# add_chain_participants
# ---------------------------------------------------------------------------


class TestAddChainParticipants:
    def test_adds_non_self_participants(self):
        n = _net()
        n.set_worker_id(0)
        n.add_chain_participants(1, [0, 1, 2])
        assert 0 not in n.chain_participants[1]
        assert 1 in n.chain_participants[1]
        assert 2 in n.chain_participants[1]

    def test_no_duplicates(self):
        n = _net()
        n.set_worker_id(0)
        n.add_chain_participants(1, [1, 2])
        n.add_chain_participants(1, [1, 3])
        assert n.chain_participants[1].count(1) == 1
        assert 3 in n.chain_participants[1]


# ---------------------------------------------------------------------------
# merge_remote_logic_aborts
# ---------------------------------------------------------------------------


class TestMergeRemoteLogicAborts:
    def test_merges_sets(self):
        n = _net()
        n.logic_aborts_everywhere = {1, 2}
        n.merge_remote_logic_aborts({2, 3, 4})
        assert n.logic_aborts_everywhere == {1, 2, 3, 4}


# ---------------------------------------------------------------------------
# clear_aborted_events_for_fallback
# ---------------------------------------------------------------------------


class TestClearAbortedForFallback:
    def test_clears_three_dicts(self):
        n = _net()
        n.aborted_events[1] = "err"
        n.logic_aborts_everywhere.add(1)
        n.client_responses[1] = "resp"
        n.clear_aborted_events_for_fallback()
        assert len(n.aborted_events) == 0
        assert len(n.logic_aborts_everywhere) == 0
        assert len(n.client_responses) == 0


# ---------------------------------------------------------------------------
# encode_message / decode_message / get_msg_type  (static, no loop needed)
# ---------------------------------------------------------------------------


class TestEncodeDecodeMessage:
    """BaseNetworking.encode_message returns: msg_type(1B) + serializer_id(1B) + payload.
    No 8-byte length prefix (that's added by NetworkingManager.encode_message).
    """

    def test_cloudpickle_roundtrip(self):
        msg = ("hello", 42)
        encoded = BaseNetworking.encode_message(msg, MessageType.ClientMsg, Serializer.CLOUDPICKLE)
        assert len(encoded) > 4
        msg_type = BaseNetworking.get_msg_type(encoded)
        assert msg_type == MessageType.ClientMsg
        decoded = BaseNetworking.decode_message(encoded)
        assert decoded == ("hello", 42)

    def test_msgpack_roundtrip(self):
        msg = ("test", [1, 2, 3])
        encoded = BaseNetworking.encode_message(msg, MessageType.RunFunRemote, Serializer.MSGPACK)
        msg_type = BaseNetworking.get_msg_type(encoded)
        assert msg_type == MessageType.RunFunRemote
        decoded = BaseNetworking.decode_message(encoded)
        assert decoded == ["test", [1, 2, 3]]  # msgpack converts tuples to lists

    def test_none_serializer_passthrough(self):
        raw = b"raw_bytes"
        encoded = BaseNetworking.encode_message(raw, MessageType.SnapMarker, Serializer.NONE)
        decoded = BaseNetworking.decode_message(encoded)
        assert decoded == raw

    async def test_unsupported_serializer_raises(self):
        with pytest.raises(SerializerNotSupportedError):
            BaseNetworking.encode_message("x", MessageType.ClientMsg, 99)

    def test_first_byte_is_msg_type(self):
        msg = "hello"
        encoded = BaseNetworking.encode_message(msg, MessageType.ClientMsg, Serializer.CLOUDPICKLE)
        assert encoded[0] == MessageType.ClientMsg

    def test_second_byte_is_serializer_id(self):
        msg = "hello"
        encoded = BaseNetworking.encode_message(msg, MessageType.ClientMsg, Serializer.CLOUDPICKLE)
        assert encoded[1] == 0  # cloudpickle = 0

    def test_compressed_msgpack_roundtrip(self):
        msg = {"key": "value", "num": 42}
        encoded = BaseNetworking.encode_message(msg, MessageType.ClientMsg, Serializer.COMPRESSED_MSGPACK)
        decoded = BaseNetworking.decode_message(encoded)
        assert decoded == msg


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


class TestAddAckFractionStrEdgeCases:
    async def test_fraction_exceeds_one_logs_error(self):
        n = _net()
        n.prepare_function_chain(10)
        n.add_ack_fraction_str(10, "1/2", [], 0)
        n.add_ack_fraction_str(10, "3/4", [], 0)
        # fraction is 1/2 + 3/4 = 5/4 > 1, should log but not crash
        assert n.ack_fraction[10] > 1

    async def test_fraction_key_error_on_missing_tid(self):
        n = _net()
        # Don't prepare chain — ack_fraction[99] does not exist
        n.add_ack_fraction_str(99, "1/2", [], 0)
        # Should not raise; handled internally via KeyError

    async def test_partial_node_count_accumulates(self):
        n = _net()
        n.prepare_function_chain(10)
        n.add_ack_fraction_str(10, "1/4", [1], 2)
        assert n.ack_cnts[10] == (0, 2)
        n.add_ack_fraction_str(10, "1/4", [2], 3)
        assert n.ack_cnts[10] == (0, 5)


class TestAddAckCntEdgeCases:
    async def test_cnt_exceeds_total_logs_error(self):
        n = _net()
        n.prepare_function_chain(10)
        n.ack_cnts[10] = (0, 1)
        n.add_ack_cnt(10, 1)
        assert n.waited_ack_events[10].is_set()
        # Adding more exceeds total
        n.waited_ack_events[10].clear()
        n.add_ack_cnt(10, 1)
        assert n.ack_cnts[10] == (2, 1)  # 2 > 1

    async def test_cnt_key_error_on_missing_tid(self):
        n = _net()
        n.add_ack_cnt(99, 1)  # should not raise


class TestAddResponseNone:
    async def test_none_response_stored_with_logging(self):
        n = _net()
        n.add_response(1, None)
        assert n.client_responses[1] is None


class TestEncodeDecodePickle:
    def test_pickle_roundtrip(self):
        msg = {"key": [1, 2, 3]}
        encoded = BaseNetworking.encode_message(msg, MessageType.ClientMsg, Serializer.PICKLE)
        assert encoded[1] == 2  # pickle serializer id
        decoded = BaseNetworking.decode_message(encoded)
        assert decoded == msg

    async def test_decode_unsupported_serializer_raises(self):
        # Craft a message with an invalid serializer id
        import struct

        data = struct.pack(">B", MessageType.ClientMsg) + struct.pack(">B", 99) + b"payload"
        with pytest.raises(SerializerNotSupportedError):
            BaseNetworking.decode_message(data)


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_format(self):
        n = _net(port=6000)
        r = repr(n)
        assert "ConcreteNetworking" in r
        assert "10.0.0.1:6000" in r
        assert "WORKER_COR" in r
