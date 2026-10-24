"""A transaction must not read a migrating key as missing while it is in a batch.

When a destination asks the source for a key (RequestRemoteKey) after the async
sender already took that key into a batch, the source answers None. The waiting
transaction must keep waiting for the batch instead of running without the key.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

from styx.common.message_types import MessageType
from styx.common.serialization import Serializer
from styx.common.tcp_networking import NetworkingManager

from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.worker_service import Worker

OP = "op"
DST = (OP, 1)


def _receive_remote_key_msg(key, found, value) -> bytes:
    # Drop the 8-byte length prefix, as the receiving TCP reader does.
    return NetworkingManager.encode_message(
        msg=(DST, key, found, value),
        msg_type=MessageType.ReceiveRemoteKey,
        serializer=Serializer.MSGPACK,
    )[8:]


def _worker():
    w = Worker.__new__(Worker)
    w.networking = SimpleNamespace(decode_message=NetworkingManager.decode_message)
    w.protocol_networking = MagicMock()
    w.protocol_networking.wait_remote_key_event = {DST: {"k1": asyncio.Event()}}
    w.local_state = InMemoryOperatorState({DST})
    w.local_state.add_remote_keys(DST, {"k1": (2, 0)})
    return w


class TestRemoteKeyResponse:
    async def test_not_found_response_leaves_waiter_for_the_batch(self):
        w = _worker()
        await w._handle_receive_remote_key(_receive_remote_key_msg("k1", False, None), MessageType.ReceiveRemoteKey)
        w.protocol_networking.key_received.assert_not_called()
        assert w.local_state.in_remote_keys("k1", OP, 1)

    async def test_value_response_installs_key_and_wakes_waiter(self):
        w = _worker()
        await w._handle_receive_remote_key(_receive_remote_key_msg("k1", True, "v1"), MessageType.ReceiveRemoteKey)
        w.protocol_networking.key_received.assert_called_once_with(DST, "k1")
        assert w.local_state.data[DST]["k1"] == "v1"
        assert not w.local_state.in_remote_keys("k1", OP, 1)

    async def test_found_none_value_installs_key_and_wakes_waiter(self):
        w = _worker()
        await w._handle_receive_remote_key(_receive_remote_key_msg("k1", True, None), MessageType.ReceiveRemoteKey)
        w.protocol_networking.key_received.assert_called_once_with(DST, "k1")
        assert w.local_state.data[DST] == {"k1": None}
        assert not w.local_state.in_remote_keys("k1", OP, 1)
