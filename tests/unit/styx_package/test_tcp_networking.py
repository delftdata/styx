"""Unit tests for styx/common/tcp_networking.py"""

import struct
from unittest.mock import patch

import pytest
from styx.common.exceptions import SerializerNotSupportedError
from styx.common.message_types import MessageType
from styx.common.serialization import Serializer
from styx.common.tcp_networking import (
    NetworkingManager,
    SocketPool,
    StyxSocketClient,
)

# ---------------------------------------------------------------------------
# StyxSocketClient
# ---------------------------------------------------------------------------


class TestStyxSocketClient:
    def test_init_defaults(self):
        c = StyxSocketClient()
        assert c.reader is None
        assert c.writer is None
        assert c.target_host is None
        assert c.target_port is None
        assert c.n_retries == 3

    def test_send_message_raises_when_no_connection(self):
        c = StyxSocketClient()
        with pytest.raises(ConnectionError, match="Writer is not initialized"):
            import asyncio

            asyncio.run(c.send_message(b"test"))

    def test_send_message_rq_rs_raises_when_no_connection(self):
        c = StyxSocketClient()
        with pytest.raises(ConnectionError, match="Reader/Writer not initialized"):
            import asyncio

            asyncio.run(c.send_message_rq_rs(b"test"))


# ---------------------------------------------------------------------------
# SocketPool
# ---------------------------------------------------------------------------


class TestSocketPool:
    def test_init(self):
        pool = SocketPool("localhost", 8080, size=2)
        assert pool.host == "localhost"
        assert pool.port == 8080
        assert pool.size == 2
        assert pool.conns == []
        assert pool.index == 0

    def test_round_robin(self):
        pool = SocketPool("localhost", 8080, size=3)
        c1, c2, c3 = StyxSocketClient(), StyxSocketClient(), StyxSocketClient()
        pool.conns = [c1, c2, c3]

        assert next(pool) is c1
        assert next(pool) is c2
        assert next(pool) is c3
        assert next(pool) is c1  # wraps around

    def test_iter_returns_self(self):
        pool = SocketPool("localhost", 8080)
        assert iter(pool) is pool


# ---------------------------------------------------------------------------
# NetworkingManager
# ---------------------------------------------------------------------------


class TestNetworkingManager:
    def test_init(self):
        with patch("socket.gethostbyname", return_value="10.0.0.1"), patch("socket.gethostname", return_value="worker"):
            nm = NetworkingManager(host_port=6000, size=2)
        assert nm.host_port == 6000
        assert nm.socket_pool_size == 2
        assert nm.pools == {}
        assert nm.peers == {}
        assert nm.wait_remote_key_event == {}

    def test_set_peers(self):
        with patch("socket.gethostbyname", return_value="10.0.0.1"), patch("socket.gethostname", return_value="worker"):
            nm = NetworkingManager(host_port=6000)
        peers = {0: ("host1", 5000, 6000), 1: ("host2", 5001, 6001)}
        nm.set_peers(peers)
        assert nm.peers == peers

    @pytest.mark.asyncio
    async def test_close_all_connections_empty(self):
        with patch("socket.gethostbyname", return_value="10.0.0.1"), patch("socket.gethostname", return_value="worker"):
            nm = NetworkingManager(host_port=6000)
        await nm.close_all_connections()
        assert nm.pools == {}

    @pytest.mark.asyncio
    async def test_key_received_sets_event(self):
        import asyncio

        with patch("socket.gethostbyname", return_value="10.0.0.1"), patch("socket.gethostname", return_value="worker"):
            nm = NetworkingManager(host_port=6000)
        ev = asyncio.Event()
        nm.wait_remote_key_event[("op", 0)] = {"key1": ev}
        nm.key_received(["op", 0], "key1")
        assert ev.is_set()


# ---------------------------------------------------------------------------
# NetworkingManager.encode_message (static)
# ---------------------------------------------------------------------------


class TestNetworkingManagerEncode:
    def test_cloudpickle_format(self):
        msg = ("hello",)
        encoded = NetworkingManager.encode_message(msg, MessageType.ClientMsg, Serializer.CLOUDPICKLE)
        # 8 bytes length + 1 byte msg_type + 1 byte serializer_id + payload
        (length,) = struct.unpack(">Q", encoded[:8])
        assert length == len(encoded) - 8
        assert encoded[8] == MessageType.ClientMsg
        assert encoded[9] == 0  # cloudpickle serializer id

    def test_msgpack_format(self):
        msg = ("small",)
        encoded = NetworkingManager.encode_message(msg, MessageType.ClientMsg, Serializer.MSGPACK)
        assert encoded[8] == MessageType.ClientMsg
        # serializer id is 1 for msgpack (or 4 if compressed, but small msg shouldn't compress)
        assert encoded[9] in (1, 4)

    def test_none_serializer(self):
        msg = b"raw"
        encoded = NetworkingManager.encode_message(msg, MessageType.SnapMarker, Serializer.NONE)
        assert encoded[9] == 3  # NONE serializer id

    async def test_unsupported_raises(self):
        with pytest.raises(SerializerNotSupportedError):
            NetworkingManager.encode_message("x", MessageType.ClientMsg, 99)
