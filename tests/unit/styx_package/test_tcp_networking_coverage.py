"""Additional coverage tests for styx/common/tcp_networking.py

Covers: StyxSocketClient.create_connection, send_message retries, close paths,
SocketPool.create_socket_connections/close, NetworkingManager.send_message,
encode_message all branches, request_key, wait_for_remote_key_event.
"""

import asyncio
import struct
from unittest.mock import AsyncMock, MagicMock, patch

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
# Helpers
# ---------------------------------------------------------------------------


def _nm(port=6000):
    with patch("socket.gethostbyname", return_value="10.0.0.1"), patch("socket.gethostname", return_value="worker"):
        return NetworkingManager(host_port=port)


# ---------------------------------------------------------------------------
# StyxSocketClient.create_connection
# ---------------------------------------------------------------------------


class TestStyxSocketClientCreateConnection:
    @pytest.mark.asyncio
    async def test_successful_connection(self):
        client = StyxSocketClient()
        mock_reader = MagicMock()
        mock_writer = MagicMock()
        with patch("asyncio.open_connection", new_callable=AsyncMock, return_value=(mock_reader, mock_writer)):
            result = await client.create_connection("localhost", 8080)
        assert result is True
        assert client.reader is mock_reader
        assert client.writer is mock_writer
        assert client.target_host == "localhost"
        assert client.target_port == 8080

    @pytest.mark.asyncio
    async def test_oserror_retries_and_succeeds(self):
        client = StyxSocketClient()
        mock_reader = MagicMock()
        mock_writer = MagicMock()
        call_count = 0

        async def flaky_connect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                msg = "Connection refused"
                raise OSError(msg)
            return mock_reader, mock_writer

        with (
            patch("asyncio.open_connection", side_effect=flaky_connect),
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client.create_connection("localhost", 8080)
        assert result is True
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_max_retries_exhausted(self):
        client = StyxSocketClient()
        client.n_retries = 2

        async def always_fail(*args, **kwargs):
            msg = "Connection refused"
            raise OSError(msg)

        with patch("asyncio.open_connection", side_effect=always_fail), patch("asyncio.sleep", new_callable=AsyncMock):
            result = await client.create_connection("localhost", 8080)
        assert result is False


# ---------------------------------------------------------------------------
# StyxSocketClient.close
# ---------------------------------------------------------------------------


class TestStyxSocketClientClose:
    @pytest.mark.asyncio
    async def test_close_normal(self):
        client = StyxSocketClient()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        client.writer = mock_writer
        client.reader = MagicMock()

        await client.close()
        mock_writer.close.assert_called_once()
        assert client.reader is None
        assert client.writer is None

    @pytest.mark.asyncio
    async def test_close_when_no_writer(self):
        client = StyxSocketClient()
        await client.close()  # should not raise
        assert client.reader is None
        assert client.writer is None

    @pytest.mark.asyncio
    async def test_close_connection_reset(self):
        client = StyxSocketClient()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock(side_effect=ConnectionResetError)
        client.writer = mock_writer
        client.reader = MagicMock()

        await client.close()  # should handle gracefully
        assert client.reader is None
        assert client.writer is None


# ---------------------------------------------------------------------------
# SocketPool
# ---------------------------------------------------------------------------


class TestSocketPoolConnections:
    @pytest.mark.asyncio
    async def test_create_socket_connections(self):
        pool = SocketPool("localhost", 8080, size=2)
        with patch.object(StyxSocketClient, "create_connection", new_callable=AsyncMock, return_value=True):
            await pool.create_socket_connections()
        assert len(pool.conns) == 2

    @pytest.mark.asyncio
    async def test_close_pool(self):
        pool = SocketPool("localhost", 8080, size=2)
        c1, c2 = StyxSocketClient(), StyxSocketClient()
        c1.close = AsyncMock()
        c2.close = AsyncMock()
        pool.conns = [c1, c2]

        await pool.close()
        c1.close.assert_called_once()
        c2.close.assert_called_once()
        assert pool.conns == []


# ---------------------------------------------------------------------------
# NetworkingManager.send_message
# ---------------------------------------------------------------------------


class TestNetworkingManagerSendMessage:
    @pytest.mark.asyncio
    async def test_send_creates_pool_if_missing(self):
        nm = _nm()
        mock_socket = MagicMock()
        mock_socket.send_message = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.create_socket_connections = AsyncMock()
        mock_pool.__next__ = MagicMock(return_value=mock_socket)

        with patch.object(nm, "create_socket_connection", new_callable=AsyncMock) as mock_create:
            # First call creates, then we add pool
            async def create_side_effect(host, port):
                nm.pools[(host, port)] = mock_pool

            mock_create.side_effect = create_side_effect
            await nm.send_message("10.0.0.2", 6001, ("data",), MessageType.ClientMsg, Serializer.MSGPACK)

        mock_create.assert_called_once_with("10.0.0.2", 6001)
        mock_socket.send_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_reuses_existing_pool(self):
        nm = _nm()
        mock_socket = MagicMock()
        mock_socket.send_message = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.__next__ = MagicMock(return_value=mock_socket)
        nm.pools[("10.0.0.2", 6001)] = mock_pool

        await nm.send_message("10.0.0.2", 6001, ("data",), MessageType.ClientMsg, Serializer.MSGPACK)
        mock_socket.send_message.assert_called_once()


# ---------------------------------------------------------------------------
# NetworkingManager.close_all_connections
# ---------------------------------------------------------------------------


class TestNetworkingManagerCloseAll:
    @pytest.mark.asyncio
    async def test_closes_all_pools(self):
        nm = _nm()
        pool1 = MagicMock()
        pool1.close = AsyncMock()
        pool2 = MagicMock()
        pool2.close = AsyncMock()
        nm.pools = {("h1", 6000): pool1, ("h2", 6001): pool2}

        await nm.close_all_connections()
        pool1.close.assert_called_once()
        pool2.close.assert_called_once()
        assert nm.pools == {}


# ---------------------------------------------------------------------------
# NetworkingManager.close_worker_connections
# ---------------------------------------------------------------------------


class TestCloseWorkerConnections:
    @pytest.mark.asyncio
    async def test_closes_specific_pool(self):
        nm = _nm()
        pool = MagicMock()
        pool.close = AsyncMock()
        nm.pools[("h1", 6000)] = pool

        await nm.close_worker_connections("h1", 6000)
        pool.close.assert_called_once()
        assert ("h1", 6000) not in nm.pools

    @pytest.mark.asyncio
    async def test_missing_pool_no_error(self):
        nm = _nm()
        await nm.close_worker_connections("nonexistent", 9999)


# ---------------------------------------------------------------------------
# NetworkingManager.encode_message — all serializer branches
# ---------------------------------------------------------------------------


class TestNetworkingManagerEncodeAllBranches:
    def test_pickle_serializer(self):
        encoded = NetworkingManager.encode_message({"a": 1}, MessageType.ClientMsg, Serializer.PICKLE)
        (length,) = struct.unpack(">Q", encoded[:8])
        assert length == len(encoded) - 8
        assert encoded[9] == 2  # pickle id

    def test_compressed_msgpack_serializer(self):
        encoded = NetworkingManager.encode_message({"a": 1}, MessageType.ClientMsg, Serializer.COMPRESSED_MSGPACK)
        assert encoded[9] == 4  # compressed msgpack id

    def test_none_serializer(self):
        encoded = NetworkingManager.encode_message(b"raw", MessageType.ClientMsg, Serializer.NONE)
        assert encoded[9] == 3

    async def test_unsupported_raises(self):
        with pytest.raises(SerializerNotSupportedError):
            NetworkingManager.encode_message("x", MessageType.ClientMsg, 99)


# ---------------------------------------------------------------------------
# NetworkingManager.request_key
# ---------------------------------------------------------------------------


class TestRequestKey:
    @pytest.mark.asyncio
    async def test_request_key_sends_message(self):
        nm = _nm()
        nm.peers = {1: ("10.0.0.2", 5001, 6001)}
        nm.send_message = AsyncMock()

        await nm.request_key("users", 0, "k1", (1, 5))
        nm.send_message.assert_called_once()
        assert ("users", 0) in nm.wait_remote_key_event
        assert "k1" in nm.wait_remote_key_event[("users", 0)]

    @pytest.mark.asyncio
    async def test_request_key_none_worker_id_returns_early(self):
        nm = _nm()
        nm.send_message = AsyncMock()
        await nm.request_key("users", 0, "k1", None)
        nm.send_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_request_key_duplicate_returns_early(self):
        nm = _nm()
        nm.send_message = AsyncMock()
        nm.peers = {1: ("10.0.0.2", 5001, 6001)}
        nm.wait_remote_key_event[("users", 0)] = {"k1": asyncio.Event()}

        await nm.request_key("users", 0, "k1", (1, 5))
        nm.send_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_request_key_adds_to_existing_partition(self):
        nm = _nm()
        nm.peers = {1: ("10.0.0.2", 5001, 6001)}
        nm.send_message = AsyncMock()
        nm.wait_remote_key_event[("users", 0)] = {"k_other": asyncio.Event()}

        await nm.request_key("users", 0, "k_new", (1, 5))
        assert "k_new" in nm.wait_remote_key_event[("users", 0)]
        assert "k_other" in nm.wait_remote_key_event[("users", 0)]


# ---------------------------------------------------------------------------
# NetworkingManager.wait_for_remote_key_event
# ---------------------------------------------------------------------------


class TestWaitForRemoteKeyEvent:
    @pytest.mark.asyncio
    async def test_wait_and_cleanup(self):
        nm = _nm()
        ev = asyncio.Event()
        nm.wait_remote_key_event[("users", 0)] = {"k1": ev}

        ev.set()
        await nm.wait_for_remote_key_event("users", 0, "k1")
        # Should be cleaned up
        assert ("users", 0) not in nm.wait_remote_key_event

    @pytest.mark.asyncio
    async def test_wait_partial_cleanup(self):
        nm = _nm()
        ev1 = asyncio.Event()
        ev2 = asyncio.Event()
        nm.wait_remote_key_event[("users", 0)] = {"k1": ev1, "k2": ev2}

        ev1.set()
        await nm.wait_for_remote_key_event("users", 0, "k1")
        # Only k1 cleaned up, k2 remains
        assert "k2" in nm.wait_remote_key_event[("users", 0)]
        assert "k1" not in nm.wait_remote_key_event[("users", 0)]


# ---------------------------------------------------------------------------
# NetworkingManager.send_message_request_response
# ---------------------------------------------------------------------------


class TestSendMessageRequestResponse:
    @pytest.mark.asyncio
    async def test_successful_rq_rs(self):
        nm = _nm()
        mock_socket = MagicMock()
        # Return valid encoded response
        from styx.common.base_networking import BaseNetworking

        response_data = BaseNetworking.encode_message(("ok",), MessageType.ClientMsg, Serializer.CLOUDPICKLE)
        mock_socket.send_message_rq_rs = AsyncMock(return_value=response_data)

        mock_pool = MagicMock()
        mock_pool.__next__ = MagicMock(return_value=mock_socket)
        mock_pool.create_socket_connections = AsyncMock()
        nm.pools[("10.0.0.2", 6001)] = mock_pool

        result = await nm.send_message_request_response(
            "10.0.0.2", 6001, ("hello",), MessageType.ClientMsg, Serializer.CLOUDPICKLE
        )
        assert result == ("ok",)

    @pytest.mark.asyncio
    async def test_none_response_raises(self):
        nm = _nm()
        mock_socket = MagicMock()
        mock_socket.send_message_rq_rs = AsyncMock(return_value=None)

        mock_pool = MagicMock()
        mock_pool.__next__ = MagicMock(return_value=mock_socket)
        nm.pools[("10.0.0.2", 6001)] = mock_pool

        with pytest.raises(ConnectionError):
            await nm.send_message_request_response("10.0.0.2", 6001, ("hello",), MessageType.ClientMsg)
