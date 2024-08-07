import unittest
import zmq
from unittest.mock import Mock, patch, AsyncMock

from styx.common.networking import MessagingMode, SocketPool


class TestNetworking(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.host = "host"
        self.port = 0
        self.socket_pool = SocketPool(self.host, self.port)

    async def test_init(self):
        assert self.socket_pool.zmq_socket_type == zmq.DEALER  # Default
        socket_pool_a = SocketPool(
            self.host, self.port, mode=MessagingMode.PROTOCOL_PROTOCOL)
        assert socket_pool_a.zmq_socket_type == zmq.PUSH

    @patch("styx.common.networking.create_zmq_stream")
    async def test_create_socket_connections(self, create_zmq_stream):
        soc = Mock()
        create_zmq_stream.return_value = soc
        await self.socket_pool.create_socket_connections()
        assert len(self.socket_pool.conns) == self.socket_pool.size
        soc.transport.setsockopt.assert_called()
        assert soc.transport.setsockopt.call_count == self.socket_pool.size

    @patch("styx.common.networking.create_zmq_stream", AsyncMock(return_value=Mock()))
    @patch("styx.common.networking.asyncio.Lock")
    async def test_socket_pool_close(self, Lock):
        lock = Mock()
        lock.locked = Mock(return_value=True)
        Lock.return_value = lock
        await self.socket_pool.create_socket_connections()
        conns = self.socket_pool.conns[:]
        self.socket_pool.close()
        assert len(self.socket_pool.conns) == 0
        for conn in conns:
            conn.zmq_socket.close.assert_called()
            conn.socket_lock.release.assert_called()

        # Don't release when not locked
        lock.locked = Mock(return_value=False)
        await self.socket_pool.create_socket_connections()
        conns = self.socket_pool.conns[:]
        self.socket_pool.close()
        for conn in conns:
            conn.zmq_socket.release.assert_not_called()

    @patch("styx.common.networking.asyncio.Lock")
    async def test_iteration_cyclic(self, _):
        await self.socket_pool.create_socket_connections()
        count = 0
        first_conn = next(self.socket_pool)
        also_first_conn = None
        for conn in self.socket_pool:
            count += 1
            if count == self.socket_pool.size:
                also_first_conn = conn
                break
        assert first_conn is also_first_conn
