import unittest
import socket
import asyncio
import fractions

from unittest.mock import Mock, patch, AsyncMock

from styx.common.exceptions import SerializerNotSupported
from styx.common.networking import NetworkingManager, SocketConnection
from styx.common.run_func_payload import RunFuncPayload
from styx.common.serialization import Serializer


class SocketPoolMock(Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.close = Mock()
        self.create_socket_connections = AsyncMock()
        self.__socket_connection = SocketConnectionMock()

    def __iter__(self):
        return self

    def __next__(self) -> SocketConnection:
        return self.__socket_connection


class SocketConnectionMock(Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.zmq_socket = Mock()
        self.zmq_socket.read = AsyncMock()
        self.socket_lock = asyncio.Lock()


class TestNetworking(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.networking = NetworkingManager(0)

    async def test_in_the_same_network(self):
        host = str(socket.gethostbyname(socket.gethostname()))
        assert self.networking.in_the_same_network(host, 0)
        assert not self.networking.in_the_same_network(host, 1)
        assert not self.networking.in_the_same_network("placeholder", 0)

    async def test_set_worker_id(self):
        self.networking.set_worker_id(0)
        assert self.networking.worker_id == 0

    async def test_cleanup_after_epoch(self):
        self.networking.prepare_function_chain(0)
        self.networking.prepare_function_chain(1)
        assert len(self.networking.ack_cnts) > 0

        self.networking.cleanup_after_epoch()

        attrs_that_should_reset = [
            self.networking.waited_ack_events,
            self.networking.ack_fraction,
            self.networking.ack_cnts,
            self.networking.aborted_events,
            self.networking.remote_function_calls,
            self.networking.logic_aborts_everywhere,
            self.networking.chain_participants
        ]

        assert all([len(x) == 0 for x in attrs_that_should_reset])

    async def test_add_remote_function_call(self):
        t_id = 0
        self.networking.add_remote_function_call(t_id, Mock(RunFuncPayload))
        assert t_id in self.networking.remote_function_calls and len(
            self.networking.remote_function_calls[t_id]) == 1
        self.networking.add_remote_function_call(t_id, Mock(RunFuncPayload))
        assert len(self.networking.remote_function_calls[t_id]) == 2

    async def test_add_chain_participants(self):
        self.networking.prepare_function_chain(0)
        self.networking.prepare_function_chain(1)

        # Empty list doesn't add chain participants
        self.networking.add_chain_participants(0, [])
        assert len(self.networking.chain_participants[0]) == 0

        # Non-empty list adds chain participants
        self.networking.add_chain_participants(0, [0])
        assert len(self.networking.chain_participants[0]) == 1

        # Non-empty list with a different chain participants adds it
        self.networking.add_chain_participants(0, [1])
        assert len(self.networking.chain_participants[0]) == 2

        # Non-empty list with a chain participant that was already in the list doesn't add it
        self.networking.add_chain_participants(0, [1])
        assert len(self.networking.chain_participants[0]) == 2

        # Don't add anything if chain part is equal to worker id
        self.networking.add_chain_participants(0, [-1])
        assert len(self.networking.chain_participants[0]) == 2

    async def test_prepare_function_chain(self):
        t_id = 0
        self.networking.prepare_function_chain(t_id)
        assert t_id in self.networking.waited_ack_events and type(
            self.networking.waited_ack_events[t_id]) is asyncio.Event
        assert t_id in self.networking.ack_fraction and self.networking.ack_fraction[t_id] == fractions.Fraction(0)
        assert t_id in self.networking.ack_cnts and self.networking.ack_cnts[t_id] == (0, 0)
        assert t_id in self.networking.chain_participants and len(
            self.networking.chain_participants[t_id]) == 0

    async def test_add_ack_fraction_str(self):
        self.networking.add_chain_participants = Mock(
            self.networking.add_chain_participants)
        t_id = 0
        self.networking.prepare_function_chain(t_id)
        self.networking.add_ack_fraction_str(t_id, "2/3", [], 0)

        self.networking.add_chain_participants.assert_called_once()
        assert t_id in self.networking.ack_cnts and len(
            self.networking.ack_cnts[0]) == 2

    def test_add_ack_fraction_str_aborted(self):
        try:
            self.networking.add_chain_participants = Mock(
                self.networking.add_chain_participants)
            t_id = 0
            self.networking.prepare_function_chain(t_id)
            self.networking.abort_chain(t_id, "ERROR", b'')
            self.networking.add_ack_fraction_str(t_id, "2/3", [], 0)
            self.networking.add_chain_participants.assert_not_called()
        except KeyError:
            self.fail("Should have returned before any key error could have occurred.")

    async def test_add_ack_fraction_str_all_gathered(self):
        self.networking.add_chain_participants = Mock(
            self.networking.add_chain_participants)
        t_id = 0
        self.networking.prepare_function_chain(t_id)
        self.networking.add_ack_fraction_str(t_id, "1", [], 0)
        assert self.networking.waited_ack_events[t_id].is_set()

    async def test_add_ack_fraction_str_overflow(self):
        self.networking.add_chain_participants = Mock(
            self.networking.add_chain_participants)
        t_id = 0
        self.networking.prepare_function_chain(t_id)

        # Fraction larger than 1 (multiple instances of same function have run)
        with patch('styx.common.networking.logging') as logging_mock:
            self.networking.add_ack_fraction_str(t_id, "2", [], 0)
            logging_mock.error.assert_called_once()

    async def test_add_ack_fraction_str_no_preparation(self):
        self.networking.add_chain_participants = Mock(
            self.networking.add_chain_participants)
        t_id = 0
        with patch('styx.common.networking.logging') as logging_mock:
            self.networking.add_ack_fraction_str(t_id, "2/3", [], 0)
            logging_mock.error.assert_called_once()

    async def test_add_ack_cnt(self):
        self.networking.add_chain_participants = Mock(
            self.networking.add_chain_participants)
        t_id = 0
        self.networking.prepare_function_chain(t_id)
        self.networking.add_ack_fraction_str(t_id, "2/3", [], 2)

        self.networking.add_ack_cnt(t_id)
        assert self.networking.ack_cnts[t_id][0] == 1

    def test_add_ack_cnt_aborted(self):
        try:
            self.networking.add_chain_participants = Mock(
                self.networking.add_chain_participants)
            t_id = 0
            self.networking.prepare_function_chain(t_id)
            self.networking.abort_chain(t_id, "ERROR", b'')
            self.networking.add_ack_cnt(t_id)
        except KeyError:
            self.fail(
                "Should have returned before any key error could have occurred.")

    async def test_add_ack_cnt_complete(self):
        self.networking.add_chain_participants = Mock(
            self.networking.add_chain_participants)
        t_id = 0
        self.networking.prepare_function_chain(t_id)
        self.networking.add_ack_fraction_str(t_id, "2/3", [], 1)

        self.networking.add_ack_cnt(t_id)
        assert self.networking.waited_ack_events[t_id].is_set()

    async def test_add_ack_cnt_more_acks_than_partial_nodes(self):
        t_id = 0
        self.networking.prepare_function_chain(t_id)

        # More acks than nodes
        with patch('styx.common.networking.logging') as logging_mock:
            self.networking.add_ack_cnt(t_id)
            logging_mock.error.assert_called_once()

    async def test_add_ack_cnt_no_preparation(self):
        t_id = 0
        with patch('styx.common.networking.logging') as logging_mock:
            self.networking.add_ack_cnt(t_id)
            logging_mock.error.assert_called_once()

    async def test_reset_ack_for_fallback(self):
        t_id = 0
        self.networking.prepare_function_chain(t_id)
        self.networking.abort_chain(t_id, "ERROR", b'')
        self.networking.add_ack_fraction_str(t_id, "2/3", [], 1)
        self.networking.add_ack_cnt(t_id)
        self.networking.reset_ack_for_fallback(t_id)
        assert not self.networking.waited_ack_events[t_id].is_set()
        assert self.networking.ack_fraction[t_id] == fractions.Fraction(0)

    async def test_reset_ack_for_fallback_no_event(self):
        t_id = 0
        self.networking.reset_ack_for_fallback(t_id)
        assert t_id not in self.networking.waited_ack_events
        assert t_id not in self.networking.ack_fraction

    async def test_clear_aborted_events_for_fallback(self):
        t_id_0 = 0
        t_id_1 = 1
        self.networking.prepare_function_chain(t_id_0)
        self.networking.prepare_function_chain(t_id_1)
        self.networking.clear_aborted_events_for_fallback()
        assert len(self.networking.aborted_events) == 0

    @patch('styx.common.networking.SocketPool', SocketPoolMock)
    async def test_close_all_connections(self):
        await self.networking.create_socket_connection("host1", 0)
        await self.networking.create_socket_connection("host2", 0)
        pools_copy = self.networking.pools.copy()
        await self.networking.close_all_connections()

        for pool in pools_copy:
            pools_copy[pool].close.assert_called_once()

        assert len(self.networking.pools) == 0

    @patch('styx.common.networking.SocketPool', SocketPoolMock)
    async def test_create_socket_connection(self):
        host_port_tuple = "host", 0
        await self.networking.create_socket_connection(*host_port_tuple)
        assert host_port_tuple in self.networking.pools
        self.networking.pools[host_port_tuple].create_socket_connections.assert_called_once()

    @patch('styx.common.networking.SocketPool', SocketPoolMock)
    async def test_close_socket_connection(self):
        host_port_tuple = "host", 0
        await self.networking.create_socket_connection(*host_port_tuple)
        pool = self.networking.pools[host_port_tuple]
        await self.networking.close_socket_connection(*host_port_tuple)
        pool.close.assert_called_once()
        assert host_port_tuple not in self.networking.pools

    @patch('styx.common.networking.SocketPool', SocketPoolMock)
    async def test_close_socket_connection_no_port(self):
        with patch('styx.common.networking.logging') as logging_mock:
            host_port_tuple = "host", 0
            await self.networking.close_socket_connection(*host_port_tuple)
            logging_mock.warning.assert_called_once()

    @patch('styx.common.networking.SocketConnection', SocketConnectionMock)
    @patch('styx.common.networking.SocketPool', SocketPoolMock)
    async def test_send_message(self):
        host, port = "host", 0
        message = b'message'
        await self.networking.create_socket_connection(host, port)
        await self.networking.send_message(host, port, message, 0)
        next(self.networking.pools[(host, port)]).zmq_socket.write.assert_called_once()

    @patch('styx.common.networking.SocketPool', SocketPoolMock)
    async def test_send_message_create_conn(self):
        host, port = "host", 0
        message = b'message'
        await self.networking.send_message(host, port, message, 0)
        assert (host, port) in self.networking.pools

    async def test_merge_remote_logic_aborts(self):
        self.networking.prepare_function_chain(0)
        self.networking.abort_chain(0, "ERROR", b'')
        self.networking.merge_remote_logic_aborts({1})
        assert 0 in self.networking.logic_aborts_everywhere
        assert 1 in self.networking.logic_aborts_everywhere

    @patch('styx.common.networking.SocketConnection', SocketConnectionMock)
    @patch('styx.common.networking.SocketPool', SocketPoolMock)
    @patch('styx.common.networking.NetworkingManager.decode_message', Mock())
    async def test_send_message_request_response(self):
        host, port = "host", 0
        message = b'message'
        await self.networking.create_socket_connection(host, port)
        await self.networking.send_message_request_response(host, port, message, 0)
        next(self.networking.pools[(host, port)]).zmq_socket.read.assert_called_once()

    @patch('styx.common.networking.SocketConnection', SocketConnectionMock)
    @patch('styx.common.networking.SocketPool', SocketPoolMock)
    @patch('styx.common.networking.NetworkingManager.decode_message', Mock())
    async def test_send_message_request_response_create_conn(self):
        host, port = "host", 0
        message = b'message'
        await self.networking.send_message_request_response(host, port, message, 0)
        assert (host, port) in self.networking.pools

    async def test_encode_decode_message(self):
        message = b'message'
        for serializer in Serializer:
            encoded = self.networking.encode_message(message, 0, serializer)
            decoded = self.networking.decode_message(encoded)
            assert decoded == message

    async def test_encode_message_invalid_serializer(self):
        with patch('styx.common.networking.logging') as logging_mock, self.assertRaises(SerializerNotSupported):
            message = b'message'
            self.networking.encode_message(message, 0, 'something invalid')
            logging_mock.error.assert_called_once()

    async def test_decode_message_invalid_serializer(self):
        with patch('styx.common.networking.logging') as logging_mock, self.assertRaises(SerializerNotSupported):
            message = b'message'
            # Assuming 123 will never be a valid option...
            self.networking.decode_message(bytes((0, 123))+message)
            logging_mock.error.assert_called_once()

    async def test_get_message_type(self):
        message = b'message'
        assert self.networking.get_msg_type(bytes((0, 1)) + message) == 0
