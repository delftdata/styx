import unittest
from unittest.mock import Mock, ANY

from styx.common.operator import Operator
from styx.common.networking import NetworkingManager
from styx.common.message_types import MessageType


class TestOperatorPrivate(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.operator = Operator('operator', n_partitions=1)
        self.net_man = Mock(NetworkingManager)
        self.operator._Operator__networking = self.net_man
        self.net_man.worker_id = 1

    async def test_send_chain_abort(self):
        self.net_man.in_the_same_network.return_value = True
        await self.operator._send_chain_abort(0, "host", 0, 0, b'')
        self.net_man.in_the_same_network.assert_called_once()
        self.net_man.abort_chain.assert_called_once()

        self.net_man.in_the_same_network.return_value = False
        await self.operator._send_chain_abort(0, "host", 0, 0, b'')
        self.net_man.send_message.assert_called_once()

    async def test_send_cache_ack(self):
        self.net_man.in_the_same_network.return_value = True
        await self.operator._Operator__send_cache_ack("host", 0, 0)
        self.net_man.in_the_same_network.assert_called_once()
        self.net_man.add_ack_cnt.assert_called_once()

        self.net_man.in_the_same_network.return_value = False
        await self.operator._Operator__send_cache_ack("host", 0, 0)
        self.net_man.send_message.assert_called_once_with(
            "host", 0, msg=ANY, msg_type=MessageType.AckCache, serializer=ANY)

    async def test_send_ack(self):
        self.net_man.in_the_same_network.return_value = True
        await self.operator._Operator__send_ack("host", 0, 0, "0", [0], 0)
        self.net_man.in_the_same_network.assert_called_once()
        self.net_man.add_ack_fraction_str.assert_called_once()

        self.net_man.in_the_same_network.return_value = False
        await self.operator._Operator__send_ack("host", 0, 0, "0", [0], 0)
        self.net_man.send_message.assert_called_once_with("host", 0, msg=(
            ANY, ANY, [0, self.net_man.worker_id], ANY), msg_type=MessageType.Ack, serializer=ANY)

        await self.operator._Operator__send_ack("host", 0, 0, "0", [0, self.net_man.worker_id], 0)
        self.net_man.send_message.assert_called_with("host", 0, msg=(
            ANY, ANY, [0, self.net_man.worker_id], ANY), msg_type=MessageType.Ack, serializer=ANY)
