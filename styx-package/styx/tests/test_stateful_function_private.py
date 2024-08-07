import unittest
from unittest.mock import Mock, AsyncMock

from styx.common.networking import NetworkingManager
from styx.common.stateful_function import StatefulFunction
from styx.common.base_state import BaseOperatorState
from styx.common.base_protocol import BaseTransactionalProtocol

def construct_stateful_function(state, networking, protocol, fallback=False, cache=False):
    statefun = StatefulFunction(
        0,
        "func",
        "op",
        state,
        networking,
        0,
        {"op": {"0": ("host", 0, 0)}},
        0,
        b'0',
        fallback,
        cache,
        protocol
    )
    statefun.run = AsyncMock(statefun.run, return_value=1)
    return statefun


class TestStatefulFunctionPrivate(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.state = Mock(BaseOperatorState)
        method_overrides = {
            'get.return_value': 1337,
            'get_immediate.return_value': 42
        }
        self.state.configure_mock(**method_overrides)
        self.networking = Mock(NetworkingManager(0))
        self.protocol = Mock(BaseTransactionalProtocol)
        self.statefun = construct_stateful_function(
            self.state, self.networking, self.protocol)

    async def test_send_async_calls_empty_remote_calls(self):
        res = await self.statefun._StatefulFunction__send_async_calls("host", 0, 0, [], 0)
        assert res == 0

    async def test_send_async_calls_none_host(self):
        self.statefun.call_remote_async("op", "func", 0, tuple())
        res = await self.statefun._StatefulFunction__send_async_calls(None, 0, 0, [], 0)
        self.networking.prepare_function_chain.assert_called_once()
        assert res == 1

    async def test_send_async_calls_host_non_local(self):
        self.networking.in_the_same_network.return_value = True
        self.statefun.call_remote_async("op", "func", 0, tuple())
        chain_participants = []
        res = await self.statefun._StatefulFunction__send_async_calls("host", 0, 0, chain_participants, 0)
        assert len(chain_participants) == 0

        # Only add to chain_participants if not in same network
        self.networking.in_the_same_network.return_value = False
        res = await self.statefun._StatefulFunction__send_async_calls("host", 0, 0, chain_participants, 0)
        assert len(chain_participants) > 0

    async def test_send_async_calls_local(self):
        self.networking.in_the_same_network.return_value = True

        self.statefun.call_remote_async("op", "func", 0, tuple())
        res = await self.statefun._StatefulFunction__send_async_calls(None, 0, 0, [], 0)

        self.protocol.run_function.assert_called_once()
        self.networking.add_remote_function_call.assert_called_once()

    async def test_send_async_calls_local_fallback(self):
        self.networking.in_the_same_network.return_value = True
        self.statefun._StatefulFunction__fallback_enabled = True

        self.statefun.call_remote_async("op", "func", 0, tuple())
        res = await self.statefun._StatefulFunction__send_async_calls(None, 0, 0, [], 0)

        self.protocol.run_fallback_function.assert_called_once()
        self.networking.add_remote_function_call.assert_called_once()

    async def test_send_async_calls_non_local(self):
        self.networking.in_the_same_network.return_value = False
        self.statefun._StatefulFunction__call_remote_function_no_response = AsyncMock()

        self.statefun.call_remote_async("op", "func", 0, tuple())
        res = await self.statefun._StatefulFunction__send_async_calls(None, 0, 0, [], 0)

        self.statefun._StatefulFunction__call_remote_function_no_response.assert_called_once()

    async def test_call_remote_function_no_response(self):
        self.statefun._StatefulFunction__prepare_message_transmission = Mock(return_value=(None, None, None))
        res = await self.statefun._StatefulFunction__call_remote_function_no_response("op", "func", 0, 0, tuple())
        self.statefun._StatefulFunction__prepare_message_transmission.assert_called_once()
        self.networking.send_message.assert_called_once()

    async def test_prepare_message_transmission(self):
        # NOTE: The private API of __prepare_message_transmission is broken,
        # because it returns None if the operator cannot be found which cannot
        # be unpacked into the expected tuple. Instead of returning None and
        # breaking stuff within its caller, it should throw an exception or
        # return some default/error payload that has the same signature as its
        # other returns
        res_no_ack = self.statefun._StatefulFunction__prepare_message_transmission("op", 0, "func", 0, tuple())
        assert len(res_no_ack) == 3

        # with self.assertRaises(SomeSensibleException):
        #     res = self.statefun._StatefulFunction__prepare_message_transmission("randomstuff", 0, "func", 0, tuple())
        
        res_ack = self.statefun._StatefulFunction__prepare_message_transmission("op", 0, "func", 0, tuple(), ack_payload=Mock())
        assert len(res_ack) == 3 and len(res_ack[0]) - len(res_no_ack[0]) == 1