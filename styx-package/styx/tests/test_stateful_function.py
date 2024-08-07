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
    statefun._StatefulFunction__send_async_calls = AsyncMock(return_value=1)
    statefun.run = AsyncMock(statefun.run, return_value=1)
    return statefun

# NOTE: Move 'make_key_hashable' to some util module?
class TestStatefulFunction(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.state = Mock(BaseOperatorState)
        method_overrides = {
            'get.return_value': 1337,
            'get_immediate.return_value': 42
        }
        self.state.configure_mock(**method_overrides)
        self.networking = Mock(NetworkingManager)
        self.protocol = Mock(BaseTransactionalProtocol)
        self.statefun = construct_stateful_function(
            self.state, self.networking, self.protocol)

    def test_get(self):
        val = self.statefun.get()
        self.state.get.assert_called_once()
        assert val == 1337

        self.statefun = construct_stateful_function(
            self.state, self.networking, self.protocol, True)
        val_immedate = self.statefun.get()
        self.state.get_immediate.assert_called_once()
        assert val_immedate == 42

    def test_put(self):
        self.statefun.put(0)
        self.state.put.assert_called_once()

        self.statefun = construct_stateful_function(
            self.state, self.networking, self.protocol, True)
        self.statefun.put(0)
        self.state.put_immediate.assert_called_once()

    def test_batch_insert(self):
        self.statefun.batch_insert({})
        self.state.batch_insert.assert_not_called()

        dictionary = {0: 0, 1: 1}
        self.statefun.batch_insert(dictionary)
        self.state.batch_insert.assert_called_once_with(
            dictionary, self.statefun._StatefulFunction__operator_name)

    def test_data(self):
        self.statefun.data
        self.state.get_all.assert_called_once_with(
            self.statefun._StatefulFunction__t_id,
            self.statefun._StatefulFunction__operator_name
        )

    def test_call_remote_async(self):
        self.statefun.call_remote_async(
            "op",
            "func",
            0,
            tuple()
        )
        self.networking.in_the_same_network.assert_called_once_with("host", 0)
        assert len(self.statefun._StatefulFunction__async_remote_calls) == 1

    async def test_call_basic(self):
        res = await self.statefun()
        self.statefun.run.assert_called_once()
        assert res == (1, 0, 0)

    async def test_call_fallback_cache(self):
        self.statefun = construct_stateful_function(
            self.state, self.networking, self.protocol, True, True)
        res = await self.statefun()
        assert res == (1, 0, -1)

        # Even when there are async remote calls, still use cache
        self.statefun.call_remote_async("op", "func", 0, tuple())
        res = await self.statefun()
        assert res == (1, 1, -1)
        self.statefun._StatefulFunction__send_async_calls.assert_not_called()

        # Don't use cache when only fallback is enabled
        self.statefun = construct_stateful_function(
            self.state, self.networking, self.protocol, True, False)
        res = await self.statefun()
        assert res == (1, 0, 0)

        self.statefun.call_remote_async("op", "func", 0, tuple())
        res = await self.statefun()
        assert res == (1, 1, -1)
        self.statefun._StatefulFunction__send_async_calls.assert_called_once()

    async def test_call_remote(self):
        self.statefun.call_remote_async("op", "func", 0, tuple())
        res = await self.statefun()
        assert res == (1, 1, -1)
        self.statefun._StatefulFunction__send_async_calls.assert_called_once()

    async def test_call_remote_calls(self):
        res = await self.statefun(ack_port=0, ack_share=0, ack_host="host", chain_participants=[], partial_node_count=1)
        assert res == (1, 1, 1)
        self.statefun._StatefulFunction__send_async_calls.assert_called_once()
