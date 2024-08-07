import unittest
from unittest.mock import Mock, AsyncMock, ANY
from collections import namedtuple

from styx.common.operator import Operator
from styx.common.base_protocol import BaseTransactionalProtocol
from styx.common.exceptions import OperatorDoesNotContainFunction

Payload = namedtuple("Payload", ["ack_host", "ack_port", "ack_id",
                     "fraction_str", "chain_participants", "partial_node_count"])


def args(payload=None, cache=False):
    return [
        1,
        1,
        b'0',
        1,
        'func_for_test',
        payload,
        cache,
        cache,
        tuple(),
        Mock(BaseTransactionalProtocol)
    ]


async def func_for_test(ctx):
    return 1337


class TestOperator(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.operator = Operator('operator', n_partitions=1)
        self.operator._send_chain_abort = AsyncMock()
        self.operator._Operator__send_cache_ack = AsyncMock()
        self.operator._Operator__send_ack = AsyncMock()

    def test_register(self):
        def some_func():
            pass
        self.operator.register(some_func)
        assert some_func.__name__ in self.operator.functions
        assert self.operator.functions[some_func.__name__] is some_func

    def test_attach_state_networking(self):
        assert self.operator._Operator__state == ... and self.operator._Operator__networking == ... and self.operator._Operator__dns == {}
        self.operator.attach_state_networking(True, True, True)
        assert self.operator._Operator__state and self.operator._Operator__networking and self.operator._Operator__dns

    def test_set_n_partitions(self):
        assert self.operator.n_partitions == 1
        self.operator.set_n_partitions(2)
        assert self.operator.n_partitions == 2

    async def test_run_function(self):
        with self.assertRaises(OperatorDoesNotContainFunction):
            await self.operator.run_function(*args())
        self.operator.register(func_for_test)
        res = await self.operator.run_function(*args())
        assert res == 1337

    async def test_run_function_chain(self):
        func = AsyncMock(func_for_test)
        func.__name__ = "func_for_test"
        self.operator.register(func)
        payload = Payload("ack_host", 0, 0, "fraction_str", [], 0)
        res = await self.operator.run_function(*args(payload))
        self.operator.functions["func_for_test"].assert_called_once_with(ANY)

    async def test_run_function_chain_ack(self):
        self.operator.register(func_for_test)
        payload = Payload("ack_host", 0, 0, "fraction_str", [], 0)
        res = await self.operator.run_function(*args(payload))
        self.operator._Operator__send_ack.assert_called_once()

    async def test_run_function_cache_ack(self):
        self.operator.register(func_for_test)
        payload = Payload("ack_host", 0, 0, "fraction_str", [], 0)
        res = await self.operator.run_function(*args(payload, True))
        self.operator._Operator__send_cache_ack.assert_called_once()

    async def test_run_function_chain_abort(self):
        async def func_for_test(ctx):
            raise Exception(1337)

        self.operator.register(func_for_test)
        payload = Payload("ack_host", 0, 0, "fraction_str", [], 0)
        res = await self.operator.run_function(*args(payload))
        self.operator._send_chain_abort.assert_called_once()
