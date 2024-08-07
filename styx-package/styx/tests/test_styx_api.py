import unittest
import unittest.mock

from styx.common.local_state_backends import LocalStateBackend
from styx.common.networking import NetworkingManager
from styx.common.operator import Operator
from styx.common.base_protocol import BaseTransactionalProtocol
from styx.common.stateflow_graph import StateflowGraph
from styx.common.stateful_function import StatefulFunction

from worker.operator_state.aria.in_memory_state import InMemoryOperatorState

from typing import Iterable, cast


class TestState(unittest.IsolatedAsyncioTestCase):

    async def test_graph_creation(self):
        graph = StateflowGraph(
            'test_graph', operator_state_backend=LocalStateBackend.DICT)
        operator_1 = Operator('operator_1', n_partitions=3)
        operator_2 = Operator('operator_2', n_partitions=2)
        graph.add_operators(operator_1, operator_2)
        assert len(graph.nodes) == 2

        networking = NetworkingManager(0)
        local_state = InMemoryOperatorState(
            {operator_name for operator_name in graph.nodes.keys()})
        [operator.attach_state_networking(local_state, networking, {}) for
         operator in cast(Iterable[Operator], graph.nodes.values())]

        # add function that uses context
        @operator_1.register
        async def fun_1(ctx: StatefulFunction, other_str: str, some_other_str: str):
            ctx.put('test_value_')
            value = ctx.get()
            value = value + other_str + some_other_str
            return value

        args = ('other_str', 'some_other_str')
        res = await operator_1.run_function(1,
                                            1,
                                            b'0',
                                            1,
                                            'fun_1',
                                            None,
                                            False,
                                            False,
                                            args,
                                            unittest.mock.Mock(BaseTransactionalProtocol))
        assert res == 'test_value_other_strsome_other_str'
