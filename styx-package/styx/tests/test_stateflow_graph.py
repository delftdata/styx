import unittest

from styx.common.stateflow_graph import StateflowGraph
from styx.common.base_operator import BaseOperator
from styx.common.local_state_backends import LocalStateBackend


class TestStateflowGraph(unittest.TestCase):
    def setUp(self):
        self.graph = StateflowGraph("graph", LocalStateBackend.DICT)

    def test_add_single_operator(self):
        operator = BaseOperator("test")
        self.graph.add_operator(operator)

        assert operator.name in self.graph.nodes
        assert self.graph.nodes[operator.name] is operator

    def test_add_multiple_operators(self):
        operator = BaseOperator("test")
        another_operator = BaseOperator("test2")
        operators = [operator, another_operator]
        self.graph.add_operators(*operators)

        for op in operators:
            assert op.name in self.graph.nodes
            assert self.graph.nodes[op.name] is op

    def test_iterable(self):
        op_names = ["a", "b", "c"]
        self.graph.add_operators(*[BaseOperator(name) for name in op_names])
        for name, op in self.graph:
            assert name in op_names
            assert type(op) == BaseOperator
