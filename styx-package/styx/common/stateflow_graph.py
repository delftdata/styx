from .local_state_backends import LocalStateBackend
from .operator import BaseOperator, Operator


class StateflowGraph(object):

    def __init__(self, name: str, operator_state_backend: LocalStateBackend):
        self.name: str = name
        self.operator_state_backend: LocalStateBackend = operator_state_backend
        self.nodes: dict[str, BaseOperator | Operator] = {}

    def add_operator(self, operator: BaseOperator):
        self.nodes[operator.name] = operator

    def get_egress_topic_names(self) -> list[str]:
        return [node.name + "--OUT" for node in self.nodes.values()]

    def get_operator(self, operator: BaseOperator) -> BaseOperator:
        return self.nodes[operator.name]

    def add_operators(self, *operators):
        [self.add_operator(operator) for operator in operators if issubclass(type(operator), BaseOperator)]

    def __iter__(self):
        return (
            (operator_name,
             self.nodes[operator_name]
             )
            for operator_name in self.nodes.keys())
