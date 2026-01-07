from typing import Iterator

from .local_state_backends import LocalStateBackend
from .operator import BaseOperator, Operator


class StateflowGraph(object):
    """Represents a dataflow graph of operators in a Styx application.

    Each node in the graph is an operator. The graph tracks the
    execution structure, provides utilities for topic resolution,
    and enables operator lookup and iteration.
    """

    def __init__(self, name: str, operator_state_backend: LocalStateBackend):
        """Initializes the StateflowGraph.

        Args:
            name (str): Name of the graph.
            operator_state_backend (LocalStateBackend): The state backend used by all operators.
        """
        self.name: str = name
        self.operator_state_backend: LocalStateBackend = operator_state_backend
        self.nodes: dict[str, BaseOperator | Operator] = {}

    def add_operator(self, operator: BaseOperator | Operator):
        """Adds a single operator to the graph.

        Args:
            operator (BaseOperator): Operator instance to add.
        """
        self.nodes[operator.name] = operator

    def get_egress_topic_names(self) -> list[str]:
        """Returns the Kafka egress topic names for all operators.

        Returns:
            list[str]: A list of topic names with '--OUT' suffix.
        """
        return [node.name + "--OUT" for node in self.nodes.values()]

    def get_operator(self, operator: BaseOperator | Operator) -> BaseOperator | Operator:
        """Retrieves an operator from the graph by name.

        Args:
            operator (BaseOperator): The operator whose name will be used as a key.

        Returns:
            BaseOperator: The corresponding operator instance.
        """
        return self.nodes[operator.name]

    def get_operator_by_name(self, operator_name: str) -> BaseOperator | Operator:
        return self.nodes[operator_name]

    def add_operators(self, *operators):
        """Adds multiple operators to the graph.

        Args:
            *operators: Variable-length list of BaseOperator instances.
        """
        [self.add_operator(operator) for operator in operators if issubclass(type(operator), BaseOperator)]

    def __iter__(self) -> Iterator[tuple[str, BaseOperator]]:
        """Returns an iterator over (operator_name, operator) pairs.

        Returns:
            Iterator[tuple[str, BaseOperator]]: Iterable of name-operator pairs.
        """
        return (
            (operator_name,
             self.nodes[operator_name]
             )
            for operator_name in self.nodes.keys())


    def __repr__(self):
        node_info = ", ".join(f"{key}={op.name}({op.n_partitions}p)" for key, op in self.nodes.items())
        return f"StateflowGraph(name={self.name!r}, nodes={{ {node_info} }})"

    def __str__(self):
        node_descriptions = []
        for key, op in self.nodes.items():
            node_descriptions.append(f"{key}: {op.name} (partitions: {op.n_partitions})")
        nodes_str = "\n  ".join(node_descriptions)
        return f"StateflowGraph '{self.name}' with nodes:\n  {nodes_str}"
