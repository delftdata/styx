import logging
from typing import TYPE_CHECKING

from styx.common.operator import BaseOperator, Operator

if TYPE_CHECKING:
    from collections.abc import Iterator

    from styx.common.local_state_backends import LocalStateBackend


class StateflowGraph:
    """Represents a dataflow graph of operators in a Styx application.

    Each node in the graph is an operator. The graph tracks the
    execution structure, provides utilities for topic resolution,
    and enables operator lookup and iteration.
    """

    def __init__(
        self, name: str, operator_state_backend: LocalStateBackend, max_operator_parallelism: int = 10
    ) -> None:
        """Initializes the StateflowGraph.

        Args:
            name (str): Name of the graph.
            operator_state_backend (LocalStateBackend): The state backend used by all operators.
        """
        self.name: str = name
        self.operator_state_backend: LocalStateBackend = operator_state_backend
        self.nodes: dict[str, BaseOperator | Operator] = {}
        self.max_operator_parallelism: int = max_operator_parallelism

    def add_operator(self, operator: BaseOperator | Operator) -> None:
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

    def get_operator(
        self,
        operator: BaseOperator | Operator,
    ) -> BaseOperator | Operator:
        """Retrieves an operator from the graph by name.

        Args:
            operator (BaseOperator): The operator whose name will be used as a key.

        Returns:
            BaseOperator: The corresponding operator instance.
        """
        return self.nodes[operator.name]

    def get_operator_by_name(self, operator_name: str) -> BaseOperator | Operator:
        return self.nodes[operator_name]

    def add_operators(self, *operators: BaseOperator | Operator) -> None:
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
            (
                operator_name,
                self.nodes[operator_name],
            )
            for operator_name in self.nodes
        )

    def __repr__(self) -> str:
        node_info = ", ".join(f"{key}={op.name}({op.n_partitions}p)" for key, op in self.nodes.items())
        return f"StateflowGraph(name={self.name!r}, nodes={{ {node_info} }})"

    def __str__(self) -> str:
        node_descriptions = []
        for key, op in self.nodes.items():
            node_descriptions.append(
                f"{key}: {op.name} (partitions: {op.n_partitions})",
            )
        nodes_str = "\n  ".join(node_descriptions)
        return f"StateflowGraph '{self.name}' with nodes:\n  {nodes_str}"

    def compare_with(self, other: StateflowGraph) -> tuple[bool, bool]:
        """Compare this graph with another graph and detect structural changes.

        This method is intended to validate whether an update from the current
        graph to ``other`` is supported and whether it requires state migration.

        The comparison performs the following checks:

        - **New operators**: If an operator exists in ``other`` but not in the
          current graph, a warning is logged. Adding new operators is currently
          unsupported and marks the update as incompatible.
        - **Parallelism changes**: If an operator exists in both graphs but its
          ``n_partitions`` value differs, a warning is logged and state migration
          is marked as required.

        Operator implementation/code changes are not inspected here; only
        structural differences (operator presence and partition counts) are
        evaluated.

        Args:
            other (StateflowGraph):
                The target graph to compare against (typically the updated graph).

        Returns:
            tuple[bool, bool]:
                A tuple ``(compatible, migration_required)`` where:

                - ``compatible`` is ``False`` if unsupported structural changes
                  are detected (e.g., new operators).
                - ``migration_required`` is ``True`` if operator parallelism
                  changes were detected and state migration must be triggered.
        """
        compatible = True
        migration_required = False

        # Warn on new operators (unsupported)
        new_ops = set(other.nodes.keys()) - set(self.nodes.keys())
        for op_name in sorted(new_ops):
            logging.warning(
                "StateflowGraph update for %r: new operator %r detected (unsupported).",
                self.name,
                op_name,
            )
            compatible = False

        common_ops = set(self.nodes.keys()) & set(other.nodes.keys())
        for op_name in sorted(common_ops):
            old_op = self.nodes[op_name]
            new_op = other.nodes[op_name]
            if getattr(old_op, "n_partitions", None) != getattr(new_op, "n_partitions", None):
                logging.warning(
                    "StateflowGraph update for %r: operator %r partition count changed: %s -> %s."
                    " State Migration will trigger with the update.",
                    self.name,
                    op_name,
                    getattr(old_op, "n_partitions", None),
                    getattr(new_op, "n_partitions", None),
                )
                migration_required = True
        return compatible, migration_required
