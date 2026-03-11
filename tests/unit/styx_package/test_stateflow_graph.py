"""Unit tests for styx/common/stateflow_graph.py"""

from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _graph(name: str = "app", n: int = 4) -> StateflowGraph:
    return StateflowGraph(name, LocalStateBackend.DICT, max_operator_parallelism=n)


def _op(name: str, n_partitions: int = 2) -> Operator:
    return Operator(name, n_partitions)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestStateflowGraphInit:
    def test_name_stored(self):
        g = _graph("myapp")
        assert g.name == "myapp"

    def test_backend_stored(self):
        g = _graph()
        assert g.operator_state_backend is LocalStateBackend.DICT

    def test_nodes_empty_on_init(self):
        assert _graph().nodes == {}

    def test_default_max_parallelism(self):
        g = StateflowGraph("app", LocalStateBackend.DICT)
        assert g.max_operator_parallelism == 10

    def test_custom_max_parallelism(self):
        g = _graph(n=20)
        assert g.max_operator_parallelism == 20


# ---------------------------------------------------------------------------
# add_operator / add_operators
# ---------------------------------------------------------------------------


class TestAddOperator:
    def test_add_single(self):
        g = _graph()
        op = _op("users")
        g.add_operator(op)
        assert "users" in g.nodes
        assert g.nodes["users"] is op

    def test_add_multiple_sequentially(self):
        g = _graph()
        g.add_operator(_op("users"))
        g.add_operator(_op("orders"))
        assert set(g.nodes.keys()) == {"users", "orders"}

    def test_add_operators_variadic(self):
        g = _graph()
        g.add_operators(_op("a"), _op("b"), _op("c"))
        assert set(g.nodes.keys()) == {"a", "b", "c"}

    def test_add_operator_overwrites(self):
        g = _graph()
        op1 = _op("users", 2)
        op2 = _op("users", 8)
        g.add_operator(op1)
        g.add_operator(op2)
        assert g.nodes["users"] is op2

    def test_add_operators_skips_non_operator(self):
        g = _graph()
        g.add_operators(_op("valid"), "not_an_operator", 42)  # type: ignore[arg-type]
        assert list(g.nodes.keys()) == ["valid"]


# ---------------------------------------------------------------------------
# get_operator / get_operator_by_name
# ---------------------------------------------------------------------------


class TestGetOperator:
    def test_get_by_operator_instance(self):
        g = _graph()
        op = _op("users")
        g.add_operator(op)
        assert g.get_operator(op) is op

    def test_get_by_name(self):
        g = _graph()
        op = _op("orders")
        g.add_operator(op)
        assert g.get_operator_by_name("orders") is op


# ---------------------------------------------------------------------------
# get_egress_topic_names
# ---------------------------------------------------------------------------


class TestEgressTopics:
    def test_empty_graph(self):
        assert _graph().get_egress_topic_names() == []

    def test_single_operator(self):
        g = _graph()
        g.add_operator(_op("users"))
        assert g.get_egress_topic_names() == ["users--OUT"]

    def test_multiple_operators(self):
        g = _graph()
        g.add_operators(_op("users"), _op("orders"), _op("items"))
        topics = g.get_egress_topic_names()
        assert set(topics) == {"users--OUT", "orders--OUT", "items--OUT"}
        assert len(topics) == 3


# ---------------------------------------------------------------------------
# __iter__
# ---------------------------------------------------------------------------


class TestIter:
    def test_yields_name_operator_pairs(self):
        g = _graph()
        op1 = _op("users")
        op2 = _op("orders")
        g.add_operator(op1)
        g.add_operator(op2)
        pairs = list(g)
        names = [name for name, _ in pairs]
        ops = [op for _, op in pairs]
        assert set(names) == {"users", "orders"}
        assert op1 in ops
        assert op2 in ops

    def test_empty_graph_iter(self):
        assert list(_graph()) == []

    def test_iter_length_matches_nodes(self):
        g = _graph()
        g.add_operators(_op("a"), _op("b"), _op("c"))
        assert len(list(g)) == 3


# ---------------------------------------------------------------------------
# __repr__ / __str__
# ---------------------------------------------------------------------------


class TestReprStr:
    def test_repr_contains_name(self):
        g = _graph("myapp")
        assert "myapp" in repr(g)

    def test_repr_contains_operator_info(self):
        g = _graph()
        g.add_operator(_op("users", 4))
        r = repr(g)
        assert "users" in r
        assert "4p" in r

    def test_str_contains_graph_name(self):
        g = _graph("workflow")
        assert "workflow" in str(g)

    def test_str_contains_operator_names(self):
        g = _graph()
        g.add_operators(_op("alpha"), _op("beta"))
        s = str(g)
        assert "alpha" in s
        assert "beta" in s


# ---------------------------------------------------------------------------
# compare_with
# ---------------------------------------------------------------------------


class TestCompareWith:
    def test_identical_graphs_no_changes(self):
        g1 = _graph()
        g1.add_operator(_op("users", 4))
        g2 = _graph()
        g2.add_operator(_op("users", 4))
        compatible, migration = g1.compare_with(g2)
        assert compatible is True
        assert migration is False

    def test_new_operator_in_other_is_incompatible(self):
        g1 = _graph()
        g1.add_operator(_op("users", 4))
        g2 = _graph()
        g2.add_operator(_op("users", 4))
        g2.add_operator(_op("orders", 2))  # new operator
        compatible, migration = g1.compare_with(g2)
        assert compatible is False
        assert migration is False

    def test_partition_count_change_triggers_migration(self):
        g1 = _graph()
        g1.add_operator(_op("users", 4))
        g2 = _graph()
        g2.add_operator(_op("users", 8))  # scale up
        compatible, migration = g1.compare_with(g2)
        assert compatible is True
        assert migration is True

    def test_new_operator_and_partition_change(self):
        g1 = _graph()
        g1.add_operator(_op("users", 4))
        g2 = _graph()
        g2.add_operator(_op("users", 8))  # scale up
        g2.add_operator(_op("orders", 2))  # new operator
        compatible, migration = g1.compare_with(g2)
        assert compatible is False
        assert migration is True

    def test_removed_operator_no_migration(self):
        # Operator removed from the other graph → not in new_ops, not in common_ops
        g1 = _graph()
        g1.add_operators(_op("users", 4), _op("orders", 2))
        g2 = _graph()
        g2.add_operator(_op("users", 4))  # orders removed
        compatible, migration = g1.compare_with(g2)
        assert compatible is True
        assert migration is False

    def test_multiple_operators_some_changed(self):
        g1 = _graph()
        g1.add_operators(_op("users", 4), _op("orders", 2))
        g2 = _graph()
        g2.add_operators(_op("users", 4), _op("orders", 4))  # orders scaled
        compatible, migration = g1.compare_with(g2)
        assert compatible is True
        assert migration is True

    def test_empty_graphs_are_compatible(self):
        compatible, migration = _graph().compare_with(_graph())
        assert compatible is True
        assert migration is False
