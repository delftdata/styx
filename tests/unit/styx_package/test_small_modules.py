"""Unit tests for small styx-package modules:
- styx/common/function.py
- styx/common/local_state_backends.py
- styx/common/stateflow_ingress.py
- styx/common/stateflow_worker.py
- styx/common/exceptions.py
- styx/common/base_operator.py
"""

import pytest
from styx.common.base_operator import BaseOperator
from styx.common.exceptions import (
    FutureAlreadySetError,
    FutureTimedOutError,
    GraphNotSerializableError,
    InvalidRangePartitioningError,
    NonSupportedKeyTypeError,
    NotAStateflowGraphError,
    OperatorDoesNotContainFunctionError,
    SerializerNotSupportedError,
)
from styx.common.function import Function
from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_ingress import IngressTypes, StateflowIngress
from styx.common.stateflow_worker import StateflowWorker

# ---------------------------------------------------------------------------
# Function (ABC)
# ---------------------------------------------------------------------------


class ConcreteFunction(Function):
    def run(self, *args, **kwargs):
        return sum(args)


class TestFunction:
    def test_name_stored(self):
        f = ConcreteFunction("adder")
        assert f.name == "adder"

    def test_call_delegates_to_run(self):
        f = ConcreteFunction("adder")
        assert f(1, 2, 3) == 6

    def test_run_directly(self):
        f = ConcreteFunction("adder")
        assert f.run(10, 20) == 30

    def test_abstract_run_not_implemented(self):
        with pytest.raises(TypeError):
            Function("base")


# ---------------------------------------------------------------------------
# LocalStateBackend
# ---------------------------------------------------------------------------


class TestLocalStateBackend:
    def test_dict_value_exists(self):
        assert LocalStateBackend.DICT is not None

    def test_enum_members(self):
        assert len(LocalStateBackend) == 1


# ---------------------------------------------------------------------------
# StateflowIngress
# ---------------------------------------------------------------------------


class TestStateflowIngress:
    def test_fields(self):
        si = StateflowIngress(host="10.0.0.1", port=5000, ext_host="external.com", ext_port=8080)
        assert si.host == "10.0.0.1"
        assert si.port == 5000
        assert si.ext_host == "external.com"
        assert si.ext_port == 8080

    def test_equality(self):
        si1 = StateflowIngress("h", 1, "e", 2)
        si2 = StateflowIngress("h", 1, "e", 2)
        assert si1 == si2


class TestIngressTypes:
    def test_kafka_exists(self):
        assert IngressTypes.KAFKA is not None

    def test_enum_members(self):
        assert len(IngressTypes) == 1


# ---------------------------------------------------------------------------
# StateflowWorker
# ---------------------------------------------------------------------------


class TestStateflowWorker:
    def test_fields(self):
        sw = StateflowWorker(host="10.0.0.1", port=5000, protocol_port=6000)
        assert sw.host == "10.0.0.1"
        assert sw.port == 5000
        assert sw.protocol_port == 6000

    def test_equality(self):
        sw1 = StateflowWorker("h", 1, 2)
        sw2 = StateflowWorker("h", 1, 2)
        assert sw1 == sw2


# ---------------------------------------------------------------------------
# BaseOperator (ABC)
# ---------------------------------------------------------------------------


class ConcreteOperator(BaseOperator):
    def which_partition(self, key):
        return hash(key) % self.n_partitions

    def make_shadow(self):
        pass


class TestBaseOperator:
    def test_name_and_partitions(self):
        op = ConcreteOperator("test_op", 4)
        assert op.name == "test_op"
        assert op.n_partitions == 4

    def test_default_n_partitions(self):
        op = ConcreteOperator("op")
        assert op.n_partitions == 1

    def test_which_partition_not_implemented(self):
        op = BaseOperator("op")
        with pytest.raises(NotImplementedError):
            op.which_partition("key")

    def test_which_partition_returns_int(self):
        op = ConcreteOperator("op", 4)
        result = op.which_partition("key")
        assert isinstance(result, int)
        assert 0 <= result < 4


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_serializer_not_supported(self):
        with pytest.raises(SerializerNotSupportedError):
            raise SerializerNotSupportedError

    def test_operator_does_not_contain_function(self):
        with pytest.raises(OperatorDoesNotContainFunctionError):
            raise OperatorDoesNotContainFunctionError

    def test_non_supported_key_type(self):
        with pytest.raises(NonSupportedKeyTypeError):
            raise NonSupportedKeyTypeError

    def test_not_a_stateflow_graph(self):
        with pytest.raises(NotAStateflowGraphError):
            raise NotAStateflowGraphError

    def test_future_already_set(self):
        with pytest.raises(FutureAlreadySetError):
            msg = "already set"
            raise FutureAlreadySetError(msg)

    def test_future_timed_out(self):
        with pytest.raises(FutureTimedOutError):
            msg = "timed out"
            raise FutureTimedOutError(msg)

    def test_invalid_range_partitioning(self):
        with pytest.raises(InvalidRangePartitioningError):
            raise InvalidRangePartitioningError

    def test_graph_not_serializable(self):
        with pytest.raises(GraphNotSerializableError):
            raise GraphNotSerializableError

    def test_all_exceptions_are_exception_subclass(self):
        for exc_class in [
            SerializerNotSupportedError,
            OperatorDoesNotContainFunctionError,
            NonSupportedKeyTypeError,
            NotAStateflowGraphError,
            FutureAlreadySetError,
            FutureTimedOutError,
            InvalidRangePartitioningError,
            GraphNotSerializableError,
        ]:
            assert issubclass(exc_class, Exception)
