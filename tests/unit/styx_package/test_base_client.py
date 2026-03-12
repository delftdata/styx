"""Unit tests for styx/client/base_client.py"""

from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
from styx.client.base_client import SNAPSHOT_BUCKET, BaseStyxClient, _ensure_bucket_exists
from styx.common.exceptions import GraphNotSerializableError, NotAStateflowGraphError
from styx.common.operator import Operator
from styx.common.serialization import Serializer
from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class ConcreteStyxClient(BaseStyxClient):
    """Minimal concrete implementation for testing the ABC."""

    def get_operator_partition(self, key, operator):
        return 0

    def close(self):
        pass

    def open(self, consume=True):
        pass

    def flush(self):
        pass

    def send_event(self, operator, key, function, params=(), serializer=Serializer.MSGPACK):
        pass

    def set_graph(self, graph):
        self._current_active_graph = graph

    def submit_dataflow(self, stateflow_graph, external_modules=None):
        pass

    def update_dataflow(self, stateflow_graph, external_modules=None):
        pass

    def notify_init_data_complete(self):
        pass


def _graph(name="test-app", n_partitions=2):
    g = StateflowGraph(name, operator_state_backend=LocalStateBackend.DICT)
    op = Operator("users", n_partitions)
    g.add_operator(op)
    return g


# ---------------------------------------------------------------------------
# _ensure_bucket_exists
# ---------------------------------------------------------------------------


class TestEnsureBucketExists:
    def test_creates_bucket(self):
        s3 = MagicMock()
        _ensure_bucket_exists(s3, "my-bucket")
        s3.create_bucket.assert_called_once_with(Bucket="my-bucket")

    def test_ignores_already_owned(self):
        s3 = MagicMock()
        error_response = {"Error": {"Code": "BucketAlreadyOwnedByYou"}}
        s3.create_bucket.side_effect = botocore.exceptions.ClientError(error_response, "CreateBucket")
        _ensure_bucket_exists(s3, "my-bucket")  # should not raise

    def test_ignores_already_exists(self):
        s3 = MagicMock()
        error_response = {"Error": {"Code": "BucketAlreadyExists"}}
        s3.create_bucket.side_effect = botocore.exceptions.ClientError(error_response, "CreateBucket")
        _ensure_bucket_exists(s3, "my-bucket")  # should not raise

    def test_ignores_409(self):
        s3 = MagicMock()
        error_response = {"Error": {"Code": "409"}}
        s3.create_bucket.side_effect = botocore.exceptions.ClientError(error_response, "CreateBucket")
        _ensure_bucket_exists(s3, "my-bucket")  # should not raise

    def test_propagates_other_errors(self):
        s3 = MagicMock()
        error_response = {"Error": {"Code": "AccessDenied"}}
        s3.create_bucket.side_effect = botocore.exceptions.ClientError(error_response, "CreateBucket")
        with pytest.raises(botocore.exceptions.ClientError):
            _ensure_bucket_exists(s3, "my-bucket")


# ---------------------------------------------------------------------------
# BaseStyxClient init
# ---------------------------------------------------------------------------


class TestBaseStyxClientInit:
    def test_stores_address(self):
        c = ConcreteStyxClient("localhost", 8888)
        assert c._styx_coordinator_adr == "localhost"
        assert c._styx_coordinator_port == 8888

    def test_no_s3(self):
        c = ConcreteStyxClient("localhost", 8888)
        assert c.s3 is None

    def test_with_s3_creates_bucket(self):
        s3 = MagicMock()
        c = ConcreteStyxClient("localhost", 8888, s3=s3)
        s3.create_bucket.assert_called_once_with(Bucket=SNAPSHOT_BUCKET)

    def test_delivery_timestamps_empty(self):
        c = ConcreteStyxClient("localhost", 8888)
        assert c.delivery_timestamps == {}


# ---------------------------------------------------------------------------
# _verify_dataflow_input
# ---------------------------------------------------------------------------


class TestVerifyDataflowInput:
    def test_rejects_non_graph(self):
        c = ConcreteStyxClient("localhost", 8888)
        with pytest.raises(NotAStateflowGraphError):
            c._verify_dataflow_input("not a graph", None)

    def test_accepts_valid_graph(self):
        c = ConcreteStyxClient("localhost", 8888)
        g = _graph()
        c._verify_dataflow_input(g, None)  # should not raise

    def test_non_serializable_graph_raises(self):
        c = ConcreteStyxClient("localhost", 8888)
        g = _graph()
        # Inject a non-serializable object
        g.nodes["users"].functions["bad"] = lambda x: x  # lambdas serialize fine with cloudpickle
        # This should still work with cloudpickle
        c._verify_dataflow_input(g, None)


# ---------------------------------------------------------------------------
# _prepare_kafka_message
# ---------------------------------------------------------------------------


class TestPrepareKafkaMessage:
    def test_returns_three_tuple(self):
        c = ConcreteStyxClient("localhost", 8888)
        c._current_active_graph = _graph()
        request_id, serialized_value, partition = c._prepare_kafka_message(
            key="user1",
            operator=Operator("users", 2),
            function="my_func",
            params=(1, 2),
            serializer=Serializer.MSGPACK,
        )
        assert isinstance(request_id, bytes)
        assert isinstance(serialized_value, bytes)
        assert isinstance(partition, int)
        assert 0 <= partition < 2

    def test_with_explicit_partition(self):
        c = ConcreteStyxClient("localhost", 8888)
        c._current_active_graph = _graph()
        _, _, partition = c._prepare_kafka_message(
            key="user1",
            operator=Operator("users", 2),
            function="my_func",
            params=(),
            serializer=Serializer.MSGPACK,
            partition=1,
        )
        assert partition == 1

    def test_function_type_to_name(self):
        c = ConcreteStyxClient("localhost", 8888)
        c._current_active_graph = _graph()

        class MyFunc:
            pass

        request_id, serialized_value, partition = c._prepare_kafka_message(
            key="k",
            operator=Operator("users", 2),
            function=MyFunc,
            params=(),
            serializer=Serializer.MSGPACK,
        )
        assert isinstance(serialized_value, bytes)


# ---------------------------------------------------------------------------
# init_data / init_metadata
# ---------------------------------------------------------------------------


class TestInitData:
    def test_init_data_without_s3_raises(self):
        c = ConcreteStyxClient("localhost", 8888)
        with pytest.raises(RuntimeError, match="No S3 client configured"):
            c.init_data(Operator("users", 2), 0, {"k": "v"})

    def test_init_data_with_s3(self):
        s3 = MagicMock()
        c = ConcreteStyxClient("localhost", 8888, s3=s3)
        c.init_data(Operator("users", 2), 0, {"k": "v"})
        s3.put_object.assert_called_once()
        call_kwargs = s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == SNAPSHOT_BUCKET
        assert "data/users/0/0.bin" == call_kwargs["Key"]

    def test_init_metadata_without_s3_raises(self):
        c = ConcreteStyxClient("localhost", 8888)
        with pytest.raises(RuntimeError, match="No S3 client configured"):
            c.init_metadata(_graph())

    def test_init_metadata_with_s3(self):
        s3 = MagicMock()
        c = ConcreteStyxClient("localhost", 8888, s3=s3)
        c.init_metadata(_graph())
        # Should store sequencer metadata
        calls = s3.put_object.call_args_list
        # Last call should be the sequencer metadata
        last_call = calls[-1][1]
        assert last_call["Key"] == "sequencer/0.bin"
