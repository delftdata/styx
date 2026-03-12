"""Unit tests for worker/ingress/styx_kafka_ingress.py"""

from unittest.mock import MagicMock

from styx.common.message_types import MessageType
from styx.common.run_func_payload import RunFuncPayload

from worker.ingress.styx_kafka_ingress import StyxKafkaIngress

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ingress():
    networking = MagicMock()
    networking.get_msg_type = MagicMock(return_value=MessageType.ClientMsg)
    networking.decode_message = MagicMock(return_value=("users", "key1", "my_func", (1,), 0))
    networking.in_the_same_network = MagicMock(return_value=True)

    sequencer = MagicMock()
    sequencer.lock = MagicMock()

    state = MagicMock()
    state.exists = MagicMock(return_value=True)

    op = MagicMock()
    op.which_partition = MagicMock(return_value=0)
    op.dns = {"users": {0: ("host1", 5000, 6000)}}

    registered_operators = {("users", 0): op}

    ingress = StyxKafkaIngress(
        networking=networking,
        sequencer=sequencer,
        state=state,
        registered_operators=registered_operators,
        worker_id=0,
        kafka_url="localhost:9092",
        epoch_interval_ms=1,
        sequence_max_size=1000,
    )
    return ingress, networking, sequencer, state


def _make_msg(key=b"req1", offset=0, partition=0, value=b"\x01\x01test"):
    msg = MagicMock()
    msg.key = key
    msg.offset = offset
    msg.partition = partition
    msg.value = value
    return msg


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestKafkaIngressInit:
    def test_defaults(self):
        ingress, *_ = _make_ingress()
        assert ingress.worker_id == 0
        assert ingress.kafka_url == "localhost:9092"
        assert ingress.epoch_interval_ms == 1
        assert ingress.sequence_max_size == 1000
        assert ingress.send_message_tasks == []
        assert not ingress.started.is_set()
        assert ingress.kafka_consumer is None


# ---------------------------------------------------------------------------
# handle_message_from_kafka
# ---------------------------------------------------------------------------


class TestHandleMessageFromKafka:
    def test_normal_operation_sequences(self):
        ingress, _networking, sequencer, _state = _make_ingress()
        msg = _make_msg()
        ingress.handle_message_from_kafka(msg)
        sequencer.sequence.assert_called_once()
        payload = sequencer.sequence.call_args[0][0]
        assert isinstance(payload, RunFuncPayload)
        assert payload.key == "key1"
        assert payload.operator_name == "users"
        assert payload.function_name == "my_func"

    def test_key_none_sequences(self):
        """When key is None (insert operation), it should still sequence."""
        ingress, networking, sequencer, _state = _make_ingress()
        networking.decode_message.return_value = ("users", None, "insert_func", (), 0)
        msg = _make_msg()
        ingress.handle_message_from_kafka(msg)
        sequencer.sequence.assert_called_once()

    def test_key_not_in_state_correct_partition(self):
        """Key doesn't exist in state but partition matches = insert."""
        ingress, _networking, sequencer, state = _make_ingress()
        state.exists.return_value = False
        # which_partition returns same as the message partition
        ingress.registered_operators[("users", 0)].which_partition.return_value = 0

        msg = _make_msg()
        ingress.handle_message_from_kafka(msg)
        sequencer.sequence.assert_called_once()

    def test_key_not_in_state_wrong_partition_local(self):
        """Key migrated to different partition, but still local worker."""
        ingress, networking, sequencer, state = _make_ingress()
        state.exists.return_value = False
        networking.decode_message.return_value = ("users", "key1", "my_func", (1,), 0)
        # which_partition returns a different partition than the message's partition
        ingress.registered_operators[("users", 0)].which_partition.return_value = 1
        networking.in_the_same_network.return_value = True
        ingress.registered_operators[("users", 0)].dns = {"users": {1: ("host1", 5000, 6000)}}

        msg = _make_msg()
        ingress.handle_message_from_kafka(msg)
        sequencer.sequence.assert_called_once()
        payload = sequencer.sequence.call_args[0][0]
        assert payload.partition == 1  # rerouted to correct partition

    def test_key_not_in_state_wrong_partition_remote(self):
        """Key migrated to different partition on a remote worker."""
        ingress, networking, sequencer, state = _make_ingress()
        state.exists.return_value = False
        networking.decode_message.return_value = ("users", "key1", "my_func", (1,), 0)
        ingress.registered_operators[("users", 0)].which_partition.return_value = 1
        networking.in_the_same_network.return_value = False
        ingress.registered_operators[("users", 0)].dns = {"users": {1: ("remote_host", 5001, 6001)}}

        msg = _make_msg()
        ingress.handle_message_from_kafka(msg)
        sequencer.sequence.assert_not_called()
        assert len(ingress.send_message_tasks) == 1

    async def test_invalid_message_type(self):
        """Non-ClientMsg types should be logged as errors."""
        ingress, networking, sequencer, _state = _make_ingress()
        networking.get_msg_type.return_value = 99  # invalid type
        msg = _make_msg()
        ingress.handle_message_from_kafka(msg)
        sequencer.sequence.assert_not_called()
