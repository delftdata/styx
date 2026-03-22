"""Unit tests for worker/ingress/styx_kafka_ingress.py"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from aiokafka.errors import KafkaConnectionError, UnknownTopicOrPartitionError
import pytest
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


# ---------------------------------------------------------------------------
# Shadow partition redirect (post-recovery with client partitioner mismatch)
# ---------------------------------------------------------------------------


def _make_ingress_with_shadow_partitions():
    """Create an ingress with 4 active + 4 shadow partitions.

    Simulates post-recovery state where the system has 4 active partitions
    but the client sends to 8 partitions. Shadow partitions (4-7) should
    redirect messages to the correct active partition (0-3).
    """
    networking = MagicMock()
    networking.get_msg_type = MagicMock(return_value=MessageType.ClientMsg)
    networking.in_the_same_network = MagicMock(return_value=True)

    sequencer = MagicMock()
    state = MagicMock()
    state.exists = MagicMock(return_value=False)  # shadow partitions have no state

    dns = {
        "ycsb": {
            0: ("host1", 5000, 6000),
            1: ("host2", 5001, 6001),
            2: ("host3", 5002, 6002),
            3: ("host3", 5003, 6003),
            4: ("host1", 5000, 6000),  # shadow
            5: ("host2", 5001, 6001),  # shadow
            6: ("host3", 5002, 6002),  # shadow
            7: ("host3", 5003, 6003),  # shadow
        },
    }

    registered_operators = {}
    for part in range(8):
        op = MagicMock()
        # 4-partition partitioner: always maps to 0-3
        op.which_partition = MagicMock(side_effect=lambda key, p=part: hash(key) % 4)
        op.dns = dns
        registered_operators[("ycsb", part)] = op

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


class TestShadowPartitionRedirect:
    """Tests that messages arriving on shadow partitions (4-7) are correctly
    redirected to active partitions (0-3) after recovery.

    Bug scenario: client updates its partitioner to 8 partitions during
    migration, worker crashes, recovery restores 4-partition layout. Client
    continues sending to partitions 4-7 which are shadow. Without proper
    redirect, ~50% of messages are lost.
    """

    def test_message_on_shadow_partition_redirected_locally(self):
        """Message sent to shadow partition 4 should be redirected to the
        correct active partition and sequenced locally."""
        ingress, networking, sequencer, state = _make_ingress_with_shadow_partitions()
        # Client sends to partition 4 (shadow) with key "test_key"
        networking.decode_message.return_value = ("ycsb", "test_key", "read", (), 4)
        # which_partition returns an active partition (0-3)
        ingress.registered_operators[("ycsb", 4)].which_partition = MagicMock(return_value=0)

        msg = _make_msg(partition=4)
        ingress.handle_message_from_kafka(msg)

        sequencer.sequence.assert_called_once()
        payload = sequencer.sequence.call_args[0][0]
        assert payload.partition == 0  # redirected to active partition

    def test_message_on_shadow_partition_redirected_remotely(self):
        """Message on shadow partition redirected to remote worker via
        WrongPartitionRequest."""
        ingress, networking, sequencer, state = _make_ingress_with_shadow_partitions()
        networking.decode_message.return_value = ("ycsb", "test_key", "read", (), 5)
        ingress.registered_operators[("ycsb", 5)].which_partition = MagicMock(return_value=1)
        networking.in_the_same_network.return_value = False

        msg = _make_msg(partition=5)
        ingress.handle_message_from_kafka(msg)

        sequencer.sequence.assert_not_called()
        assert len(ingress.send_message_tasks) == 1

    def test_message_on_active_partition_not_redirected(self):
        """Message on active partition 0 with existing key is processed normally."""
        ingress, networking, sequencer, state = _make_ingress_with_shadow_partitions()
        state.exists.return_value = True
        networking.decode_message.return_value = ("ycsb", "test_key", "read", (), 0)

        msg = _make_msg(partition=0)
        ingress.handle_message_from_kafka(msg)

        sequencer.sequence.assert_called_once()
        payload = sequencer.sequence.call_args[0][0]
        assert payload.partition == 0

    def test_redirect_preserves_kafka_offset_metadata(self):
        """Redirected messages must preserve kafka_offset and
        kafka_ingress_partition for offset tracking.

        Bug: if these aren't preserved, _advance_offsets can't track
        progress and messages are re-consumed after migration.
        """
        ingress, networking, sequencer, state = _make_ingress_with_shadow_partitions()
        networking.decode_message.return_value = ("ycsb", "test_key", "read", (), 6)
        ingress.registered_operators[("ycsb", 6)].which_partition = MagicMock(return_value=2)

        msg = _make_msg(partition=6, offset=42)
        ingress.handle_message_from_kafka(msg)

        payload = sequencer.sequence.call_args[0][0]
        assert payload.kafka_offset == 42
        assert payload.kafka_ingress_partition == 6

    def test_all_shadow_partitions_redirect(self):
        """Verify all shadow partitions (4-7) redirect correctly."""
        ingress, networking, sequencer, state = _make_ingress_with_shadow_partitions()

        for shadow_part in range(4, 8):
            sequencer.reset_mock()
            active_part = shadow_part % 4
            networking.decode_message.return_value = (
                "ycsb",
                f"key_{shadow_part}",
                "read",
                (),
                shadow_part,
            )
            ingress.registered_operators[("ycsb", shadow_part)].which_partition = MagicMock(return_value=active_part)

            msg = _make_msg(partition=shadow_part)
            ingress.handle_message_from_kafka(msg)

            sequencer.sequence.assert_called_once()
            payload = sequencer.sequence.call_args[0][0]
            assert payload.partition == active_part, f"Shadow partition {shadow_part} should redirect to {active_part}"


# ---------------------------------------------------------------------------
# Kafka consumer startup exception handling
# ---------------------------------------------------------------------------


class TestKafkaConsumerStartupExceptionHandling:
    """Tests that the Kafka consumer startup loop correctly catches both
    UnknownTopicOrPartitionError and KafkaConnectionError.

    Bug caught: Python 2 syntax `except A, B:` (which means `except A as B:`
    in Python 3) only catches A and silently shadows the name B. This caused
    KafkaConnectionError to crash the ingress during recovery, killing all
    message consumption for that worker.
    """

    @pytest.mark.asyncio
    async def test_kafka_connection_error_is_caught_and_retried(self):
        """KafkaConnectionError during consumer.start() should be caught
        and retried, not crash the ingress."""
        ingress, *_ = _make_ingress()
        mock_consumer = AsyncMock()
        # First call raises KafkaConnectionError, second succeeds
        mock_consumer.start = AsyncMock(
            side_effect=[KafkaConnectionError(), None],
        )
        # getmany raises CancelledError to exit the consumer loop cleanly
        mock_consumer.getmany = AsyncMock(side_effect=asyncio.CancelledError())
        mock_consumer.stop = AsyncMock()

        with patch("worker.ingress.styx_kafka_ingress.AIOKafkaConsumer", return_value=mock_consumer):
            with patch("worker.ingress.styx_kafka_ingress.asyncio.sleep", new_callable=AsyncMock):
                with pytest.raises(asyncio.CancelledError):
                    await ingress.start_kafka_consumer([], {})

        # Consumer should have been started twice (retry after error)
        assert mock_consumer.start.call_count == 2

    @pytest.mark.asyncio
    async def test_unknown_topic_error_is_caught_and_retried(self):
        """UnknownTopicOrPartitionError during consumer.start() should be
        caught and retried."""
        ingress, *_ = _make_ingress()
        mock_consumer = AsyncMock()
        mock_consumer.start = AsyncMock(
            side_effect=[UnknownTopicOrPartitionError(), None],
        )
        mock_consumer.getmany = AsyncMock(side_effect=asyncio.CancelledError())
        mock_consumer.stop = AsyncMock()

        with patch("worker.ingress.styx_kafka_ingress.AIOKafkaConsumer", return_value=mock_consumer):
            with patch("worker.ingress.styx_kafka_ingress.asyncio.sleep", new_callable=AsyncMock):
                with pytest.raises(asyncio.CancelledError):
                    await ingress.start_kafka_consumer([], {})

        assert mock_consumer.start.call_count == 2

    @pytest.mark.asyncio
    async def test_other_exceptions_propagate(self):
        """Non-Kafka exceptions should propagate, not be silently swallowed."""
        ingress, *_ = _make_ingress()
        mock_consumer = AsyncMock()
        mock_consumer.start = AsyncMock(side_effect=ValueError("unexpected"))

        with patch("worker.ingress.styx_kafka_ingress.AIOKafkaConsumer", return_value=mock_consumer):
            with pytest.raises(ValueError, match="unexpected"):
                await ingress.start_kafka_consumer([], {})

    def test_except_clause_catches_both_exception_types(self):
        """Verify the except clause syntax is correct (not Python 2 style).

        Python 2: `except A, B:` catches A and binds to B
        Python 3: `except A, B:` means `except A as B:` — only catches A!
        Correct:  `except (A, B):` catches both A and B

        This test inspects the source to prevent regressions from formatters
        or linters that might not catch this subtle syntax difference.
        """
        import ast
        import inspect
        import textwrap

        source = textwrap.dedent(inspect.getsource(StyxKafkaIngress.start_kafka_consumer))
        tree = ast.parse(source)

        # Find all except handlers in the function
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler) and node.type is not None:
                # If the handler catches a tuple of exceptions, node.type
                # is an ast.Tuple. If it's Python 2 style `except A, B:`,
                # node.type is just a Name (only catches A) and node.name
                # is set to the string "B".
                if isinstance(node.type, ast.Name) and node.type.id == "UnknownTopicOrPartitionError":
                    # This means: except UnknownTopicOrPartitionError as <something>
                    # i.e., KafkaConnectionError is NOT caught — this is the bug
                    pytest.fail(
                        "except clause uses Python 2 syntax "
                        "'except UnknownTopicOrPartitionError, KafkaConnectionError:' "
                        "which only catches UnknownTopicOrPartitionError. "
                        "Use 'except (UnknownTopicOrPartitionError, KafkaConnectionError):' "
                        "to catch both.",
                    )
