"""Unit tests for worker/egress/styx_kafka_batch_egress.py"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from aiokafka import TopicPartition
import pytest

from worker.egress.styx_kafka_batch_egress import StyxKafkaBatchEgress

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _egress(offsets=None, restart=False):
    if offsets is None:
        offsets = {("users", 0): -1, ("users", 1): -1}
    return StyxKafkaBatchEgress(offsets, restart_after_failure=restart)


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestEgressInit:
    def test_defaults(self):
        e = _egress()
        assert e.kafka_egress_producer is None
        assert e.batch == []
        assert e.restart_after_failure is False
        assert not e.started.is_set()

    def test_offsets_stored(self):
        offsets = {("op", 0): 5, ("op", 1): 10}
        e = _egress(offsets)
        assert e.topic_partition_output_offsets == offsets

    def test_restart_flag(self):
        e = _egress(restart=True)
        assert e.restart_after_failure is True


# ---------------------------------------------------------------------------
# clear_messages_sent_before_recovery
# ---------------------------------------------------------------------------


class TestClearMessagesSentBeforeRecovery:
    def test_clears(self):
        e = _egress()
        tp = TopicPartition("users--OUT", 0)
        e.messages_sent_before_recovery[tp].add(b"key1")
        e.clear_messages_sent_before_recovery()
        assert len(e.messages_sent_before_recovery) == 0


# ---------------------------------------------------------------------------
# send
# ---------------------------------------------------------------------------


class TestEgressSend:
    @pytest.mark.asyncio
    async def test_send_adds_to_batch(self):
        e = _egress()
        e.started.set()
        mock_producer = MagicMock()
        future = asyncio.Future()
        future.set_result(MagicMock())
        mock_producer.send = AsyncMock(return_value=future)
        e.kafka_egress_producer = mock_producer

        await e.send(b"key1", b"value1", "users", 0)
        assert len(e.batch) == 1
        mock_producer.send.assert_called_once_with(
            "users--OUT",
            key=b"key1",
            value=b"value1",
            partition=0,
        )

    @pytest.mark.asyncio
    async def test_send_skips_already_sent(self):
        e = _egress()
        e.started.set()
        mock_producer = MagicMock()
        e.kafka_egress_producer = mock_producer

        tp = TopicPartition("users--OUT", 0)
        e.messages_sent_before_recovery[tp].add(b"key1")

        await e.send(b"key1", b"value1", "users", 0)
        assert len(e.batch) == 0  # skipped
        mock_producer.send.assert_not_called()
        assert b"key1" not in e.messages_sent_before_recovery[tp]


# ---------------------------------------------------------------------------
# send_batch
# ---------------------------------------------------------------------------


class TestSendBatch:
    @pytest.mark.asyncio
    async def test_empty_batch_noop(self):
        e = _egress()
        await e.send_batch()  # should not raise

    @pytest.mark.asyncio
    async def test_batch_gathers_and_updates_offsets(self):
        e = _egress()

        # Simulate futures that resolve to RecordMetadata-like objects
        meta1 = MagicMock()
        meta1.topic = "users--OUT"
        meta1.partition = 0
        meta1.offset = 42

        f1 = asyncio.Future()
        f1.set_result(meta1)
        e.batch = [f1]

        await e.send_batch()
        assert e.topic_partition_output_offsets[("users", 0)] == 42
        assert e.batch == []


# ---------------------------------------------------------------------------
# process_messages_sent_before_recovery
# ---------------------------------------------------------------------------


class TestProcessMessagesSentBeforeRecovery:
    def test_adds_keys(self):
        e = _egress()
        tp = TopicPartition("users--OUT", 0)
        current_offsets = {tp: 10}
        all_done = {tp: False}

        msg1 = MagicMock()
        msg1.topic = "users--OUT"
        msg1.partition = 0
        msg1.offset = 5
        msg1.key = b"key1"

        msg2 = MagicMock()
        msg2.topic = "users--OUT"
        msg2.partition = 0
        msg2.offset = 9  # >= current_offsets[tp] - 1
        msg2.key = b"key2"

        e.process_messages_sent_before_recovery([msg1, msg2], current_offsets, all_done)
        assert b"key1" in e.messages_sent_before_recovery[tp]
        assert b"key2" in e.messages_sent_before_recovery[tp]
        assert all_done[tp] is True  # msg2 offset >= 10 - 1
