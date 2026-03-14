"""Additional coverage tests for worker/egress/styx_kafka_batch_egress.py

Covers: send with unstarted event, send_immediate dedup path,
send_message_to_topic, stop.
"""

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
# send — waits for started event
# ---------------------------------------------------------------------------


class TestSendWaitsForStarted:
    @pytest.mark.asyncio
    async def test_send_waits_for_started(self):
        e = _egress()
        mock_producer = MagicMock()
        future = asyncio.Future()
        future.set_result(MagicMock())
        mock_producer.send = AsyncMock(return_value=future)
        e.kafka_egress_producer = mock_producer

        # Set started after a brief delay
        async def set_started():
            await asyncio.sleep(0.01)
            e.started.set()

        task = asyncio.create_task(set_started())
        await e.send(b"key1", b"value1", "users", 0)
        await task
        assert len(e.batch) == 1


# ---------------------------------------------------------------------------
# send_immediate — dedup and normal paths
# ---------------------------------------------------------------------------


class TestSendImmediate:
    @pytest.mark.asyncio
    async def test_send_immediate_normal(self):
        e = _egress()
        e.started.set()
        meta = MagicMock()
        meta.offset = 10
        mock_producer = MagicMock()
        mock_producer.send_and_wait = AsyncMock(return_value=meta)
        e.kafka_egress_producer = mock_producer

        await e.send_immediate(b"key1", b"value1", "users", 0)
        assert e.topic_partition_output_offsets[("users", 0)] == 10

    @pytest.mark.asyncio
    async def test_send_immediate_dedup(self):
        e = _egress()
        e.started.set()
        mock_producer = MagicMock()
        e.kafka_egress_producer = mock_producer

        tp = TopicPartition("users--OUT", 0)
        e.messages_sent_before_recovery[tp].add(b"key1")

        await e.send_immediate(b"key1", b"value1", "users", 0)
        mock_producer.send_and_wait.assert_not_called()
        assert b"key1" not in e.messages_sent_before_recovery[tp]


# ---------------------------------------------------------------------------
# send_message_to_topic
# ---------------------------------------------------------------------------


class TestSendMessageToTopic:
    @pytest.mark.asyncio
    async def test_sends_to_named_topic(self):
        e = _egress()
        e.started.set()
        meta = MagicMock()
        meta.offset = 5
        mock_producer = MagicMock()
        mock_producer.send_and_wait = AsyncMock(return_value=meta)
        e.kafka_egress_producer = mock_producer

        result = await e.send_message_to_topic(b"key", b"msg", "my-topic")
        mock_producer.send_and_wait.assert_called_once_with("my-topic", key=b"key", value=b"msg")
        assert result is meta

    @pytest.mark.asyncio
    async def test_send_message_to_topic_waits_for_started(self):
        e = _egress()
        meta = MagicMock()
        mock_producer = MagicMock()
        mock_producer.send_and_wait = AsyncMock(return_value=meta)
        e.kafka_egress_producer = mock_producer

        async def set_started():
            await asyncio.sleep(0.01)
            e.started.set()

        task = asyncio.create_task(set_started())
        result = await e.send_message_to_topic(b"key", b"msg", "topic")
        await task
        assert result is meta


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


class TestStop:
    @pytest.mark.asyncio
    async def test_stop_sends_batch_and_stops(self):
        e = _egress()
        mock_producer = MagicMock()
        mock_producer.stop = AsyncMock()
        e.kafka_egress_producer = mock_producer

        await e.stop()
        mock_producer.stop.assert_called_once()


# ---------------------------------------------------------------------------
# send_batch with multiple results
# ---------------------------------------------------------------------------


class TestSendBatchMultiple:
    @pytest.mark.asyncio
    async def test_batch_multiple_partitions(self):
        e = _egress()

        meta1 = MagicMock(topic="users--OUT", partition=0, offset=10)
        meta2 = MagicMock(topic="users--OUT", partition=1, offset=20)

        f1 = asyncio.Future()
        f1.set_result(meta1)
        f2 = asyncio.Future()
        f2.set_result(meta2)
        e.batch = [f1, f2]

        await e.send_batch()
        assert e.topic_partition_output_offsets[("users", 0)] == 10
        assert e.topic_partition_output_offsets[("users", 1)] == 20
        assert e.batch == []
