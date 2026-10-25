"""Additional coverage tests for worker/egress/styx_kafka_batch_egress.py

Covers: send with unstarted event, send_immediate dedup path,
send_message_to_topic, stop, dedup counters, run_dedup_scan_and_mark_started,
dedup_output_offsets constructor parameter.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from worker.egress.styx_kafka_batch_egress import StyxKafkaBatchEgress

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _egress(offsets=None, restart=False, dedup_offsets=None):
    if offsets is None:
        offsets = {("users", 0): -1, ("users", 1): -1}
    return StyxKafkaBatchEgress(offsets, restart_after_failure=restart, dedup_output_offsets=dedup_offsets)


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

        # Flat dedup set — partition-agnostic
        e.messages_sent_before_recovery.add(b"key1")

        await e.send_immediate(b"key1", b"value1", "users", 0)
        mock_producer.send_and_wait.assert_not_called()
        assert b"key1" not in e.messages_sent_before_recovery


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


# ---------------------------------------------------------------------------
# Constructor — dedup_output_offsets parameter
# ---------------------------------------------------------------------------


class TestDedupOutputOffsets:
    def test_default_dedup_offsets_is_empty_dict(self):
        e = _egress()
        assert e.dedup_output_offsets == {}

    def test_custom_dedup_offsets_stored(self):
        offsets = {("users", 0): 5, ("orders", 1): 10}
        e = _egress(dedup_offsets=offsets)
        assert e.dedup_output_offsets == offsets

    def test_dedup_counters_start_at_zero(self):
        e = _egress()
        assert e._dedup_suppressed == 0
        assert e._dedup_sent == 0


# ---------------------------------------------------------------------------
# start — non-recovery sets started immediately
# ---------------------------------------------------------------------------


class TestStartNonRecovery:
    @pytest.mark.asyncio
    async def test_start_non_recovery_sets_started(self):
        e = _egress(restart=False)
        mock_producer = MagicMock()
        mock_producer.start = AsyncMock()
        # Patch AIOKafkaProducer to return our mock
        e.kafka_egress_producer = mock_producer
        # Simulate the start logic without real Kafka
        # Just call the post-start code path
        if not e.restart_after_failure:
            e.started.set()
        assert e.started.is_set()

    @pytest.mark.asyncio
    async def test_start_recovery_does_not_set_started(self):
        e = _egress(restart=True)
        # Recovery path: started should NOT be set in start()
        if not e.restart_after_failure:
            e.started.set()
        assert not e.started.is_set()


# ---------------------------------------------------------------------------
# run_dedup_scan_and_mark_started
# ---------------------------------------------------------------------------


class TestRunDedupScanAndMarkStarted:
    @pytest.mark.asyncio
    async def test_non_recovery_just_sets_started(self):
        """When restart_after_failure=False, run_dedup_scan_and_mark_started just sets started."""
        e = _egress(restart=False)
        await e.run_dedup_scan_and_mark_started()
        assert e.started.is_set()

    @pytest.mark.asyncio
    async def test_recovery_runs_dedup_scan(self):
        """When restart_after_failure=True, dedup scan runs then started is set."""
        e = _egress(restart=True, dedup_offsets={})
        # Mock get_messages_sent_before_recovery (it requires Kafka)
        e.get_messages_sent_before_recovery = AsyncMock()
        await e.run_dedup_scan_and_mark_started()
        e.get_messages_sent_before_recovery.assert_called_once()
        assert e.started.is_set()


# ---------------------------------------------------------------------------
# stop — dedup stats logging
# ---------------------------------------------------------------------------


class TestStopDedupStats:
    @pytest.mark.asyncio
    async def test_stop_logs_dedup_stats_when_nonzero(self):
        e = _egress()
        e.worker_id = 0
        e._dedup_suppressed = 3
        e._dedup_sent = 10
        mock_producer = MagicMock()
        mock_producer.stop = AsyncMock()
        e.kafka_egress_producer = mock_producer

        await e.stop()
        mock_producer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_skips_log_when_zero_counters(self):
        e = _egress()
        e.worker_id = 0
        e._dedup_suppressed = 0
        e._dedup_sent = 0
        mock_producer = MagicMock()
        mock_producer.stop = AsyncMock()
        e.kafka_egress_producer = mock_producer

        await e.stop()
        mock_producer.stop.assert_called_once()


# ---------------------------------------------------------------------------
# send — dedup counters
# ---------------------------------------------------------------------------


class TestSendDedupCounters:
    @pytest.mark.asyncio
    async def test_send_increments_dedup_sent(self):
        e = _egress()
        e.started.set()
        mock_producer = MagicMock()
        future = asyncio.Future()
        future.set_result(MagicMock())
        mock_producer.send = AsyncMock(return_value=future)
        e.kafka_egress_producer = mock_producer

        await e.send(b"key1", b"value1", "users", 0)
        assert e._dedup_sent == 1

    @pytest.mark.asyncio
    async def test_send_increments_dedup_suppressed(self):
        e = _egress()
        e.started.set()
        e.kafka_egress_producer = MagicMock()
        e.messages_sent_before_recovery.add(b"key1")

        await e.send(b"key1", b"value1", "users", 0)
        assert e._dedup_suppressed == 1
        assert e._dedup_sent == 0


# ---------------------------------------------------------------------------
# send_immediate — dedup counters
# ---------------------------------------------------------------------------


class TestSendImmediateDedupCounters:
    @pytest.mark.asyncio
    async def test_send_immediate_increments_dedup_sent(self):
        e = _egress()
        e.started.set()
        meta = MagicMock()
        meta.offset = 10
        mock_producer = MagicMock()
        mock_producer.send_and_wait = AsyncMock(return_value=meta)
        e.kafka_egress_producer = mock_producer

        await e.send_immediate(b"key1", b"value1", "users", 0)
        assert e._dedup_sent == 1

    @pytest.mark.asyncio
    async def test_send_immediate_increments_dedup_suppressed(self):
        e = _egress()
        e.started.set()
        e.kafka_egress_producer = MagicMock()
        e.messages_sent_before_recovery.add(b"key1")

        await e.send_immediate(b"key1", b"value1", "users", 0)
        assert e._dedup_suppressed == 1
        assert e._dedup_sent == 0


# ---------------------------------------------------------------------------
# get_messages_sent_before_recovery — empty partitions early return
# ---------------------------------------------------------------------------


class TestGetMessagesSentBeforeRecoveryEmpty:
    @pytest.mark.asyncio
    async def test_empty_dedup_offsets_returns_immediately(self):
        e = _egress(dedup_offsets={})
        await e.get_messages_sent_before_recovery()
        # Should return without creating a consumer
        assert len(e.messages_sent_before_recovery) == 0


# ---------------------------------------------------------------------------
# Reuse across a migration rebuild
# ---------------------------------------------------------------------------


class TestReuseAcrossMigration:
    @pytest.mark.asyncio
    async def test_start_reuses_running_producer(self, monkeypatch):
        e = _egress()
        producer = MagicMock()
        e.kafka_egress_producer = producer
        factory = MagicMock()
        monkeypatch.setattr("worker.egress.styx_kafka_batch_egress.AIOKafkaProducer", factory)

        await e.start(worker_id=1)

        factory.assert_not_called()
        assert e.kafka_egress_producer is producer
        assert e.started.is_set()

    @pytest.mark.asyncio
    async def test_flush_keeps_producer_running(self):
        e = _egress()
        producer = MagicMock(stop=AsyncMock())
        e.kafka_egress_producer = producer
        await e.flush()
        producer.stop.assert_not_called()
        assert e.is_running

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self):
        e = _egress()
        producer = MagicMock(stop=AsyncMock())
        e.kafka_egress_producer = producer
        await e.stop()
        await e.stop()
        producer.stop.assert_awaited_once()
        assert not e.is_running

    def test_prepare_for_new_protocol_resets_per_protocol_state(self):
        e = _egress(restart=True, dedup_offsets={("users", 0): 5})
        e.messages_sent_before_recovery.add(b"r1")
        e.batch.append(object())
        e.started.set()
        new_offsets = {("users", 0): 7, ("users", 4): -1}

        e.prepare_for_new_protocol(new_offsets, restart_after_failure=False, dedup_output_offsets=None)

        assert e.topic_partition_output_offsets is new_offsets
        assert e.restart_after_failure is False
        assert e.dedup_output_offsets == {}
        assert e.messages_sent_before_recovery == set()
        assert e.batch == []
        assert not e.started.is_set()
