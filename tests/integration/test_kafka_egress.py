"""Integration tests for StyxKafkaBatchEgress against a real Kafka container."""

import asyncio

from aiokafka import AIOKafkaConsumer, TopicPartition
import pytest

pytestmark = pytest.mark.integration


class TestEgressSendToKafka:
    async def test_send_and_send_batch_produce_messages(self, kafka_container, create_topic):
        """send() + send_batch() should produce messages consumable from Kafka."""
        from worker.egress.styx_kafka_batch_egress import StyxKafkaBatchEgress

        create_topic("egress_batch_op--OUT", num_partitions=2)
        await asyncio.sleep(1)

        offsets = {("egress_batch_op", 0): -1, ("egress_batch_op", 1): -1}
        egress = StyxKafkaBatchEgress(offsets)
        await egress.start(worker_id=0)

        try:
            await egress.send(b"key1", b"value1", "egress_batch_op", 0)
            await egress.send(b"key2", b"value2", "egress_batch_op", 0)
            await egress.send_batch()
        finally:
            await egress.stop()

        # Verify by consuming
        bootstrap = kafka_container["bootstrap_server"]
        consumer = AIOKafkaConsumer(
            bootstrap_servers=[bootstrap],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        tp = TopicPartition("egress_batch_op--OUT", 0)
        consumer.assign([tp])
        await consumer.start()
        try:
            messages = await consumer.getmany(timeout_ms=5000)
            all_msgs = [m for msgs in messages.values() for m in msgs]
            assert len(all_msgs) >= 2
            keys = {m.key for m in all_msgs}
            assert b"key1" in keys
            assert b"key2" in keys
        finally:
            await consumer.stop()

    async def test_send_batch_updates_offsets(self, kafka_container, create_topic):
        """send_batch() should update topic_partition_output_offsets."""
        from worker.egress.styx_kafka_batch_egress import StyxKafkaBatchEgress

        create_topic("egress_offsets_op--OUT", num_partitions=1)
        await asyncio.sleep(1)

        offsets = {("egress_offsets_op", 0): -1}
        egress = StyxKafkaBatchEgress(offsets)
        await egress.start(worker_id=0)

        try:
            await egress.send(b"k1", b"v1", "egress_offsets_op", 0)
            await egress.send_batch()
            assert offsets[("egress_offsets_op", 0)] >= 0
        finally:
            await egress.stop()

    async def test_send_immediate_produces_and_updates_offset(self, kafka_container, create_topic):
        """send_immediate() should produce synchronously and update offsets."""
        from worker.egress.styx_kafka_batch_egress import StyxKafkaBatchEgress

        create_topic("egress_imm_op--OUT", num_partitions=1)
        await asyncio.sleep(1)

        offsets = {("egress_imm_op", 0): -1}
        egress = StyxKafkaBatchEgress(offsets)
        await egress.start(worker_id=0)

        try:
            await egress.send_immediate(b"imm_key", b"imm_val", "egress_imm_op", 0)
            assert offsets[("egress_imm_op", 0)] >= 0
        finally:
            await egress.stop()

        # Verify the message
        bootstrap = kafka_container["bootstrap_server"]
        consumer = AIOKafkaConsumer(
            bootstrap_servers=[bootstrap],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        tp = TopicPartition("egress_imm_op--OUT", 0)
        consumer.assign([tp])
        await consumer.start()
        try:
            messages = await consumer.getmany(timeout_ms=5000)
            all_msgs = [m for msgs in messages.values() for m in msgs]
            assert len(all_msgs) >= 1
            assert all_msgs[0].key == b"imm_key"
        finally:
            await consumer.stop()

    async def test_send_message_to_topic(self, kafka_container, create_topic):
        """send_message_to_topic() should produce to an arbitrary topic."""
        from worker.egress.styx_kafka_batch_egress import StyxKafkaBatchEgress

        create_topic("egress_arb_topic", num_partitions=1)
        await asyncio.sleep(1)

        offsets = {("dummy", 0): -1}
        egress = StyxKafkaBatchEgress(offsets)
        await egress.start(worker_id=0)

        try:
            metadata = await egress.send_message_to_topic(b"tkey", b"tval", "egress_arb_topic")
            assert metadata.topic == "egress_arb_topic"
            assert metadata.offset >= 0
        finally:
            await egress.stop()
