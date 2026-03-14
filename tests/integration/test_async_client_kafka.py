"""Integration tests for AsyncStyxClient Kafka produce path."""

import asyncio
import contextlib

from aiokafka import AIOKafkaConsumer, TopicPartition
import pytest
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph

pytestmark = pytest.mark.integration


def _dummy_func(ctx, key):
    pass


class TestAsyncClientKafkaProduce:
    async def test_send_event_produces_to_kafka(self, kafka_container, create_topic):
        """open(consume=False) + send_event() should produce a message to the operator topic."""
        from styx.client.async_client import AsyncStyxClient

        topic_name = "async_client_op"
        create_topic(topic_name, num_partitions=2)
        await asyncio.sleep(1)

        bootstrap = kafka_container["bootstrap_server"]
        client = AsyncStyxClient(
            styx_coordinator_adr="localhost",
            styx_coordinator_port=8888,
            kafka_url=bootstrap,
        )

        # Build graph and set directly (bypass coordinator)
        g = StateflowGraph("test-app", operator_state_backend=LocalStateBackend.DICT)
        op = Operator(topic_name, n_partitions=2)
        op.register(_dummy_func)
        g.add_operator(op)
        client.set_graph(g)

        await client.open(consume=False)
        try:
            future = await client.send_event(op, "user_1", _dummy_func)
            await client.flush()
            assert future is not None
        finally:
            # Stop producer (can't call close() as it expects consumers to exist)
            await client._kafka_producer.stop()
            # Cancel metadata task
            if client._metadata_consumer_task:
                client._metadata_consumer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await client._metadata_consumer_task
            if client._metadata_consumer:
                await client._metadata_consumer.stop()

        # Verify message landed in Kafka
        consumer = AIOKafkaConsumer(
            bootstrap_servers=[bootstrap],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        consumer.assign([TopicPartition(topic_name, p) for p in range(2)])
        await consumer.start()
        try:
            msgs = await consumer.getmany(timeout_ms=5000)
            all_msgs = [m for ms in msgs.values() for m in ms]
            assert len(all_msgs) >= 1
        finally:
            await consumer.stop()

    async def test_send_multiple_events(self, kafka_container, create_topic):
        """Multiple send_event() calls should all land on Kafka."""
        from styx.client.async_client import AsyncStyxClient

        topic_name = "async_multi_op"
        create_topic(topic_name, num_partitions=1)
        await asyncio.sleep(1)

        bootstrap = kafka_container["bootstrap_server"]
        client = AsyncStyxClient(
            styx_coordinator_adr="localhost",
            styx_coordinator_port=8888,
            kafka_url=bootstrap,
        )

        g = StateflowGraph("test-multi", operator_state_backend=LocalStateBackend.DICT)
        op = Operator(topic_name, n_partitions=1)
        op.register(_dummy_func)
        g.add_operator(op)
        client.set_graph(g)

        await client.open(consume=False)
        try:
            for i in range(5):
                await client.send_event(op, f"user_{i}", _dummy_func)
            await client.flush()
        finally:
            await client._kafka_producer.stop()
            if client._metadata_consumer_task:
                client._metadata_consumer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await client._metadata_consumer_task
            if client._metadata_consumer:
                await client._metadata_consumer.stop()

        consumer = AIOKafkaConsumer(
            bootstrap_servers=[bootstrap],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        consumer.assign([TopicPartition(topic_name, 0)])
        await consumer.start()
        try:
            msgs = await consumer.getmany(timeout_ms=5000)
            all_msgs = [m for ms in msgs.values() for m in ms]
            assert len(all_msgs) >= 5
        finally:
            await consumer.stop()
