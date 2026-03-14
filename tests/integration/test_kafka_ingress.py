"""Integration tests for StyxKafkaIngress against a real Kafka container."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from aiokafka import AIOKafkaProducer, TopicPartition
import pytest
from styx.common.base_networking import BaseNetworking
from styx.common.message_types import MessageType
from styx.common.serialization import Serializer

pytestmark = pytest.mark.integration


class TestIngressConsumesFromKafka:
    async def test_ingress_receives_and_sequences_message(self, kafka_container, create_topic):
        """Produce a ClientMsg to Kafka; verify ingress calls sequencer.sequence()."""
        from worker.ingress.styx_kafka_ingress import StyxKafkaIngress

        topic_name = "ingress_test_op"
        create_topic(topic_name, num_partitions=1)
        await asyncio.sleep(2)

        bootstrap = kafka_container["bootstrap_server"]

        # Mock dependencies
        mock_networking = MagicMock()
        mock_networking.get_msg_type = MagicMock(return_value=MessageType.ClientMsg)
        mock_networking.decode_message = MagicMock(return_value=(topic_name, "user_1", "my_func", (), 0))
        mock_networking.send_message = AsyncMock()

        mock_sequencer = MagicMock()
        mock_sequencer.lock = asyncio.Lock()
        mock_sequencer.sequence = MagicMock()

        mock_state = MagicMock()
        mock_state.exists = MagicMock(return_value=True)

        mock_operator = MagicMock()
        registered_operators = {(topic_name, 0): mock_operator}

        ingress = StyxKafkaIngress(
            networking=mock_networking,
            sequencer=mock_sequencer,
            state=mock_state,
            registered_operators=registered_operators,
            worker_id=0,
            kafka_url=bootstrap,
            epoch_interval_ms=100,
            sequence_max_size=10,
        )

        # Produce a message to the topic
        producer = AIOKafkaProducer(bootstrap_servers=[bootstrap])
        await producer.start()
        try:
            value = BaseNetworking.encode_message(
                msg=(topic_name, "user_1", "my_func", (), 0),
                msg_type=MessageType.ClientMsg,
                serializer=Serializer.MSGPACK,
            )
            await producer.send_and_wait(topic_name, key=b"req_001", value=value, partition=0)
        finally:
            await producer.stop()

        # Start ingress and let it consume
        tp = TopicPartition(topic_name, 0)
        await ingress.start([tp], {(topic_name, 0): -1})

        # Wait for the message to be processed
        await asyncio.sleep(3)
        await ingress.stop()

        # Verify sequencer.sequence was called
        assert mock_sequencer.sequence.call_count >= 1
