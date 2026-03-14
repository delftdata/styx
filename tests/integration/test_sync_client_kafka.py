"""Integration tests for SyncStyxClient Kafka produce path."""

import time

from confluent_kafka import Consumer
import pytest
from styx.common.local_state_backends import LocalStateBackend
from styx.common.operator import Operator
from styx.common.stateflow_graph import StateflowGraph

pytestmark = pytest.mark.integration


def _dummy_func(ctx, key):
    pass


class TestSyncClientKafkaProduce:
    def test_send_event_produces_to_kafka(self, kafka_container, create_topic):
        """open(consume=False) + send_event() should produce a message to Kafka."""
        from styx.client.sync_client import SyncStyxClient

        topic_name = "sync_client_op"
        create_topic(topic_name, num_partitions=2)
        time.sleep(1)

        bootstrap = kafka_container["bootstrap_server"]
        client = SyncStyxClient(
            styx_coordinator_adr="localhost",
            styx_coordinator_port=8888,
            kafka_url=bootstrap,
        )

        g = StateflowGraph("test-sync", operator_state_backend=LocalStateBackend.DICT)
        op = Operator(topic_name, n_partitions=2)
        op.register(_dummy_func)
        g.add_operator(op)
        client.set_graph(g)

        client.open(consume=False)
        try:
            future = client.send_event(op, "user_1", _dummy_func)
            client.flush()
            assert future is not None
        finally:
            client.close()

        # Verify via confluent-kafka consumer
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test-verify-sync",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic_name])
        msgs = []
        deadline = time.time() + 10
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                msgs.append(msg)
                break
        consumer.close()
        assert len(msgs) >= 1

    def test_send_multiple_events(self, kafka_container, create_topic):
        """Multiple send_event() calls should all produce to Kafka."""
        from styx.client.sync_client import SyncStyxClient

        topic_name = "sync_multi_op"
        create_topic(topic_name, num_partitions=1)
        time.sleep(1)

        bootstrap = kafka_container["bootstrap_server"]
        client = SyncStyxClient(
            styx_coordinator_adr="localhost",
            styx_coordinator_port=8888,
            kafka_url=bootstrap,
        )

        g = StateflowGraph("test-sync-multi", operator_state_backend=LocalStateBackend.DICT)
        op = Operator(topic_name, n_partitions=1)
        op.register(_dummy_func)
        g.add_operator(op)
        client.set_graph(g)

        client.open(consume=False)
        try:
            for i in range(5):
                client.send_event(op, f"user_{i}", _dummy_func)
            client.flush()
        finally:
            client.close()

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test-verify-sync-multi",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic_name])
        msgs = []
        deadline = time.time() + 10
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                msgs.append(msg)
                if len(msgs) >= 5:
                    break
        consumer.close()
        assert len(msgs) >= 5
