"""Additional coverage tests for async_client.py and sync_client.py

Covers: get_operator_partition, close, submit_dataflow, update_dataflow,
notify_init_data_complete, delivery_callback, flush, set_graph.
"""

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from styx.common.local_state_backends import LocalStateBackend
from styx.common.message_types import MessageType
from styx.common.operator import Operator
from styx.common.serialization import Serializer
from styx.common.stateflow_graph import StateflowGraph

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _graph(name="test-app", n_partitions=2):
    g = StateflowGraph(name, operator_state_backend=LocalStateBackend.DICT)
    op = Operator("users", n_partitions)
    g.add_operator(op)
    return g


# ===========================================================================
# AsyncStyxClient tests
# ===========================================================================

from styx.client.async_client import AsyncStyxClient


def _async_client():
    return AsyncStyxClient("localhost", 8888, "localhost:9092")


class TestAsyncClientGetOperatorPartition:
    @pytest.mark.asyncio
    async def test_waits_for_graph(self):
        c = _async_client()
        graph = _graph()
        c.set_graph(graph)
        op = Operator("users", 2)
        partition = await c.get_operator_partition("some_key", op)
        assert isinstance(partition, int)
        assert 0 <= partition < 2

    @pytest.mark.asyncio
    async def test_graph_not_set_blocks(self):
        c = _async_client()

        async def set_graph_later():
            await asyncio.sleep(0.01)
            c.set_graph(_graph())

        task = asyncio.create_task(set_graph_later())
        op = Operator("users", 2)
        partition = await c.get_operator_partition("key", op)
        await task
        assert isinstance(partition, int)


class TestAsyncClientClose:
    @pytest.mark.asyncio
    async def test_close_stops_all(self):
        c = _async_client()
        c._kafka_producer = MagicMock()
        c._kafka_producer.flush = AsyncMock()
        c._kafka_producer.stop = AsyncMock()
        c._result_consumer = MagicMock()
        c._result_consumer.stop = AsyncMock()
        c._metadata_consumer = MagicMock()
        c._metadata_consumer.stop = AsyncMock()
        c._result_consumer_task = asyncio.create_task(asyncio.sleep(100))
        c._metadata_consumer_task = asyncio.create_task(asyncio.sleep(100))

        await c.close()
        c._kafka_producer.stop.assert_called_once()
        c._result_consumer.stop.assert_called_once()
        c._metadata_consumer.stop.assert_called_once()


class TestAsyncClientSubmitDataflow:
    @pytest.mark.asyncio
    async def test_submit_sends_graph(self):
        c = _async_client()
        c._networking_manager = MagicMock()
        c._networking_manager.send_message = AsyncMock()

        graph = _graph()
        await c.submit_dataflow(graph)

        c._networking_manager.send_message.assert_called_once()
        call_kwargs = c._networking_manager.send_message.call_args
        assert call_kwargs[1]["msg_type"] == MessageType.SendExecutionGraph
        assert c.graph_known_event.is_set()
        assert c._current_active_graph is graph


class TestAsyncClientUpdateDataflow:
    @pytest.mark.asyncio
    async def test_update_sends_graph(self):
        c = _async_client()
        c._networking_manager = MagicMock()
        c._networking_manager.send_message = AsyncMock()

        graph = _graph()
        await c.update_dataflow(graph)

        c._networking_manager.send_message.assert_called_once()
        call_kwargs = c._networking_manager.send_message.call_args
        assert call_kwargs[1]["msg_type"] == MessageType.UpdateExecutionGraph


class TestAsyncClientNotifyInitDataComplete:
    @pytest.mark.asyncio
    async def test_sends_init_data_complete(self):
        c = _async_client()
        c._networking_manager = MagicMock()
        c._networking_manager.send_message = AsyncMock()

        await c.notify_init_data_complete()
        c._networking_manager.send_message.assert_called_once()
        call_kwargs = c._networking_manager.send_message.call_args
        assert call_kwargs[1]["msg_type"] == MessageType.InitDataComplete
        assert call_kwargs[1]["serializer"] == Serializer.NONE


class TestAsyncClientSetGraph:
    def test_set_graph(self):
        c = _async_client()
        graph = _graph()
        c.set_graph(graph)
        assert c._current_active_graph is graph
        assert c.graph_known_event.is_set()


class TestAsyncClientFlush:
    @pytest.mark.asyncio
    async def test_flush(self):
        c = _async_client()
        c._kafka_producer = MagicMock()
        c._kafka_producer.flush = AsyncMock()
        await c.flush()
        c._kafka_producer.flush.assert_called_once()


# ===========================================================================
# SyncStyxClient tests
# ===========================================================================

from styx.client.sync_client import SyncStyxClient


def _sync_client():
    return SyncStyxClient("localhost", 8888, "localhost:9092")


class TestSyncClientGetOperatorPartition:
    def test_returns_partition(self):
        c = _sync_client()
        c.set_graph(_graph())
        op = Operator("users", 2)
        partition = c.get_operator_partition("some_key", op)
        assert isinstance(partition, int)

    def test_waits_for_graph(self):
        c = _sync_client()

        def set_graph_later():
            import time

            time.sleep(0.01)
            c.set_graph(_graph())

        t = threading.Thread(target=set_graph_later)
        t.start()
        op = Operator("users", 2)
        partition = c.get_operator_partition("key", op)
        t.join()
        assert isinstance(partition, int)


class TestSyncClientClose:
    def test_close(self):
        c = _sync_client()
        c._kafka_producer = MagicMock()
        c._kafka_producer.flush.return_value = 0
        c.close()
        assert c.running_result_consumer is False
        assert c.running_metadata_consumer is False
        assert c.running_polling_thread is False


class TestSyncClientSubmitDataflow:
    def test_submit_sends_via_socket(self):
        c = _sync_client()
        graph = _graph()
        mock_socket = MagicMock()

        with patch("styx.client.sync_client.socket.socket", return_value=mock_socket):
            c.submit_dataflow(graph)

        mock_socket.connect.assert_called_once_with(("localhost", 8888))
        mock_socket.send.assert_called_once()
        mock_socket.close.assert_called_once()
        assert c.graph_known_event.is_set()


class TestSyncClientUpdateDataflow:
    def test_update_sends_via_socket(self):
        c = _sync_client()
        graph = _graph()
        mock_socket = MagicMock()

        with patch("styx.client.sync_client.socket.socket", return_value=mock_socket):
            c.update_dataflow(graph)

        mock_socket.connect.assert_called_once_with(("localhost", 8888))
        mock_socket.send.assert_called_once()
        mock_socket.close.assert_called_once()


class TestSyncClientNotifyInitDataComplete:
    def test_sends_via_socket(self):
        c = _sync_client()
        mock_socket = MagicMock()

        with patch("styx.client.sync_client.socket.socket", return_value=mock_socket):
            c.notify_init_data_complete()

        mock_socket.connect.assert_called_once()
        mock_socket.send.assert_called_once()
        mock_socket.close.assert_called_once()


class TestSyncClientDeliveryCallback:
    def test_success_callback(self):
        c = _sync_client()
        msg = MagicMock()
        msg.key.return_value = b"req1"
        msg.timestamp.return_value = (0, 12345)

        c.delivery_callback(None, msg)
        assert c._delivery_timestamps[b"req1"] == 12345

    def test_error_callback(self):
        c = _sync_client()
        msg = MagicMock()
        msg.key.return_value = b"req1"

        c.delivery_callback("some error", msg)
        assert b"req1" not in c._delivery_timestamps

    def test_success_with_future(self):
        c = _sync_client()
        from styx.client.styx_future import StyxFuture

        future = StyxFuture(request_id=b"req1")
        msg = MagicMock()
        msg.key.return_value = b"req1"
        msg.timestamp.return_value = (0, 12345)

        c.delivery_callback(None, msg, future=future)
        assert c._delivery_timestamps[b"req1"] == 12345
        assert future._val.in_timestamp == 12345


class TestSyncClientFlush:
    def test_flush_success(self):
        c = _sync_client()
        c._kafka_producer = MagicMock()
        c._kafka_producer.flush.return_value = 0
        c.flush()  # should not raise

    def test_flush_failure(self):
        c = _sync_client()
        c._kafka_producer = MagicMock()
        c._kafka_producer.flush.return_value = 5
        with pytest.raises(AssertionError):
            c.flush()


class TestSyncClientSetGraph:
    def test_set_graph(self):
        c = _sync_client()
        graph = _graph()
        c.set_graph(graph)
        assert c._current_active_graph is graph
        assert c.graph_known_event.is_set()


class TestSyncClientPollingThread:
    def test_start_polling_thread(self):
        c = _sync_client()
        c.start_polling_thread()
        assert c.running_polling_thread is True
        assert c.polling_thread is not None
        c.running_polling_thread = False
        c.polling_thread.join(timeout=1)


class TestSyncClientFutureTracking:
    def _client(self, consuming):
        c = _sync_client()
        c.set_graph(_graph())
        c._kafka_producer = MagicMock()
        c.running_result_consumer = consuming
        return c

    def test_send_event_without_consumer_does_not_track(self):
        c = self._client(consuming=False)
        future = c.send_event(Operator("users", 2), "k", "fn")
        assert c._futures == {}
        # the delivery report still stamps the returned future
        on_delivery = c._kafka_producer.produce.call_args.kwargs["on_delivery"]
        msg = MagicMock()
        msg.key.return_value = future.request_id
        msg.timestamp.return_value = (0, 777)
        on_delivery(None, msg)
        assert future._val.in_timestamp == 777
        assert c.delivery_timestamps[future.request_id] == 777

    def test_send_event_with_consumer_tracks(self):
        c = self._client(consuming=True)
        future = c.send_event(Operator("users", 2), "k", "fn")
        assert c._futures == {future.request_id: future}

    def test_result_resolves_and_drops_future(self):
        from styx.common.serialization import msgpack_serialization

        c = self._client(consuming=True)
        future = c.send_event(Operator("users", 2), "k", "fn")

        def result(key):
            m = MagicMock()
            m.error.return_value = None
            m.key.return_value = key
            m.value.return_value = msgpack_serialization("ok")
            m.timestamp.return_value = (0, 999)
            return m

        polls = [result(b"someone-else"), result(future.request_id), result(future.request_id)]

        def poll(_timeout):
            if not polls:
                c.running_result_consumer = False
                return None
            return polls.pop(0)

        consumer = MagicMock()
        consumer.list_topics.return_value.topics = {"users--OUT": MagicMock(partitions={0: None})}
        consumer.poll.side_effect = poll
        with patch("styx.client.sync_client.Consumer", return_value=consumer):
            c.start_consuming_results()

        assert future.done()
        assert future.get().response == "ok"
        assert future.get().out_timestamp == 999
        # dropped once resolved; the duplicate response is ignored
        assert c._futures == {}


class TestAsyncClientFutureTracking:
    def _client(self, consuming):
        c = _async_client()
        c.set_graph(_graph())
        c._kafka_producer = MagicMock()
        c._kafka_producer.send_and_wait = AsyncMock(return_value=MagicMock(timestamp=555))
        if consuming:
            c._result_consumer_task = MagicMock()
        return c

    @pytest.mark.asyncio
    async def test_send_event_without_consumer_does_not_track(self):
        c = self._client(consuming=False)
        future = await c.send_event(Operator("users", 2), "k", "fn")
        assert c._futures == {}
        assert future._val.in_timestamp == 555
        assert c.delivery_timestamps[future.request_id] == 555

    @pytest.mark.asyncio
    async def test_send_event_with_consumer_tracks(self):
        c = self._client(consuming=True)
        future = await c.send_event(Operator("users", 2), "k", "fn")
        assert c._futures == {future.request_id: future}
        assert future._val.in_timestamp == 555

    @pytest.mark.asyncio
    async def test_result_resolves_and_drops_future(self):
        c = self._client(consuming=True)
        future = await c.send_event(Operator("users", 2), "k", "fn")

        def record(key):
            return MagicMock(key=key, value="ok", timestamp=999)

        batches = [{"tp": [record(b"someone-else"), record(future.request_id), record(future.request_id)]}]

        async def getmany(timeout_ms):
            if batches:
                return batches.pop(0)
            raise asyncio.CancelledError

        consumer = MagicMock()
        consumer.start = AsyncMock()
        consumer.topics = AsyncMock(return_value={"users--OUT"})
        consumer.getmany.side_effect = getmany
        with (
            patch("styx.client.async_client.AIOKafkaConsumer", return_value=consumer),
            pytest.raises(asyncio.CancelledError),
        ):
            await c.start_result_consumer_task()

        assert future.done()
        assert (await future.get()).response == "ok"
        assert c._futures == {}


class TestAsyncClientCloseRobustness:
    @pytest.mark.asyncio
    async def test_close_without_result_consumer(self):
        # open(consume=False) never creates the result consumer or its task
        c = _async_client()
        c._kafka_producer = MagicMock()
        c._kafka_producer.flush = AsyncMock()
        c._kafka_producer.stop = AsyncMock()
        c._metadata_consumer = MagicMock()
        c._metadata_consumer.stop = AsyncMock()
        c._metadata_consumer_task = asyncio.create_task(asyncio.sleep(100))

        await c.close()
        c._metadata_consumer.stop.assert_called_once()
        assert c._metadata_consumer_task.cancelled()

    @pytest.mark.asyncio
    async def test_close_cancels_loops_before_stopping_consumers(self):
        c = _async_client()
        c._kafka_producer = MagicMock()
        c._kafka_producer.flush = AsyncMock()
        c._kafka_producer.stop = AsyncMock()
        c._result_consumer_task = asyncio.create_task(asyncio.sleep(100))
        c._metadata_consumer_task = asyncio.create_task(asyncio.sleep(100))
        tasks_done_at_stop = []

        async def stop():
            tasks_done_at_stop.append((c._result_consumer_task.done(), c._metadata_consumer_task.done()))

        c._result_consumer = MagicMock(stop=AsyncMock(side_effect=stop))
        c._metadata_consumer = MagicMock(stop=AsyncMock(side_effect=stop))

        await c.close()
        assert tasks_done_at_stop == [(True, True), (True, True)]


def _undecodable_then(graph):
    from styx.common.serialization import cloudpickle_serialization

    return [b"not a pickle", cloudpickle_serialization(graph)]


class TestMetadataConsumerSkipsUndecodableRecords:
    def test_sync(self):
        c = _sync_client()
        graph = _graph()
        values = _undecodable_then(graph)

        def poll(_timeout):
            if not values:
                c.running_metadata_consumer = False
                return None
            msg = MagicMock()
            msg.error.return_value = None
            msg.value.return_value = values.pop(0)
            return msg

        consumer = MagicMock()
        consumer.list_topics.return_value.topics = {"styx-metadata": MagicMock(partitions={0: None})}
        consumer.poll.side_effect = poll
        c.running_metadata_consumer = True
        with patch("styx.client.sync_client.Consumer", return_value=consumer):
            c.start_consuming_metadata()

        assert c.graph_known_event.is_set()
        assert c._current_active_graph.name == graph.name

    @pytest.mark.asyncio
    async def test_async(self):
        c = _async_client()
        graph = _graph()
        batches = [{"tp": [MagicMock(value=v, offset=i) for i, v in enumerate(_undecodable_then(graph))]}]

        async def getmany(timeout_ms):
            if batches:
                return batches.pop(0)
            raise asyncio.CancelledError

        consumer = MagicMock()
        consumer.start = AsyncMock()
        consumer.topics = AsyncMock(return_value={"styx-metadata"})
        consumer.getmany.side_effect = getmany
        with (
            patch("styx.client.async_client.AIOKafkaConsumer", return_value=consumer),
            pytest.raises(asyncio.CancelledError),
        ):
            await c.start_consuming_metadata()

        assert c.graph_known_event.is_set()
        assert c._current_active_graph.name == graph.name
