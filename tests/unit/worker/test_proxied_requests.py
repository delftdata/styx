"""Requests consumed by one worker for a key that migrated to a peer's partition.

They stay in the consuming worker's epoch and run on the peer as a proxied call,
so an ingress partition's offset is only ever advanced by the worker consuming
it. The peer reports the whole outcome (response, abort, participants, fallback
read/write-set change) in one ProxyResult, which the root awaits before it can
commit.
"""

import asyncio
from types import SimpleNamespace

import pytest
from styx.common.base_networking import BaseNetworking
from styx.common.message_types import MessageType
from styx.common.operator import Operator
from styx.common.run_func_payload import RunFuncPayload
from styx.common.serialization import Serializer
from styx.common.tcp_networking import NetworkingManager
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from worker.ingress.styx_kafka_ingress import StyxKafkaIngress
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.sequencer.sequencer import Sequencer
from worker.transactional_protocols import aria as aria_module
from worker.transactional_protocols.aria import AriaProtocol

OP = "op"
W1 = ("w1", 5001, 6001)  # consumes ingress partition 0
W2 = ("w2", 5002, 6002)  # owns partition 1, where the key now lives
DNS = {OP: {0: W1, 1: W2}}
PEERS = {1: W1, 2: W2}


async def read(ctx):
    return ctx.key, ctx.get()


async def fail(_ctx):
    msg = "no such account"
    raise ValueError(msg)


class _Link:
    """Delivers protocol messages between fake workers, like the TCP readers."""

    def __init__(self) -> None:
        self.workers: dict[tuple[str, int], AriaProtocol] = {}
        self.sent: list[MessageType] = []

    async def deliver(self, host, port, msg, msg_type, serializer) -> bool:
        self.sent.append(msg_type)
        data = NetworkingManager.encode_message(msg=msg, msg_type=msg_type, serializer=serializer)[8:]
        await asyncio.sleep(0)
        await self.workers[(host, port)].protocol_handlers_map[msg_type](data)
        return True


class _Networking(NetworkingManager):
    def __init__(self, link: _Link, addr: tuple, worker_id: int) -> None:
        super().__init__(addr[2])
        self.host_name = addr[0]
        self.set_worker_id(worker_id)
        self._link = link

    async def send_message(self, host, port, msg, msg_type, serializer=Serializer.MSGPACK) -> bool:
        return await self._link.deliver(host, port, msg, msg_type, serializer)


def _worker(link: _Link, worker_id: int, addr: tuple, partitions: set[int]) -> AriaProtocol:
    state = InMemoryOperatorState({(OP, p) for p in partitions})
    networking = _Networking(link, addr, worker_id)
    registered = {}
    for p in partitions:
        operator = Operator(OP, n_partitions=2)
        operator.register(read)
        operator.register(fail)
        operator.attach_state_networking(state, networking, DNS, None)
        registered[(OP, p)] = operator

    proto = AriaProtocol.__new__(AriaProtocol)
    proto.id = worker_id
    proto.incarnation = 0
    proto.peers = {w: a for w, a in PEERS.items() if w != worker_id}
    proto.dns = DNS
    proto.local_state = state
    proto.networking = networking
    proto.registered_operators = registered
    proto.background_functions = AIOTaskScheduler()
    proto.waiting_on_transactions = {}
    proto.fallback_locking_event_map = {}
    proto._proxy_results = {}
    proto._proxy_rw_changed = set()
    proto._proxied_calls = 0
    proto.protocol_handlers_map = {
        MessageType.RunFunProxy: proto._handle_run_fun_proxy,
        MessageType.ProxyResult: proto._handle_proxy_result,
    }
    link.workers[(addr[0], addr[2])] = proto
    return proto


def _cluster():
    link = _Link()
    root = _worker(link, 1, W1, {0})
    owner = _worker(link, 2, W2, {1})
    owner.local_state.batch_insert({"k": "v"}, OP, 1)
    return link, root, owner


def _misrouted(function_name: str = "read") -> RunFuncPayload:
    # As the ingress sequences it: partition is where the key lives now, the
    # ingress partition and offset are where it was consumed.
    return RunFuncPayload(
        request_id=b"rq",
        key="k",
        operator_name=OP,
        partition=1,
        function_name=function_name,
        params=(),
        kafka_offset=41,
        kafka_ingress_partition=0,
    )


class TestProxiedRequest:
    async def test_runs_on_owner_and_answers_at_the_root(self):
        link, root, owner = _cluster()

        assert await root.run_function(7, _misrouted()) is True

        assert root.networking.client_responses[7] == ["k", "v"]
        assert link.sent == [MessageType.RunFunProxy, MessageType.ProxyResult]
        # The owner holds the transaction's reservation, for conflict detection,
        # and is a chain participant, so a fallback run reaches it with Unlock.
        assert "k" in owner.local_state.read_sets[(OP, 1)][7]
        assert root.networking.chain_participants[7] == [2]
        assert 7 not in root._proxy_results
        assert root._proxied_calls == 1
        assert owner._proxied_calls == 0

    async def test_logic_abort_reaches_the_root(self):
        _link, root, _owner = _cluster()

        assert await root.run_function(7, _misrouted("fail")) is False

        assert root.networking.aborted_events[7] == "no such account"
        assert 7 in root.networking.logic_aborts_everywhere
        assert 7 not in root.networking.client_responses

    async def test_fallback_read_write_set_change_reaches_the_root(self):
        _link, root, owner = _cluster()
        owner.local_state.has_fallback_rw_set_changed = lambda t_id: t_id == 7

        await root.run_function(7, _misrouted(), fallback_mode=True)

        assert 7 in root._proxy_rw_changed

    async def test_response_goes_to_the_ingress_partition(self):
        _link, root, _owner = _cluster()
        payload = _misrouted()
        assert root._response_partition(payload) == 0
        local = RunFuncPayload(request_id=b"r", key="x", operator_name=OP, partition=0, function_name="read", params=())
        assert root._response_partition(local) == 0

    async def test_participant_call_for_a_missing_partition_is_not_proxied(self):
        # Only a root (no ack payload) is proxied; a chain call must target a local partition.
        link, root, _owner = _cluster()
        payload = _misrouted()
        payload.ack_payload = ("w9", 6009, 7, "1.0", [])
        with pytest.raises(AttributeError):
            await root.run_function(7, payload)
        assert link.sent == []


class TestStaleProxyMessages:
    async def test_result_from_another_incarnation_is_dropped(self):
        _link, root, _owner = _cluster()
        pending = asyncio.get_running_loop().create_future()
        root._proxy_results[7] = pending
        data = NetworkingManager.encode_message(
            msg=(7, "stale", None, [2], False, 99),
            msg_type=MessageType.ProxyResult,
            serializer=Serializer.MSGPACK,
        )[8:]

        await root._handle_proxy_result(data)

        assert not pending.done()

    async def test_call_from_another_incarnation_is_not_run(self):
        link, _root, owner = _cluster()
        data = NetworkingManager.encode_message(
            msg=(7, 1, b"rq", OP, "read", "k", 1, False, (), 99),
            msg_type=MessageType.RunFunProxy,
            serializer=Serializer.MSGPACK,
        )[8:]

        await owner._handle_run_fun_proxy(data)
        await asyncio.sleep(0)

        assert link.sent == []
        assert 7 not in owner.local_state.read_sets[(OP, 1)]


class TestIngressOffsetsStayWithTheConsumer:
    def test_proxied_request_advances_the_consumers_ingress_offset(self):
        proto = SimpleNamespace(concurrency_aborts_everywhere=set(), topic_partition_offsets={(OP, 0): 40})
        item = SimpleNamespace(t_id=7, payload=_misrouted())
        AriaProtocol._advance_offsets(proto, [item])
        assert proto.topic_partition_offsets == {(OP, 0): 41}


def _key_on(operator: Operator, partition: int) -> str:
    return next(key for key in (f"key{i}" for i in range(1000)) if operator.which_partition(key) == partition)


def _record(offset: int, key: str) -> SimpleNamespace:
    # As the client sent it before the migration: addressed to ingress partition 0.
    value = BaseNetworking.encode_message(
        msg=(OP, key, "read", (), 0),
        msg_type=MessageType.ClientMsg,
        serializer=Serializer.MSGPACK,
    )
    return SimpleNamespace(key=f"rq{offset}".encode(), offset=offset, partition=0, value=value)


class TestOffsetNeverPassesAnUnrunRequest:
    async def test_snapshot_offset_covers_only_requests_that_ran(self):
        # Regression: the consuming worker used to forward a request for a migrated
        # key into the owner's epoch and advance the ingress offset past it for its
        # own later requests. A snapshot taken before the owner ran it recorded an
        # offset past it, and recovery skipped the request.
        _link, root, owner = _cluster()
        operator = root.registered_operators[(OP, 0)]
        migrated, local = _key_on(operator, 1), _key_on(operator, 0)
        owner.local_state.batch_insert({migrated: "m"}, OP, 1)
        root.local_state.batch_insert({local: "l"}, OP, 0)

        sequencer = Sequencer()
        sequencer.set_sequencer_id([2], 1)
        ingress = StyxKafkaIngress(
            networking=root.networking,
            sequencer=sequencer,
            state=root.local_state,
            registered_operators=root.registered_operators,
            worker_id=1,
            kafka_url="unused",
            epoch_interval_ms=1,
            sequence_max_size=100,
        )
        records = [_record(40, migrated), _record(41, local)]
        for record in records:
            ingress.handle_message_from_kafka(record)

        # The consuming worker's epoch, up to its snapshot.
        root.concurrency_aborts_everywhere = set()
        root.topic_partition_offsets = {(OP, 0): 39}
        sequence = sequencer.get_epoch()
        for item in sequence:
            await root.run_function(item.t_id, item.payload)
        root._advance_offsets(sequence)

        recorded = root.topic_partition_offsets[(OP, 0)]
        answered = {item.payload.kafka_offset for item in sequence if item.t_id in root.networking.client_responses}
        assert recorded == 41
        assert {r.offset for r in records if r.offset <= recorded} <= answered


class TestStallDiagnostics:
    async def test_watchdog_reports_what_a_stalled_epoch_waits_on(self, monkeypatch):
        _link, root, _owner = _cluster()
        warnings: list[str] = []
        monkeypatch.setattr(aria_module, "EPOCH_STALL_WARN_S", 0.01)
        monkeypatch.setattr(aria_module.logging, "warning", warnings.append)
        root.sequencer = SimpleNamespace(epoch_counter=5)
        root.networking.prepare_function_chain(7)
        root._proxy_results[9] = asyncio.get_running_loop().create_future()

        root._begin_wait("chains")
        watchdog = asyncio.create_task(root._stall_watchdog())
        await asyncio.sleep(0.05)
        root._end_wait()
        reported = len(warnings)
        await asyncio.sleep(0.05)
        watchdog.cancel()

        assert reported > 0
        assert "STALL @epoch 5: waiting on chains" in warnings[0]
        assert "unfinished chains: 1 [7]" in warnings[0]
        assert "proxied calls pending: 1 [9]" in warnings[0]
        # Nothing once the wait is over.
        assert len(warnings) == reported

    async def test_result_with_no_pending_call_is_reported(self, monkeypatch):
        _link, root, _owner = _cluster()
        warnings: list[str] = []
        monkeypatch.setattr(aria_module.logging, "warning", warnings.append)
        data = NetworkingManager.encode_message(
            msg=(7, "v", None, [2], False, 0),
            msg_type=MessageType.ProxyResult,
            serializer=Serializer.MSGPACK,
        )[8:]

        await root._handle_proxy_result(data)

        assert warnings == ["Dropping ProxyResult for T_ID 7 with no pending proxied call"]
