"""Snapshot consistency of async state migration.

A key moved by the async migration sender is removed from its source (with a
tombstone in the source's delta) and inserted at its destination (in the
destination's delta). A snapshot must never record the first without the second:

- the delta flush must not wipe migration writes that land while it is awaiting;
- a batch must not be in flight across an epoch's snapshot cut.
"""

import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

from styx.common.message_types import MessageType
from styx.common.tcp_networking import NetworkingManager

from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.transactional_protocols import aria
from worker.transactional_protocols.aria import AriaProtocol

OP = "op"
SRC = (OP, 0)
DST = (OP, 1)
W1 = ("w1", 5001, 6001)
W2 = ("w2", 5002, 6002)
DNS = {OP: {0: W1, 1: W2}}
LINK_DELAY_S = 0.01


# ---------------------------------------------------------------------------
# Delta flush
# ---------------------------------------------------------------------------


class TestDeltaFlushKeepsConcurrentMigrationWrites:
    async def test_batch_landing_during_flush_goes_to_next_delta(self):
        state = InMemoryOperatorState({SRC, DST})
        state.batch_insert({"k0": "v0"}, OP, 0)
        sent: list[dict] = []

        async def send_message(*_args, msg, **_kwargs):
            # The real send serializes before its first await.
            sent.append(copy.deepcopy(msg[0]))
            await asyncio.sleep(0)
            # An async migration batch lands while the flush is awaiting.
            state.set_batch_data_from_migration(DST, {"k1": "v1"})

        proto = SimpleNamespace(
            local_state=state,
            snapshotting_networking_manager=SimpleNamespace(send_message=AsyncMock(side_effect=send_message)),
            networking=SimpleNamespace(host_name="w1"),
            snapshotting_port=7000,
        )
        await AriaProtocol.send_delta_to_snapshotting_proc(proto)

        assert sent == [{SRC: {"k0": "v0"}, DST: {}}]
        # Not wiped: it ships with the next flush.
        assert state.delta_map[DST] == {"k1": "v1"}
        assert state.delta_map[SRC] == {}

    def test_take_delta_map_returns_deltas_and_starts_fresh(self):
        state = InMemoryOperatorState({SRC, DST})
        state.batch_insert({"k0": "v0"}, OP, 0)
        delta = state.take_delta_map()
        assert delta == {SRC: {"k0": "v0"}, DST: {}}
        assert state.delta_map == {SRC: {}, DST: {}}
        state.batch_insert({"k1": "v1"}, OP, 1)
        assert delta[DST] == {}


# ---------------------------------------------------------------------------
# Batches across the snapshot cut
# ---------------------------------------------------------------------------


class _Link:
    """Delivers protocol messages between fake workers after a delay, like a real link."""

    def __init__(self) -> None:
        self.workers: dict[tuple[str, int], AriaProtocol] = {}
        self.to_coordinator: list[MessageType] = []
        self.in_flight: set[asyncio.Task] = set()

    def view(self) -> SimpleNamespace:
        return SimpleNamespace(
            send_message=self.send_message,
            decode_message=NetworkingManager.decode_message,
            wait_remote_key_event={},
        )

    async def send_message(self, host, port, msg, msg_type, serializer) -> None:
        # Drop the 8-byte length prefix, as the receiving TCP reader does.
        data = NetworkingManager.encode_message(msg=msg, msg_type=msg_type, serializer=serializer)[8:]
        target = self.workers.get((host, port))
        if target is None:
            self.to_coordinator.append(msg_type)
            return

        async def deliver() -> None:
            await asyncio.sleep(LINK_DELAY_S)
            await target.protocol_handlers_map[msg_type](data)

        task = asyncio.get_running_loop().create_task(deliver())
        self.in_flight.add(task)
        task.add_done_callback(self.in_flight.discard)


def _worker(link: _Link, worker_id: int, addr: tuple, peers: dict, state: InMemoryOperatorState) -> AriaProtocol:
    proto = AriaProtocol.__new__(AriaProtocol)
    proto.id = worker_id
    proto.peers = peers
    proto.dns = DNS
    proto.local_state = state
    proto.networking = link.view()
    proto.sequencer = SimpleNamespace(epoch_counter=0)
    proto.started = asyncio.Event()
    proto.started.set()
    proto.migrating_state = True
    proto._init_async_migration_state()
    proto.protocol_handlers_map = {
        MessageType.AsyncMigration: proto._handle_async_migration,
        MessageType.AsyncMigrationAck: proto._handle_async_migration_ack,
    }
    link.workers[(addr[0], addr[2])] = proto
    return proto


def _two_workers(n_keys: int):
    link = _Link()
    src_state = InMemoryOperatorState({SRC})
    src_state.data[SRC] = {k: f"v{k}" for k in range(n_keys)}
    src_state.add_keys_to_send({SRC: {(k, 1) for k in range(n_keys)}})
    dst_state = InMemoryOperatorState({DST})
    src = _worker(link, 1, W1, {2: W2}, src_state)
    dst = _worker(link, 2, W2, {1: W1}, dst_state)
    return link, src, dst


def _restore(base: dict, *deltas: dict) -> dict:
    """What recovery loads: the base snapshot with each worker's deltas applied."""
    data = {op_part: dict(kv) for op_part, kv in base.items()}
    for delta in deltas:
        for op_part, kv in delta.items():
            part = data.setdefault(op_part, {})
            for k, v in kv.items():
                if v is None:
                    part.pop(k, None)
                else:
                    part[k] = v
    return data


class TestMigrationBatchesDoNotStraddleSnapshotCut:
    async def test_cut_after_drain_has_every_key_on_one_side(self):
        n_keys = 10
        _link, src, dst = _two_workers(n_keys)
        base = {SRC: dict(src.local_state.data[SRC]), DST: {}}

        sender = asyncio.create_task(src._continuous_migration_sender())
        await asyncio.sleep(0)  # the sender removes a batch and puts it on the wire
        assert src.local_state.data[SRC] == {}

        # Epoch reaches its barriers: drain, then every worker flushes its delta.
        await src._drain_migration_batches()
        restored = _restore(base, src.local_state.take_delta_map(), dst.local_state.take_delta_map())

        assert restored[SRC] == {}
        assert restored[DST] == {k: f"v{k}" for k in range(n_keys)}
        src._migration_gate.set()
        await asyncio.wait_for(sender, timeout=2)

    async def test_closed_gate_keeps_sender_from_taking_batches(self, monkeypatch):
        monkeypatch.setattr(aria, "ASYNC_MIGRATION_BATCH_SIZE", 2)
        _link, src, _dst = _two_workers(6)

        sender = asyncio.create_task(src._continuous_migration_sender())
        await asyncio.sleep(0)
        await src._drain_migration_batches()
        remaining = src.local_state.keys_remaining_to_send()
        assert remaining == 4

        await asyncio.sleep(LINK_DELAY_S * 3)
        assert src.local_state.keys_remaining_to_send() == remaining  # paused across the cut

        src._migration_gate.set()  # next epoch
        await asyncio.wait_for(sender, timeout=2)
        assert src.local_state.keys_remaining_to_send() == 0

    async def test_migration_done_is_reported_after_last_ack(self):
        link, src, dst = _two_workers(3)

        sender = asyncio.create_task(src._continuous_migration_sender())
        await asyncio.sleep(0)
        assert src.local_state.keys_remaining_to_send() == 0
        assert link.to_coordinator == []  # the batch has not landed yet

        await asyncio.wait_for(sender, timeout=2)
        assert link.to_coordinator == [MessageType.MigrationDone]
        assert dst.local_state.data[DST] == {0: "v0", 1: "v1", 2: "v2"}

    async def test_sender_waits_until_protocol_started(self):
        _link, src, _dst = _two_workers(3)
        src.started.clear()

        sender = asyncio.create_task(src._continuous_migration_sender())
        await asyncio.sleep(LINK_DELAY_S * 2)
        assert src.local_state.keys_remaining_to_send() == 3

        src.started.set()
        await asyncio.wait_for(sender, timeout=2)
        assert src.local_state.keys_remaining_to_send() == 0

    async def test_stale_ack_from_previous_protocol_is_ignored(self):
        _link, src, _dst = _two_workers(1)
        src._migration_unacked.add(0)
        src._migration_all_acked.clear()

        src._ack_migration_batch(src._migration_nonce + 1, 0)
        assert not src._migration_all_acked.is_set()

        src._ack_migration_batch(src._migration_nonce, 0)
        assert src._migration_all_acked.is_set()

    async def test_drain_returns_immediately_without_batches(self):
        _link, src, _dst = _two_workers(0)
        await asyncio.wait_for(src._drain_migration_batches(), timeout=0.1)
        assert not src._migration_gate.is_set()


class TestWaitingTransactionWokenByBatch:
    async def test_waiter_for_key_in_flight_wakes_with_key_present(self):
        _link, src, dst = _two_workers(3)
        # A transaction at the destination waits for key 1, already on its way.
        waiter = asyncio.Event()
        dst.networking.wait_remote_key_event[DST] = {1: waiter}

        sender = asyncio.create_task(src._continuous_migration_sender())
        await asyncio.wait_for(waiter.wait(), timeout=2)
        assert dst.local_state.data[DST][1] == "v1"
        await asyncio.wait_for(sender, timeout=2)

    async def test_waiter_for_none_valued_key_wakes(self):
        # A key can hold None; it must still be delivered and wake its waiter.
        _link, src, dst = _two_workers(3)
        src.local_state.data[SRC][1] = None
        waiter = asyncio.Event()
        dst.networking.wait_remote_key_event[DST] = {1: waiter}

        sender = asyncio.create_task(src._continuous_migration_sender())
        await asyncio.wait_for(waiter.wait(), timeout=2)
        assert dst.local_state.data[DST][1] is None
        await asyncio.wait_for(sender, timeout=2)
