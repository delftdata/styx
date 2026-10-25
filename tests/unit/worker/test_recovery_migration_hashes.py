"""Rebuilding remote_keys after a post-checkpoint recovery.

Recovery re-derives each source's keys_to_send from its snapshot, but the
destinations also need remote_keys (which peer still holds each key of their new
partitions). Otherwise a transaction on a key that has not arrived reads it as
missing instead of fetching it, and the arriving batch later overwrites its write.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from styx.common.message_types import MessageType
from styx.common.serialization import Serializer
from styx.common.tcp_networking import NetworkingManager

from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.worker_service import Worker

OP = "op"
W1 = ("w1", 5001, 6001)
W2 = ("w2", 5002, 6002)
SNAP_ID = 7


class _Link:
    """Delivers control messages between fake workers after a short delay."""

    def __init__(self) -> None:
        self.workers: dict[tuple[str, int], Worker] = {}

    def view(self) -> SimpleNamespace:
        return SimpleNamespace(send_message=self.send_message, decode_message=NetworkingManager.decode_message)

    async def send_message(self, host, port, msg, msg_type, serializer) -> None:
        # Drop the 8-byte length prefix, as the receiving TCP reader does.
        data = NetworkingManager.encode_message(msg=msg, msg_type=msg_type, serializer=serializer)[8:]
        target = self.workers[(host, port)]

        async def deliver() -> None:
            await asyncio.sleep(0.01)
            await target._message_handlers[msg_type](data, msg_type)

        asyncio.get_running_loop().create_task(deliver())


def _worker(link: _Link, worker_id: int, addr: tuple, peers: dict, partitions: set, hashes_out: dict) -> Worker:
    w = Worker.__new__(Worker)
    w.id = worker_id
    w.peers = peers
    w.dns = {OP: {0: W1, 1: W2, 2: W1}}
    w.local_state = InMemoryOperatorState(partitions)
    w.networking = link.view()
    w._migration_generation = 1
    w._recovery_hashes_received = asyncio.Event()
    w._reset_recovery_hash_exchange(tag=SNAP_ID, hashes_out=hashes_out)
    w.function_execution_protocol = SimpleNamespace(
        egress=SimpleNamespace(run_dedup_scan_and_mark_started=AsyncMock()),
        started=asyncio.Event(),
    )
    w._message_handlers = {MessageType.RecoveryMigrationHashes: w._handle_recovery_migration_hashes}
    link.workers[(addr[0], addr[1])] = w
    return w


def _cluster():
    """W1 owns (op, 0) and (op, 2), W2 owns (op, 1). After recovery W1 still holds
    key 10 for W2's (op, 1) and key 20 for its own (op, 2)."""
    link = _Link()
    w1 = _worker(
        link,
        1,
        W1,
        {2: W2},
        {(OP, 0), (OP, 2)},
        {(OP, 1): {10: (1, 0)}, (OP, 2): {20: (1, 0)}},
    )
    w2 = _worker(link, 2, W2, {1: W1}, {(OP, 1)}, {})
    return w1, w2


async def _ready(w: Worker) -> None:
    await w._handle_ready_after_recovery(b"", MessageType.ReadyAfterRecovery)


class TestRecoveryMigrationHashExchange:
    async def test_destinations_learn_which_peer_holds_their_keys(self):
        w1, w2 = _cluster()
        await asyncio.wait_for(asyncio.gather(_ready(w1), _ready(w2)), timeout=2)

        assert w2.local_state.in_remote_keys(10, OP, 1)
        assert tuple(w2.local_state.get_worker_id_old_partition(OP, 1, 10)) == (1, 0)
        # Destination partition on the same worker is registered locally.
        assert w1.local_state.in_remote_keys(20, OP, 2)
        assert w1.function_execution_protocol.started.is_set()
        assert w2.function_execution_protocol.started.is_set()

    async def test_protocol_does_not_start_before_every_peer_reported(self):
        w1, w2 = _cluster()
        w2_ready = asyncio.create_task(_ready(w2))
        await asyncio.sleep(0.05)
        assert not w2.function_execution_protocol.started.is_set()
        w2.function_execution_protocol.egress.run_dedup_scan_and_mark_started.assert_not_awaited()

        await asyncio.wait_for(asyncio.gather(_ready(w1), w2_ready), timeout=2)
        assert w2.function_execution_protocol.started.is_set()

    async def test_hashes_for_another_recovery_are_dropped(self):
        _w1, w2 = _cluster()
        data = NetworkingManager.encode_message(
            msg=(SNAP_ID - 1, 1, [((OP, 1), {10: (1, 0)})]),
            msg_type=MessageType.RecoveryMigrationHashes,
            serializer=Serializer.MSGPACK,
        )[8:]
        await w2._handle_recovery_migration_hashes(data, MessageType.RecoveryMigrationHashes)
        assert not w2.local_state.in_remote_keys(10, OP, 1)
        assert w2._recovery_hash_senders == set()

    async def test_newer_recovery_releases_waiter_without_starting_old_protocol(self):
        _w1, w2 = _cluster()
        old_protocol = w2.function_execution_protocol
        w2_ready = asyncio.create_task(_ready(w2))
        await asyncio.sleep(0.02)

        # A new InitRecovery arrives before the peer reported.
        w2._migration_generation += 1
        w2._reset_recovery_hash_exchange(tag=-1, hashes_out={})
        await asyncio.wait_for(w2_ready, timeout=1)
        assert not old_protocol.started.is_set()

    async def test_no_exchange_for_ordinary_recovery(self):
        _w1, w2 = _cluster()
        w2._reset_recovery_hash_exchange(tag=-1, hashes_out={})
        await asyncio.wait_for(_ready(w2), timeout=1)
        assert w2.function_execution_protocol.started.is_set()
