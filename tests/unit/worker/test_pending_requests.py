"""Requests behind their ingress partition's recorded offset that have not committed.

An epoch's ingress offsets advance past all its requests, the ones rescheduled to a
later epoch included. Those are not consumed from Kafka again, so a snapshot or a
migration stop carries them, and the protocol that takes over sequences them before
anything it consumes.
"""

from types import SimpleNamespace

from styx.common.operator import Operator
from styx.common.run_func_payload import RunFuncPayload
from styx.common.serialization import msgpack_deserialization, msgpack_serialization

from worker.ingress.styx_kafka_ingress import StyxKafkaIngress
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState
from worker.sequencer import pending_requests
from worker.sequencer.sequencer import Sequencer
from worker.transactional_protocols.aria import AriaProtocol

OP = "op"


def _payload(offset: int, key: str = "k", partition: int = 0, ingress_partition: int = 0) -> RunFuncPayload:
    return RunFuncPayload(
        request_id=f"rq{offset}".encode(),
        key=key,
        operator_name=OP,
        partition=partition,
        function_name="f",
        params=(1, "a"),
        kafka_offset=offset,
        kafka_ingress_partition=ingress_partition,
    )


def _protocol(offsets: dict | None = None) -> AriaProtocol:
    """A protocol with a real sequencer and ingress over two partitions of one worker."""
    state = InMemoryOperatorState({(OP, 0), (OP, 1)})
    operators = {(OP, p): Operator(OP, n_partitions=2) for p in (0, 1)}
    proto = AriaProtocol.__new__(AriaProtocol)
    proto.id = 1
    proto.local_state = state
    proto.registered_operators = operators
    proto.sequencer = Sequencer()
    proto.sequencer.set_sequencer_id([], 1)
    proto.topic_partition_offsets = dict(offsets or {})
    proto.ingress = StyxKafkaIngress(
        networking=SimpleNamespace(),
        sequencer=proto.sequencer,
        state=state,
        registered_operators=operators,
        worker_id=1,
        kafka_url="unused",
        epoch_interval_ms=1,
        sequence_max_size=100,
    )
    proto._restored_requests = []
    return proto


def _run_epoch(proto: AriaProtocol, rescheduled_offsets: set[int]) -> None:
    """The offset bookkeeping of an epoch whose requests at `rescheduled_offsets`
    are rescheduled (fallback read/write-set change or concurrency abort)."""
    sequence = proto.sequencer.get_epoch()
    rescheduled = {item.t_id for item in sequence if item.payload.kafka_offset in rescheduled_offsets}
    proto._advance_offsets(sequence)
    proto.sequencer.increment_epoch(max_t_counter=proto.sequencer.t_counter, t_ids_to_reschedule=rescheduled)


class TestPendingAtTheCut:
    def test_rescheduled_request_is_behind_the_offset_and_pending(self):
        proto = _protocol({(OP, 0): 39})
        proto.sequencer.sequence(_payload(40))
        proto.sequencer.sequence(_payload(41))

        _run_epoch(proto, rescheduled_offsets={40})

        # The offset passes 40, which only runs next epoch: the cut must carry it.
        assert proto.topic_partition_offsets[(OP, 0)] == 41
        assert [fields[6] for fields in proto.pending_requests()] == [40]

    def test_requests_consumed_after_the_offset_are_not_pending(self):
        proto = _protocol({(OP, 0): 39})
        proto.sequencer.sequence(_payload(40))
        _run_epoch(proto, rescheduled_offsets=set())
        # Consumed after the epoch: Kafka still has it past the recorded offset.
        proto.sequencer.sequence(_payload(41))

        assert proto.topic_partition_offsets[(OP, 0)] == 40
        assert proto.pending_requests() == []

    def test_committed_epoch_leaves_nothing_pending(self):
        proto = _protocol({(OP, 0): 39})
        proto.sequencer.sequence(_payload(40))
        _run_epoch(proto, rescheduled_offsets=set())
        assert proto.pending_requests() == []


class TestRestoring:
    def test_restored_request_runs_once_before_newly_consumed_ones(self):
        # A cut at offset 41 with 40 rescheduled; the next protocol consumes from 42.
        before = _protocol({(OP, 0): 39})
        before.sequencer.sequence(_payload(40))
        before.sequencer.sequence(_payload(41))
        _run_epoch(before, rescheduled_offsets={40})
        offsets = dict(before.topic_partition_offsets)
        carried = msgpack_deserialization(msgpack_serialization(before.pending_requests()))

        after = _protocol(offsets)
        after._restored_requests = pending_requests.consumed_by(carried, after.registered_operators)
        after._sequence_restored_requests()
        after.sequencer.sequence(_payload(offsets[(OP, 0)] + 1))

        sequence = after.sequencer.get_epoch()
        assert [item.payload.kafka_offset for item in sequence] == [40, 42]
        restored = sequence[0].payload
        assert (restored.request_id, restored.params, restored.kafka_ingress_partition) == (b"rq40", (1, "a"), 0)
        assert after._restored_requests == []

    def test_restored_request_goes_to_the_partition_holding_its_key_now(self):
        proto = _protocol()
        key = next(
            f"key{i}" for i in range(1000) if proto.registered_operators[(OP, 0)].which_partition(f"key{i}") == 1
        )
        proto.local_state.batch_insert({key: "v"}, OP, 1)
        # Sequenced at partition 0 before a migration moved the key to partition 1.
        proto._restored_requests = [pending_requests.encode(_payload(40, key=key, partition=0))]

        proto._sequence_restored_requests()

        (item,) = proto.sequencer.get_epoch()
        assert (item.payload.partition, item.payload.kafka_ingress_partition) == (1, 0)


class TestConsumedBy:
    def test_keeps_this_workers_ingress_partitions_in_offset_order_once(self):
        mine, other = (OP, 0), (OP, 1)
        carried = [
            pending_requests.encode(_payload(42)),
            pending_requests.encode(_payload(40)),
            pending_requests.encode(_payload(7, ingress_partition=1)),
            # The same request from the checkpoint and from MigrationDone.
            pending_requests.encode(_payload(40)),
        ]

        kept = pending_requests.consumed_by(carried, {mine})

        assert [fields[6] for fields in kept] == [40, 42]
        assert [fields[6] for fields in pending_requests.consumed_by(carried, {other})] == [7]
