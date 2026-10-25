"""Requests already behind their ingress partition's recorded offset that have not
committed yet.

The ingress offset of an epoch advances past every request in it, including the
ones rescheduled to a later epoch (a fallback read/write-set change, or a
concurrency abort without fallback). Those are not consumed from Kafka again, so
a snapshot or a migration stop cut at that offset carries them, and the protocol
that takes over sequences them before it consumes anything.

They travel as plain lists (msgpack) so the snapshot process and the coordinator
can pass them on without knowing the payload type.
"""

from typing import TYPE_CHECKING

from styx.common.run_func_payload import RunFuncPayload

if TYPE_CHECKING:
    from collections.abc import Iterable

    from styx.common.run_func_payload import SequencedItem
    from styx.common.types import OperatorPartition

# request_id, key, operator_name, partition, function_name, params, kafka_offset,
# kafka_ingress_partition
PendingRequest = list


def encode(payload: RunFuncPayload) -> PendingRequest:
    return [
        payload.request_id,
        payload.key,
        payload.operator_name,
        payload.partition,
        payload.function_name,
        payload.params,
        payload.kafka_offset,
        payload.kafka_ingress_partition,
    ]


def decode(fields: PendingRequest) -> RunFuncPayload:
    request_id, key, operator_name, partition, function_name, params, kafka_offset, ingress_partition = fields
    return RunFuncPayload(
        request_id=request_id,
        key=key,
        operator_name=operator_name,
        partition=partition,
        function_name=function_name,
        params=tuple(params),
        kafka_offset=kafka_offset,
        kafka_ingress_partition=ingress_partition,
    )


def behind_offsets(
    items: Iterable[SequencedItem],
    ingress_offsets: dict[OperatorPartition, int],
) -> list[PendingRequest]:
    """The sequenced requests at or below their ingress partition's recorded offset."""
    behind = []
    for item in items:
        payload = item.payload
        recorded = ingress_offsets.get((payload.operator_name, payload.kafka_ingress_partition), -1)
        if 0 <= payload.kafka_offset <= recorded:
            behind.append(encode(payload))
    return behind


def consumed_by(
    pending: Iterable[PendingRequest],
    ingress_partitions: Iterable[OperatorPartition],
) -> list[PendingRequest]:
    """The pending requests of the given ingress partitions, in offset order and
    without duplicates."""
    ours = set(ingress_partitions)
    by_request: dict[bytes, PendingRequest] = {}
    for fields in pending:
        if (fields[2], fields[7]) in ours:
            by_request.setdefault(fields[0], fields)
    return sorted(by_request.values(), key=lambda fields: (fields[2], fields[7], fields[6]))
