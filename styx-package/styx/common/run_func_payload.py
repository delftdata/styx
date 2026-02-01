from dataclasses import dataclass


@dataclass
class RunFuncPayload:
    request_id: bytes
    key: object
    operator_name: str
    partition: int
    function_name: str
    params: tuple
    kafka_offset: int = -1
    kafka_ingress_partition: int = -1
    # host, port, t_id, stake, chain_participants, partial_node_count
    ack_payload: tuple[str, int, int, str, list[int], int] | None = None


@dataclass
class SequencedItem:
    t_id: int
    payload: RunFuncPayload

    def __hash__(self) -> int:
        return hash(self.t_id)

    def __lt__(self, other: SequencedItem) -> bool:
        return self.t_id < other.t_id
