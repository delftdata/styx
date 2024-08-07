from dataclasses import dataclass


@dataclass
class RunFuncPayload(object):
    request_id: bytes
    key: object
    timestamp: int
    operator_name: str
    partition: int
    function_name: str
    params: tuple
    response_socket: object = None
    kafka_offset: int = -1
    # host, port, t_id, stake, chain_participants, partial_node_count
    ack_payload: tuple[str, int, int, str, list[int], int] | None = None


@dataclass
class SequencedItem(object):
    t_id: int
    payload: RunFuncPayload

    def __hash__(self):
        return hash(self.t_id)

    def __lt__(self, other):
        return self.t_id < other.t_id
