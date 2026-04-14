from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerEpochStats:
    worker_id: int
    epoch_throughput: float
    epoch_latency: float
    local_abort_rate: float
    wal_time: float
    func_time: float
    chain_ack_time: float
    sync_time: float
    conflict_res_time: float
    commit_time: float
    fallback_time: float
    snap_time: float
    input_rate: float
    queue_backlog: float
    total_txns: int
    committed_txns: int
    logic_aborts: int
    concurrency_aborts: int
    committed_lock_free: int
    committed_fallback: int
    cpu_utilization: float
    io_wait_utilization: float
    operator_epoch_stats: list[tuple[str, int, float, float, int]]
    phase_resources: dict[str, dict[str, int]]
