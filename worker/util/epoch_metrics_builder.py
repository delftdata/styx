from dataclasses import dataclass

from styx.common.metrics import WorkerEpochStats


@dataclass
class EpochMetricsInput:
    worker_id: int
    sequence: list
    committed_lock_free: int
    committed_fallback: int
    logic_aborts_count: int
    concurrency_aborts_everywhere: set[int]
    timings: dict[str, float]  # func_ms, chain_ms, wal_ms
    phase_resources: dict

    fallback_time_ms: float
    conflict_resolution_time_ms: float
    commit_time_ms: float
    snap_time_ms: float
    sync_time_ms: float
    epoch_latency_ms: float

    input_rate: int
    queue_backlog: int
    operator_metrics: dict


class EpochMetricsBuilder:
    """Builds WorkerEpochStats from epoch processing data."""

    def __init__(self, inputs: EpochMetricsInput) -> None:
        self.inputs = inputs
        self._precalculate()

    def _precalculate(self) -> None:
        """Precalculate commonly used values."""
        self.total_txns = len(self.inputs.sequence)
        self.committed_txns = self.inputs.committed_lock_free + self.inputs.committed_fallback
        self.concurrency_aborts_count = len(
            {seq_i.t_id for seq_i in self.inputs.sequence if seq_i.t_id in self.inputs.concurrency_aborts_everywhere}
        )

    def _build_operator_stats(self) -> list[tuple[str, int, float, float, int]]:
        """Aggregate and build per-operator epoch statistics."""
        operator_agg: dict[tuple[str, int], dict[str, float | int]] = {}

        for (op_name, partition, _func_name), m in self.inputs.operator_metrics.items():
            key = (op_name, partition)
            agg = operator_agg.setdefault(key, {"calls": 0, "sum_ms": 0.0})
            agg["calls"] += m["count"]
            agg["sum_ms"] += m["sum_ms"]

        operator_epoch_stats: list[tuple[str, int, float, float, int]] = []
        epoch_seconds = max(self.inputs.epoch_latency_ms / 1000.0, 1e-6)

        for (op_name, partition), agg in operator_agg.items():
            call_count = int(agg["calls"])
            if call_count == 0:
                continue
            total_latency_ms = float(agg["sum_ms"])
            avg_latency_ms = total_latency_ms / call_count
            tps = call_count / epoch_seconds
            operator_epoch_stats.append((op_name, partition, tps, avg_latency_ms, call_count))

        return operator_epoch_stats

    def _calculate_utilization(self) -> tuple[float, float]:
        """Calculate CPU and IO wait utilization."""
        func_time = self.inputs.timings["func_ms"]
        conflict_resolution_time = self.inputs.conflict_resolution_time_ms
        commit_time = self.inputs.commit_time_ms
        fallback_time = self.inputs.fallback_time_ms

        cpu_work_ms = func_time + conflict_resolution_time + commit_time + fallback_time

        chain_time = self.inputs.timings["chain_ms"]
        wal_time = self.inputs.timings["wal_ms"]
        snap_time = self.inputs.snap_time_ms
        io_wait_time_ms = chain_time + wal_time + snap_time + self.inputs.sync_time_ms

        cpu_utilization = (cpu_work_ms / self.inputs.epoch_latency_ms) if self.inputs.epoch_latency_ms > 0 else 0.0
        io_wait_utilization = (
            (io_wait_time_ms / self.inputs.epoch_latency_ms) if self.inputs.epoch_latency_ms > 0 else 0.0
        )

        return cpu_utilization, io_wait_utilization

    def build(self) -> WorkerEpochStats:
        operator_epoch_stats = self._build_operator_stats()
        cpu_util, io_util = self._calculate_utilization()
        local_abort_rate = (self.concurrency_aborts_count / self.total_txns) if self.total_txns else 0.0

        return WorkerEpochStats(
            worker_id=self.inputs.worker_id,
            epoch_throughput=(self.committed_txns * 1000) // self.inputs.epoch_latency_ms,
            epoch_latency=self.inputs.epoch_latency_ms,
            local_abort_rate=local_abort_rate,
            wal_time=round(self.inputs.timings["wal_ms"], 4),
            func_time=round(self.inputs.timings["func_ms"], 4),
            chain_ack_time=round(self.inputs.timings["chain_ms"], 4),
            sync_time=self.inputs.sync_time_ms,
            conflict_res_time=self.inputs.conflict_resolution_time_ms,
            commit_time=self.inputs.commit_time_ms,
            fallback_time=self.inputs.fallback_time_ms,
            snap_time=self.inputs.snap_time_ms,
            input_rate=self.inputs.input_rate,
            queue_backlog=self.inputs.queue_backlog,
            total_txns=self.total_txns,
            committed_txns=self.committed_txns,
            logic_aborts=self.inputs.logic_aborts_count,
            concurrency_aborts=self.concurrency_aborts_count,
            committed_lock_free=self.inputs.committed_lock_free,
            committed_fallback=self.inputs.committed_fallback,
            cpu_utilization=cpu_util,
            io_wait_utilization=io_util,
            operator_epoch_stats=operator_epoch_stats,
            phase_resources=self.inputs.phase_resources,
        )
