"""Prometheus metrics for the Styx cluster, exposed by the coordinator.

Workers push their numbers to the coordinator (heartbeats, per-epoch
SyncCleanup, snapshot completion); this module keeps the latest values in
plain dicts/lists and only builds Prometheus metric families when a scrape
arrives. Recording is therefore a handful of float adds on the event loop,
instead of a ``labels()`` lookup plus lock per value as with
``prometheus_client.Gauge``.

Epoch statistics are exported as monotonic counters (and a summary for epoch
duration), so dashboards derive throughput, average latency, abort ratio and
the latency breakdown with ``rate()``/``increase()``. That is exact over any
window regardless of the scrape interval, whereas a "last epoch" gauge only
ever shows whichever epoch happened to finish right before the scrape.

The scrape runs on prometheus_client's HTTP thread. Writers only replace dict
entries or bump list elements, and readers snapshot dicts with ``list()``
(a single C-level call under the GIL), so no locking is needed.
"""

from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    Metric,
    SummaryMetricFamily,
)
from prometheus_client.registry import Collector

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

# Order matches the timing fields of the worker's SyncCleanup message.
EPOCH_PHASES: tuple[str, ...] = (
    "WAL",
    "1st Run",
    "Chain Acks",
    "SYNC",
    "Conflict Resolution",
    "Commit time",
    "Fallback",
    "Async Snapshot",
)

# Indices into the per-worker epoch accumulator list.
_SEQUENCED = 0
_COMMITTED = 1
_ABORTED = 2
_EPOCHS = 3
_DURATION_MS = 4
_PHASES_START = 5
_N_EPOCH_FIELDS = _PHASES_START + len(EPOCH_PHASES)


class StyxMetrics(Collector):
    def __init__(self) -> None:
        # worker_id -> (cpu %, memory bytes, net rx bytes, net tx bytes)
        self._resources: dict[int, tuple[float, int, int, int]] = {}
        # worker_id -> [sequenced, committed, aborted, epochs, duration_ms, *phase_ms]
        self._epochs: dict[int, list[float]] = {}
        # worker_id -> duration (ms) of the most recent completed snapshot
        self._snapshot_ms: dict[int, float] = {}
        # worker_id -> ms since the last heartbeat (replaced wholesale per check)
        self._heartbeat_age_ms: Mapping[int, float] = {}

    # ------------------------------------------------------------------
    # Recording (event loop)
    # ------------------------------------------------------------------
    def record_heartbeat(
        self,
        worker_id: int,
        cpu_percent: float,
        memory_bytes: int,
        net_rx_bytes: int,
        net_tx_bytes: int,
    ) -> None:
        self._resources[worker_id] = (cpu_percent, memory_bytes, net_rx_bytes, net_tx_bytes)

    def record_epoch(
        self,
        worker_id: int,
        sequenced: int,
        committed: int,
        aborted: int,
        duration_ms: float,
        phase_ms: Iterable[float],
    ) -> None:
        acc = self._epochs.get(worker_id)
        if acc is None:
            acc = self._epochs[worker_id] = [0.0] * _N_EPOCH_FIELDS
        acc[_SEQUENCED] += sequenced
        acc[_COMMITTED] += committed
        acc[_ABORTED] += aborted
        acc[_EPOCHS] += 1
        acc[_DURATION_MS] += duration_ms
        for i, ms in enumerate(phase_ms, _PHASES_START):
            acc[i] += ms

    def record_snapshot(self, worker_id: int, duration_ms: float) -> None:
        self._snapshot_ms[worker_id] = duration_ms

    def set_heartbeat_ages(self, ages_ms: Mapping[int, float]) -> None:
        self._heartbeat_age_ms = ages_ms

    def reset_heartbeat_ages(self, worker_ids: Iterable[int]) -> None:
        self._heartbeat_age_ms = dict.fromkeys(worker_ids, 0.0)

    # ------------------------------------------------------------------
    # Exposition (scrape thread)
    # ------------------------------------------------------------------
    def collect(self) -> Iterable[Metric]:
        worker = ["worker_id"]

        cpu = GaugeMetricFamily("styx_worker_cpu_percent", "Worker process CPU usage (%)", labels=worker)
        mem = GaugeMetricFamily("styx_worker_memory_bytes", "Worker process resident memory", labels=worker)
        rx = CounterMetricFamily(
            "styx_worker_network_receive_bytes",
            "Bytes received by the worker's network namespace, excluding loopback",
            labels=worker,
        )
        tx = CounterMetricFamily(
            "styx_worker_network_transmit_bytes",
            "Bytes transmitted by the worker's network namespace, excluding loopback",
            labels=worker,
        )
        for wid, (cpu_v, mem_v, rx_v, tx_v) in list(self._resources.items()):
            lv = [str(wid)]
            cpu.add_metric(lv, cpu_v)
            mem.add_metric(lv, mem_v)
            rx.add_metric(lv, rx_v)
            tx.add_metric(lv, tx_v)
        yield from (cpu, mem, rx, tx)

        sequenced = CounterMetricFamily(
            "styx_transactions_sequenced",
            "Transactions sequenced into an epoch (re-executions after an abort count again)",
            labels=worker,
        )
        committed = CounterMetricFamily("styx_transactions_committed", "Transactions committed", labels=worker)
        aborted = CounterMetricFamily(
            "styx_transactions_concurrency_aborted",
            "Transactions aborted by Aria conflict detection (before fallback)",
            labels=worker,
        )
        duration = SummaryMetricFamily("styx_epoch_duration_ms", "Epoch duration (ms)", labels=worker)
        phases = CounterMetricFamily(
            "styx_epoch_phase_ms",
            "Time spent in each phase of the transactional protocol (ms)",
            labels=["worker_id", "phase"],
        )
        for wid, acc in list(self._epochs.items()):
            lv = [str(wid)]
            sequenced.add_metric(lv, acc[_SEQUENCED])
            committed.add_metric(lv, acc[_COMMITTED])
            aborted.add_metric(lv, acc[_ABORTED])
            duration.add_metric(lv, count_value=acc[_EPOCHS], sum_value=acc[_DURATION_MS])
            for i, phase in enumerate(EPOCH_PHASES, _PHASES_START):
                phases.add_metric([lv[0], phase], acc[i])
        yield from (sequenced, committed, aborted, duration, phases)

        snapshot = GaugeMetricFamily(
            "styx_worker_snapshot_duration_ms",
            "Duration of the worker's most recent snapshot (ms)",
            labels=worker,
        )
        for wid, ms in list(self._snapshot_ms.items()):
            snapshot.add_metric([str(wid)], ms)
        yield snapshot

        heartbeat = GaugeMetricFamily(
            "styx_worker_heartbeat_age_ms",
            "Time since the coordinator last received a heartbeat from the worker (ms)",
            labels=worker,
        )
        for wid, ms in list(self._heartbeat_age_ms.items()):
            heartbeat.add_metric([str(wid)], ms)
        yield heartbeat


def start_metrics_server(port: int) -> StyxMetrics:
    """Serve only the Styx metrics on ``port``.

    A dedicated registry keeps prometheus_client's default process/GC/platform
    collectors out of every scrape; no dashboard uses them.
    """
    metrics = StyxMetrics()
    registry = CollectorRegistry()
    registry.register(metrics)
    start_http_server(port, registry=registry)
    return metrics
