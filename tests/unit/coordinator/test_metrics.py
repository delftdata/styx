"""Unit tests for coordinator/metrics.py"""

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.parser import text_string_to_metric_families

from coordinator.metrics import EPOCH_PHASES, StyxMetrics


def _scrape(metrics: StyxMetrics) -> dict[tuple[str, tuple], float]:
    registry = CollectorRegistry()
    registry.register(metrics)
    text = generate_latest(registry).decode()
    return {
        (sample.name, tuple(sorted(sample.labels.items()))): sample.value
        for family in text_string_to_metric_families(text)
        for sample in family.samples
    }


def _w(worker_id: int, **extra: str) -> tuple:
    return tuple(sorted({"worker_id": str(worker_id), **extra}.items()))


class TestEmpty:
    def test_scrape_with_no_data_is_valid(self):
        assert _scrape(StyxMetrics()) == {}


class TestHeartbeat:
    def test_resources_are_latest_value(self):
        m = StyxMetrics()
        m.record_heartbeat(1, 10.0, 1_000, 100, 200)
        m.record_heartbeat(1, 55.5, 2_048, 300, 400)
        samples = _scrape(m)
        assert samples[("styx_worker_cpu_percent", _w(1))] == 55.5
        assert samples[("styx_worker_memory_bytes", _w(1))] == 2_048
        assert samples[("styx_worker_network_receive_bytes_total", _w(1))] == 300
        assert samples[("styx_worker_network_transmit_bytes_total", _w(1))] == 400

    def test_heartbeat_ages_replace_previous_set(self):
        m = StyxMetrics()
        m.set_heartbeat_ages({1: 12.0, 2: 30.0})
        m.set_heartbeat_ages({1: 5.0})
        samples = _scrape(m)
        assert samples[("styx_worker_heartbeat_age_ms", _w(1))] == 5.0
        assert ("styx_worker_heartbeat_age_ms", _w(2)) not in samples

    def test_reset_heartbeat_ages(self):
        m = StyxMetrics()
        m.set_heartbeat_ages({1: 999.0})
        m.reset_heartbeat_ages([1, 2])
        samples = _scrape(m)
        assert samples[("styx_worker_heartbeat_age_ms", _w(1))] == 0.0
        assert samples[("styx_worker_heartbeat_age_ms", _w(2))] == 0.0


class TestEpoch:
    def test_epoch_counters_accumulate(self):
        m = StyxMetrics()
        phases = [float(i) for i in range(len(EPOCH_PHASES))]
        m.record_epoch(1, sequenced=10, committed=8, aborted=2, duration_ms=4.0, phase_ms=phases)
        m.record_epoch(1, sequenced=5, committed=5, aborted=0, duration_ms=6.0, phase_ms=phases)
        m.record_epoch(2, sequenced=1, committed=1, aborted=0, duration_ms=1.0, phase_ms=phases)
        samples = _scrape(m)

        assert samples[("styx_transactions_sequenced_total", _w(1))] == 15
        assert samples[("styx_transactions_committed_total", _w(1))] == 13
        assert samples[("styx_transactions_concurrency_aborted_total", _w(1))] == 2
        assert samples[("styx_epoch_duration_ms_count", _w(1))] == 2
        assert samples[("styx_epoch_duration_ms_sum", _w(1))] == 10.0
        for i, phase in enumerate(EPOCH_PHASES):
            assert samples[("styx_epoch_phase_ms_total", _w(1, phase=phase))] == 2 * i
        assert samples[("styx_transactions_committed_total", _w(2))] == 1


class TestSnapshot:
    def test_snapshot_duration_is_latest(self):
        m = StyxMetrics()
        m.record_snapshot(3, 120.0)
        m.record_snapshot(3, 80.0)
        assert _scrape(m)[("styx_worker_snapshot_duration_ms", _w(3))] == 80.0
