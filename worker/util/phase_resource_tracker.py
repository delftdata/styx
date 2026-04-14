from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import os
from pathlib import Path
import time

import psutil


@dataclass(frozen=True)
class ResourceSnapshot:
    cpu_ns: int
    rss_bytes: int
    rx_bytes: int
    tx_bytes: int


def _read_net_bytes() -> tuple[int, int]:
    """
    Reads cumulative RX/TX bytes from the current network namespace.

    Note: this is interface-level accounting (namespace/container-level), not truly per-process.
    It's still useful for attributing traffic deltas to phases within a worker process.
    """
    try:
        with Path.open("/proc/self/net/dev", encoding="utf-8") as f:
            lines = f.readlines()[2:]  # skip headers
    except FileNotFoundError:
        return 0, 0

    rx, tx = 0, 0
    min_parts = 10
    for line in lines:
        parts = line.split()
        # Format: iface: rx_bytes rx_packets ... tx_bytes tx_packets ...
        if len(parts) < min_parts:
            continue
        rx += int(parts[1])
        tx += int(parts[9])
    return rx, tx


class PhaseResourceTracker:
    """
    Tracks per-phase resource deltas within a single process.

    - CPU: process CPU time (ns) via time.process_time_ns()
    - Memory: RSS max observed during the phase (bytes)
    - Network: cumulative RX/TX bytes for the current net namespace (delta attributed to phase)
    """

    def __init__(self, pid: int | None = None) -> None:
        self._pid = pid if pid is not None else os.getpid()
        self._proc = psutil.Process(self._pid)
        self._starts: dict[str, ResourceSnapshot] = {}
        self._cpu_ns_total: dict[str, int] = defaultdict(int)
        self._rx_bytes_total: dict[str, int] = defaultdict(int)
        self._tx_bytes_total: dict[str, int] = defaultdict(int)
        self._rss_max_bytes: dict[str, int] = defaultdict(int)

    def reset_epoch(self) -> None:
        self._starts.clear()
        self._cpu_ns_total.clear()
        self._rx_bytes_total.clear()
        self._tx_bytes_total.clear()
        self._rss_max_bytes.clear()

    def _snapshot(self) -> ResourceSnapshot:
        cpu_ns = time.process_time_ns()
        rss_bytes = int(self._proc.memory_info().rss)
        rx_bytes, tx_bytes = _read_net_bytes()
        return ResourceSnapshot(cpu_ns=cpu_ns, rss_bytes=rss_bytes, rx_bytes=rx_bytes, tx_bytes=tx_bytes)

    def begin(self, phase: str) -> None:
        snap = self._snapshot()
        self._starts[phase] = snap
        self._rss_max_bytes[phase] = max(self._rss_max_bytes.get(phase, 0), snap.rss_bytes)

    def end(self, phase: str) -> None:
        start = self._starts.pop(phase, None)
        if start is None:
            return
        end = self._snapshot()
        self._cpu_ns_total[phase] += max(0, end.cpu_ns - start.cpu_ns)
        self._rx_bytes_total[phase] += max(0, end.rx_bytes - start.rx_bytes)
        self._tx_bytes_total[phase] += max(0, end.tx_bytes - start.tx_bytes)
        self._rss_max_bytes[phase] = max(self._rss_max_bytes.get(phase, 0), end.rss_bytes)

    def export(self) -> dict[str, dict[str, int]]:
        return {
            "cpu_ns": dict(self._cpu_ns_total),
            "rx_bytes": dict(self._rx_bytes_total),
            "tx_bytes": dict(self._tx_bytes_total),
            "rss_max_bytes": dict(self._rss_max_bytes),
        }
