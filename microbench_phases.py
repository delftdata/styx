#!/usr/bin/env python3
"""
microbench_phases.py — Styx-shaped TCP messaging benchmark.

Models the *actual* Styx transaction pattern that the previous benchmark missed:

  - Each transaction goes through K phases.
  - Each phase: send N messages → block waiting for N responses → next phase.
  - The blocking wait is where buffering latency hurts: any artificial delay
    added to flushing is paid per phase, on the critical path.

Compares 4 strategies:
  1. seq       — write+drain per message (pre-batching baseline)
  2. timer1ms  — buffer + background flush every 1ms (the regression)
  3. sleep0    — buffer + flush every event loop turn
  4. explicit  — buffer + caller calls flush_phase() after each phase's sends

Usage:
    python microbench_phases.py
"""

# ruff: noqa: T201

from __future__ import annotations

import asyncio
import contextlib
from itertools import cycle
import socket
import statistics
import struct
import time

# ---------------------------------------------------------------------------
# Wire format
# ---------------------------------------------------------------------------
# Frame:    8-byte big-endian size + body
# Body:     16-byte (txn_id, msg_id) + arbitrary payload
# Ack:      16-byte (txn_id, msg_id) only — server echoes the IDs


def make_message(txn_id: int, msg_id: int, payload_size: int) -> bytes:
    body = struct.pack(">QQ", txn_id, msg_id) + b"x" * max(0, payload_size - 16)
    return struct.pack(">Q", len(body)) + body


# ---------------------------------------------------------------------------
# Echo server — for every message received, writes back (txn_id, msg_id)
# ---------------------------------------------------------------------------


class EchoServer:
    def __init__(self) -> None:
        self._server: asyncio.Server | None = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                header = await reader.readexactly(8)
                (size,) = struct.unpack(">Q", header)
                body = await reader.readexactly(size)
                ids = body[:16]  # (txn_id, msg_id)
                writer.write(struct.pack(">Q", len(ids)) + ids)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    async def start(self, host: str = "127.0.0.1") -> tuple[str, int]:
        self._server = await asyncio.start_server(self._handle, host, 0)
        return self._server.sockets[0].getsockname()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


# ---------------------------------------------------------------------------
# Styx-shaped client
# ---------------------------------------------------------------------------

POOL_SIZE = 4


class Client:
    """
    4-connection pool, per-connection send buffers, per-connection reader task
    that demultiplexes responses by (txn_id, msg_id).
    """

    def __init__(self, strategy: str) -> None:
        # strategy ∈ {'seq', 'timer1ms', 'sleep0', 'explicit'}
        self.strategy = strategy
        self._writers: list[asyncio.StreamWriter] = []
        self._readers: list[asyncio.StreamReader] = []
        self._buffers: list[list[bytes]] = []
        self._locks: list[asyncio.Lock] = []
        self._pending: dict[tuple[int, int], asyncio.Future] = {}
        self._cycle = None
        self._reader_tasks: list[asyncio.Task] = []
        self._flush_task: asyncio.Task | None = None

    async def connect(self, host: str, port: int) -> None:
        for _ in range(POOL_SIZE):
            r, w = await asyncio.open_connection(host, port)
            sock: socket.socket = w.get_extra_info("socket")
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
            self._readers.append(r)
            self._writers.append(w)
            self._buffers.append([])
            self._locks.append(asyncio.Lock())
        self._cycle = cycle(range(POOL_SIZE))
        for i in range(POOL_SIZE):
            self._reader_tasks.append(asyncio.create_task(self._read_loop(i)))
        if self.strategy == "timer1ms":
            self._flush_task = asyncio.create_task(self._flush_loop(0.001))
        elif self.strategy == "sleep0":
            self._flush_task = asyncio.create_task(self._flush_loop(0))

    async def _read_loop(self, idx: int) -> None:
        r = self._readers[idx]
        try:
            while True:
                header = await r.readexactly(8)
                (size,) = struct.unpack(">Q", header)
                body = await r.readexactly(size)
                txn_id, msg_id = struct.unpack(">QQ", body[:16])
                fut = self._pending.pop((txn_id, msg_id), None)
                if fut is not None and not fut.done():
                    fut.set_result(None)
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            pass

    async def _flush_loop(self, interval: float) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                await self._flush_all()
        except asyncio.CancelledError:
            await self._flush_all()

    async def _flush_all(self) -> None:
        for i in range(POOL_SIZE):
            if not self._buffers[i]:
                continue
            async with self._locks[i]:
                if not self._buffers[i]:
                    continue
                data = b"".join(self._buffers[i])
                self._buffers[i].clear()
                self._writers[i].write(data)
                await self._writers[i].drain()

    def _pick(self) -> int:
        return next(self._cycle)

    async def send(self, msg: bytes, txn_id: int, msg_id: int) -> asyncio.Future:
        """Issue one message. Returns a future that resolves when the ACK arrives."""
        idx = self._pick()
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[(txn_id, msg_id)] = fut
        if self.strategy == "seq":
            async with self._locks[idx]:
                self._writers[idx].write(msg)
                await self._writers[idx].drain()
        else:
            self._buffers[idx].append(msg)
        return fut

    async def flush_phase(self) -> None:
        """Caller hint that a phase's sends are done. Only acted on for 'explicit'."""
        if self.strategy == "explicit":
            await self._flush_all()

    async def close(self) -> None:
        if self._flush_task is not None:
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task
        for t in self._reader_tasks:
            t.cancel()
        for t in self._reader_tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        for w in self._writers:
            with contextlib.suppress(Exception):
                w.close()
                await w.wait_closed()


# ---------------------------------------------------------------------------
# Transaction model: K phases, N_k messages per phase, blocking wait per phase
# ---------------------------------------------------------------------------


async def run_transaction(
    client: Client,
    txn_id: int,
    phases: list[int],
    payload: int,
) -> float:
    """Returns end-to-end latency in milliseconds."""
    t0 = time.perf_counter()
    msg_counter = 0
    for n_msgs in phases:
        futures = []
        for _ in range(n_msgs):
            msg = make_message(txn_id, msg_counter, payload)
            fut = await client.send(msg, txn_id, msg_counter)
            futures.append(fut)
            msg_counter += 1
        await client.flush_phase()  # no-op except for 'explicit'
        await asyncio.gather(*futures)
    return (time.perf_counter() - t0) * 1000.0


async def bench(
    strategy: str,
    host: str,
    port: int,
    phases: list[int],
    n_concurrent: int,
    payload: int,
    n_warmup: int = 5,
) -> tuple[float, float, list[float]]:
    client = Client(strategy)
    await client.connect(host, port)
    try:
        # Warmup
        await asyncio.gather(*[run_transaction(client, i, phases, payload) for i in range(n_warmup)])
        # Measure
        t0 = time.perf_counter()
        latencies = await asyncio.gather(
            *[run_transaction(client, n_warmup + i, phases, payload) for i in range(n_concurrent)],
        )
        elapsed = time.perf_counter() - t0
    finally:
        await client.close()
    return elapsed, n_concurrent / elapsed, latencies


def _pct(values: list[float], p: float) -> float:
    s = sorted(values)
    return s[max(0, int(len(s) * p) - 1)]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    payload = 256
    server = EchoServer()
    host, port = await server.start()

    scenarios = [
        ("NewOrder  (phases: 3, 18, 15, 30 — 66 msgs total)", [3, 18, 15, 30]),
        ("Payment   (phases: 3, 4 — 7 msgs total)", [3, 4]),
    ]
    strategies = ["seq", "timer1ms", "sleep0", "explicit"]
    concurrency_levels = [1, 10, 100, 500]

    sep = "=" * 88
    print(f"\n{sep}")
    print("STYX-SHAPED MICROBENCH  —  phase-based, round-trip per phase")
    print(f"  payload          : {payload} bytes")
    print(f"  pool size        : {POOL_SIZE}")
    print(f"  concurrency      : {concurrency_levels}")
    print(f"  strategies       : {strategies}")
    print(sep)

    for name, phases in scenarios:
        print(f"\n── {name}")
        print(
            f"   {'strategy':<10}  {'conc':>5}  {'mean':>9}  {'p50':>9}  "
            f"{'p99':>9}  {'throughput':>14}",
        )
        for n_conc in concurrency_levels:
            for strat in strategies:
                _elapsed, tps, lats = await bench(strat, host, port, phases, n_conc, payload)
                mean = statistics.mean(lats)
                p50 = _pct(lats, 0.50)
                p99 = _pct(lats, 0.99)
                print(
                    f"   {strat:<10}  {n_conc:>5}  {mean:>7.2f}ms  {p50:>7.2f}ms  "
                    f"{p99:>7.2f}ms  {tps:>10,.0f} txn/s",
                )
            print()  # blank line between concurrency levels

    await server.stop()
    print(f"{sep}\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
