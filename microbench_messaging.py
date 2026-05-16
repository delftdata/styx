#!/usr/bin/env python3
"""
microbench_messaging.py — Styx TCP messaging microbenchmark
Sequential vs batched sends, modelling Styx's exact wire protocol and pool.

Sequential (current):  one writer.write() + await writer.drain() per message
Batched   (proposed):  accumulate all phase messages, one write() + drain() per flush

The benchmark isolates the send layer only — no Styx workers, no serialization
overhead, just raw TCP with the same framing Styx uses.

Usage:
    python microbench_messaging.py
    python microbench_messaging.py --items 15 --payload 256 --iterations 2000
    python microbench_messaging.py --concurrent 10 100 500 1000
"""

from __future__ import annotations

# ruff: noqa: T201
import argparse
import asyncio
import contextlib
from itertools import cycle
import socket
import statistics
import struct
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

# ---------------------------------------------------------------------------
# Wire framing  (matches Styx: 8-byte big-endian size header + payload)
# ---------------------------------------------------------------------------


def make_message(payload_size: int) -> bytes:
    payload = b"x" * payload_size
    return struct.pack(">Q", len(payload)) + payload


# ---------------------------------------------------------------------------
# TCP sink  —  reads and discards messages as fast as possible
# ---------------------------------------------------------------------------


class MessageSink:
    def __init__(self) -> None:
        self._server = None
        self.total_received = 0

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                header = await reader.readexactly(8)
                (size,) = struct.unpack(">Q", header)
                await reader.readexactly(size)
                self.total_received += 1
        except asyncio.IncompleteReadError, ConnectionResetError, OSError:
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()

    async def start(self, host: str = "127.0.0.1") -> tuple[str, int]:
        self._server = await asyncio.start_server(self._handle, host, 0)
        return self._server.sockets[0].getsockname()

    async def stop(self) -> None:
        self._server.close()
        await self._server.wait_closed()


# ---------------------------------------------------------------------------
# Connection pool  —  mirrors Styx's SocketPool (default 4 connections/remote)
# ---------------------------------------------------------------------------

POOL_SIZE = 4  # Styx default socket_pool_size


class Pool:
    def __init__(self) -> None:
        self._writers: list[asyncio.StreamWriter] = []
        self._locks: list[asyncio.Lock] = []
        self._cycle = None

    async def connect(self, host: str, port: int) -> None:
        for _ in range(POOL_SIZE):
            _, writer = await asyncio.open_connection(host, port)
            sock: socket.socket = writer.get_extra_info("socket")
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
            self._writers.append(writer)
            self._locks.append(asyncio.Lock())
        self._cycle = cycle(range(POOL_SIZE))

    def _pick(self) -> tuple[asyncio.StreamWriter, asyncio.Lock]:
        idx = next(self._cycle)
        return self._writers[idx], self._locks[idx]

    async def send_sequential(self, messages: list[bytes]) -> None:
        """
        Current Styx behaviour: every call_remote_async triggers one
        send_message() call which acquires the connection lock, writes the
        single message, and drains immediately.
        """
        for msg in messages:
            writer, lock = self._pick()
            async with lock:
                writer.write(msg)
                await writer.drain()

    async def send_batched(self, messages: list[bytes]) -> None:
        """
        Proposed optimisation: accumulate all messages for this phase into a
        single bytes object, acquire the lock once, write once, drain once.
        The server parses them correctly because each message carries its own
        8-byte size header.
        """
        writer, lock = self._pick()
        async with lock:
            writer.write(b"".join(messages))
            await writer.drain()

    async def close(self) -> None:
        for w in self._writers:
            with contextlib.suppress(Exception):
                w.close()
                await w.wait_closed()


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------


async def bench_latency(
    messages: list[bytes],
    send_fn: Callable[[list[bytes]], Awaitable[None]],
    n_iter: int,
    n_warmup: int,
) -> list[float]:
    """Single-transaction latency: measure time to send all phase messages."""
    for _ in range(n_warmup):
        await send_fn(messages)
    latencies: list[float] = []
    for _ in range(n_iter):
        t0 = time.perf_counter()
        await send_fn(messages)
        latencies.append((time.perf_counter() - t0) * 1e6)
    return latencies


async def bench_throughput(
    messages: list[bytes],
    send_fn: Callable[[list[bytes]], Awaitable[None]],
    n_concurrent: int,
    n_warmup_frac: float = 0.1,
) -> tuple[float, float]:
    """
    Simulate an epoch of n_concurrent transactions all sending their phase
    messages simultaneously.  Returns (elapsed_seconds, messages_per_second).
    """
    warmup = max(1, int(n_concurrent * n_warmup_frac))
    await asyncio.gather(*[send_fn(messages) for _ in range(warmup)])

    t0 = time.perf_counter()
    await asyncio.gather(*[send_fn(messages) for _ in range(n_concurrent)])
    elapsed = time.perf_counter() - t0

    return elapsed, (n_concurrent * len(messages)) / elapsed


def _pct(values: list[float], p: float) -> float:
    s = sorted(values)
    return s[max(0, int(len(s) * p) - 1)]


def fmt_lat(latencies: list[float]) -> str:
    return (
        f"mean={statistics.mean(latencies):6.1f}µs  "
        f"p50={_pct(latencies, 0.50):6.1f}µs  "
        f"p95={_pct(latencies, 0.95):7.1f}µs  "
        f"p99={_pct(latencies, 0.99):7.1f}µs"
    )


# ---------------------------------------------------------------------------
# TPC-C scenarios derived from the message trace
# n = number of order lines per NewOrder transaction
# ---------------------------------------------------------------------------


def scenarios(n: int) -> list[tuple[str, int]]:
    return [
        ("NewOrder phase-1  coordinator → warehouse + district + customer", 3),
        (f"NewOrder phase-2  district → txn_coord + {n} items + order + new_order", n + 3),
        (f"NewOrder phase-3  {n} items → {n} stocks", n),
        (f"NewOrder phase-4  {n} stocks → txn_coord({n}) + order_line({n})", 2 * n),
        (f"NewOrder total    all phases combined ({4 * n + 8} msgs, N={n})", 4 * n + 8),
        ("Payment           coordinator → warehouse + district + customer", 3),
        ("Payment           3 replies → txn_coord + history insert", 4),
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run(args: argparse.Namespace) -> None:
    n_items = args.items
    payload_size = args.payload
    n_iter = args.iterations
    n_warmup = max(10, n_iter // 20)
    concurrent_levels: list[int] = args.concurrent

    sep = "=" * 72
    print(f"\n{sep}")
    print("STYX TCP MESSAGING MICROBENCHMARK  —  sequential vs batched")
    print(f"  Payload per message : {payload_size} bytes")
    print(f"  Pool size           : {POOL_SIZE} connections / remote  (Styx default)")
    print(f"  NewOrder items (N)  : {n_items}")
    print(f"  Latency iterations  : {n_iter}")
    print(f"  Concurrency levels  : {concurrent_levels}")
    print(sep)

    for name, n_msgs in scenarios(n_items):
        messages = [make_message(payload_size)] * n_msgs

        sink = MessageSink()
        host, port = await sink.start()
        pool = Pool()
        await pool.connect(host, port)

        print(f"\n── {name}")
        print(f"   {n_msgs} msg/txn, {payload_size} bytes/msg  = {n_msgs * (payload_size + 8):,} bytes/txn")

        # -- Latency ----------------------------------------------------------
        seq_lat = await bench_latency(messages, pool.send_sequential, n_iter, n_warmup)
        bat_lat = await bench_latency(messages, pool.send_batched, n_iter, n_warmup)

        speedup_p50 = _pct(seq_lat, 0.50) / _pct(bat_lat, 0.50)
        speedup_p99 = _pct(seq_lat, 0.99) / _pct(bat_lat, 0.99)

        print(f"   Latency  sequential : {fmt_lat(seq_lat)}")
        print(f"            batched    : {fmt_lat(bat_lat)}")
        print(f"            speedup    : {speedup_p50:.1f}x (p50)  {speedup_p99:.1f}x (p99)")

        # -- Throughput -------------------------------------------------------
        print("   Throughput  [concurrent txns  →  messages / second]")
        print(f"   {'txns':>6}   {'sequential':>14}   {'batched':>14}   speedup")
        for n_conc in concurrent_levels:
            _seq_el, seq_tp = await bench_throughput(messages, pool.send_sequential, n_conc)
            _bat_el, bat_tp = await bench_throughput(messages, pool.send_batched, n_conc)
            print(f"   {n_conc:>6}   {seq_tp:>11,.0f} msg/s   {bat_tp:>11,.0f} msg/s   {bat_tp / seq_tp:.1f}x")

        await pool.close()
        await sink.stop()

    print(f"\n{sep}\nDone.")


def main() -> None:
    p = argparse.ArgumentParser(description="Styx TCP messaging microbenchmark")
    p.add_argument("--items", type=int, default=15, help="NewOrder order lines N (default 15, max spec 15)")
    p.add_argument("--payload", type=int, default=256, help="Bytes per message payload (default 256)")
    p.add_argument("--iterations", type=int, default=1000, help="Latency iterations per scenario (default 1000)")
    p.add_argument(
        "--concurrent",
        nargs="+",
        type=int,
        default=[10, 100, 500, 1000],
        help="Concurrency levels for throughput test (default: 10 100 500 1000)",
    )
    asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    main()
