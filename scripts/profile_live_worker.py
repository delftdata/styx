#!/usr/bin/env python3
"""Profile a *running* Styx worker with py-spy.

The existing profile_hotpaths.py / profile_tpcc.py / profile_ycsb.py scripts
profile components in isolation (synthetic workloads). This one attaches to a
live worker process during a real TPC-C run and produces a flame graph showing
where wall-clock time actually goes — the only reliable way to find real
bottlenecks that synthetic microbenchmarks miss.

Prerequisite:
    pip install py-spy

Usage:
    # Local worker, known PID
    python scripts/profile_live_worker.py --pid 12345 --duration 30

    # Local worker by process name (matches the python script that contains it)
    python scripts/profile_live_worker.py --name worker_service --duration 30

    # All matching processes (e.g. multiple worker threads)
    python scripts/profile_live_worker.py --name worker_service --all --duration 30

    # Top-style live view (no SVG, just see-it-now)
    python scripts/profile_live_worker.py --pid 12345 --top

Kubernetes (run inside the pod):
    kubectl exec -it <worker-pod> -- bash -c 'pip install py-spy && py-spy record --pid 1 --duration 30 -o /tmp/p.svg'
    kubectl cp <worker-pod>:/tmp/p.svg ./worker.svg

Output:
    profile-<pid>-<timestamp>.svg  (flame graph, open in browser)

Notes:
    - py-spy is a sampling profiler so overhead is negligible (~1%).
    - On Linux it needs CAP_SYS_PTRACE or root for attach. Try:
        sudo setcap cap_sys_ptrace=eip $(which py-spy)
    - On Windows it just works (admin not required for own processes).
"""

# ruff: noqa: S603, PLC0415

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import time


def find_pids_by_name(name: str) -> list[int]:
    """Find Python process PIDs whose command line contains `name`."""
    try:
        import psutil
    except ImportError:
        sys.exit("Need psutil for --name lookup. Run: pip install psutil")

    pids: list[int] = []
    for p in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmd = " ".join(p.info.get("cmdline") or [])
            if name in cmd and "python" in (p.info.get("name") or "").lower():
                pids.append(p.info["pid"])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return pids


def require_pyspy() -> str:
    path = shutil.which("py-spy")
    if path is None:
        sys.exit("py-spy not found on PATH. Install with: pip install py-spy")
    return path


def record(pyspy: str, pid: int, duration: int, output: Path) -> int:
    cmd = [pyspy, "record", "--pid", str(pid), "--duration", str(duration), "-o", str(output)]
    print(f"  $ {' '.join(cmd)}")
    return subprocess.call(cmd)


def top(pyspy: str, pid: int) -> int:
    cmd = [pyspy, "top", "--pid", str(pid)]
    print(f"  $ {' '.join(cmd)}")
    return subprocess.call(cmd)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = ap.add_mutually_exclusive_group(required=True)
    target.add_argument("--pid", type=int, help="PID of the Styx worker process")
    target.add_argument("--name", type=str, help="Process-name substring to locate worker(s)")
    ap.add_argument("--duration", type=int, default=30, help="Recording duration in seconds (default: 30)")
    ap.add_argument("--all", action="store_true", help="With --name: profile all matches in parallel")
    ap.add_argument("--top", action="store_true", help="Show live top view instead of recording SVG")
    args = ap.parse_args()

    pyspy = require_pyspy()

    if args.pid is not None:
        pids = [args.pid]
    else:
        pids = find_pids_by_name(args.name)
        if not pids:
            sys.exit(f"No process found matching name={args.name!r}")
        print(f"Found {len(pids)} match(es) for name={args.name!r}: {pids}")
        if not args.all:
            pids = pids[:1]
            print(f"Using first match: {pids[0]} (pass --all to profile all)")

    if args.top:
        if len(pids) != 1:
            sys.exit("--top requires a single PID (omit --all or use --pid)")
        sys.exit(top(pyspy, pids[0]))

    ts = time.strftime("%Y%m%d-%H%M%S")
    procs: list[tuple[int, subprocess.Popen]] = []
    for pid in pids:
        out = Path(f"profile-{pid}-{ts}.svg")
        cmd = [pyspy, "record", "--pid", str(pid), "--duration", str(args.duration), "-o", str(out)]
        print(f"Recording PID {pid} -> {out}")
        procs.append((pid, subprocess.Popen(cmd)))

    failed = 0
    for pid, proc in procs:
        rc = proc.wait()
        if rc != 0:
            print(f"  PID {pid}: py-spy exit {rc}", file=sys.stderr)
            failed += 1
        else:
            print(f"  PID {pid}: done")

    if failed:
        sys.exit(failed)


if __name__ == "__main__":
    main()
