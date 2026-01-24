import logging
import os
import socket
import subprocess
import time
from pathlib import Path

import pytest


log = logging.getLogger("e2e.ycsb")


def wait_port(host: str, port: int, timeout_s: float = 120.0) -> None:
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as e:
            last_err = e
            time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for {host}:{port} (last error: {last_err})")


def run_and_stream(cmd, *, cwd: str, env: dict, timeout: float, banner: str):
    log.info("=" * 100)
    log.info("RUNNING: %s", banner)
    log.info("CMD: %s", " ".join(cmd))
    log.info("CWD: %s", cwd)
    log.info("=" * 100)

    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    output_lines: list[str] = []

    try:
        assert proc.stdout is not None
        start = time.time()
        for line in proc.stdout:
            output_lines.append(line)
            # keep it single-line to avoid noisy log record formatting
            log.info("[subprocess] %s", line.rstrip("\n"))

            if (time.time() - start) > timeout:
                proc.kill()
                raise TimeoutError(f"Timeout while running {banner} (>{timeout}s)")
    finally:
        try:
            proc.stdout.close() if proc.stdout else None
        except Exception:
            pass

    rc = proc.wait(timeout=30)

    log.info("=" * 100)
    log.info("FINISHED: %s (rc=%s)", banner, rc)
    log.info("=" * 100)

    return rc, "".join(output_lines)


@pytest.mark.e2e
def test_styx_e2e_ycsb(tmp_path: Path):
    repo_root = Path(__file__).resolve().parents[1]
    demo_dir = repo_root / "demo" / "demo-ycsb"
    assert demo_dir.exists(), f"Expected demo dir at {demo_dir}"

    start_script = repo_root / "scripts" / "start_styx_cluster.sh"
    stop_script = repo_root / "scripts" / "stop_styx_cluster.sh"
    assert start_script.exists(), f"Missing: {start_script}"
    assert stop_script.exists(), f"Missing: {stop_script}"

    results_dir = tmp_path / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    log.info("Results dir: %s", results_dir)

    # ---- cluster params (match your script expectations) ----
    n_partitions = 4
    epoch_size = 1000
    threads_per_worker = 1
    enable_compression = "true"
    use_composite_keys = "true"
    use_fallback_cache = "true"

    # ---- client params ----
    client_threads = 2
    n_keys = 50
    zipf_const = 0.0
    input_rate = 200  # IMPORTANT: must be >= sleeps_per_second in client.py to avoid /0
    total_time = 4
    warmup_seconds = 1
    run_with_validation = "false"

    env = os.environ.copy()

    start_cmd = [
        "bash",
        str(start_script),
        str(n_partitions),
        str(epoch_size),
        str(n_partitions),
        str(threads_per_worker),
        enable_compression,
        use_composite_keys,
        use_fallback_cache,
    ]

    stop_cmd = ["bash", str(stop_script), str(threads_per_worker)]

    try:
        rc, out = run_and_stream(
            start_cmd,
            cwd=str(repo_root),
            env=env,
            timeout=20 * 60,
            banner="START STYX CLUSTER",
        )
        if rc != 0:
            raise AssertionError(f"start_styx_cluster.sh failed (rc={rc}). Output:\n{out}")

        log.info("Waiting for Kafka external listener (127.0.0.1:9092)...")
        wait_port("127.0.0.1", 9092, timeout_s=180)
        log.info("Kafka port is up.")

        log.info("Waiting for Styx coordinator published port (127.0.0.1:8886)...")
        wait_port("127.0.0.1", 8886, timeout_s=240)
        log.info("Coordinator port is up.")

        client_cmd = [
            "python",
            "client.py",
            str(client_threads),
            str(n_keys),
            str(n_partitions),
            str(zipf_const),
            str(input_rate),
            str(total_time),
            str(results_dir),
            str(warmup_seconds),
            run_with_validation,
        ]

        rc, out = run_and_stream(
            client_cmd,
            cwd=str(demo_dir),
            env=env,
            timeout=20 * 60,
            banner="RUN YCSB CLIENT",
        )
        if rc != 0:
            raise AssertionError(f"client.py failed (rc={rc}). Output:\n{out}")

        client_csv = results_dir / "client_requests.csv"
        output_csv = results_dir / "output.csv"
        log.info("Checking artifacts: %s and %s", client_csv, output_csv)

        assert client_csv.exists(), f"Missing artifact: {client_csv}"
        assert output_csv.exists(), f"Missing artifact: {output_csv}"

    finally:
        rc, out = run_and_stream(
            stop_cmd,
            cwd=str(repo_root),
            env=env,
            timeout=10 * 60,
            banner="STOP STYX CLUSTER",
        )
        if rc != 0:
            log.warning("stop_styx_cluster.sh failed (rc=%s). Output:\n%s", rc, out)
