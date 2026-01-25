import json
import logging
import os
import socket
import subprocess
import time
from pathlib import Path
import threading

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


def run_and_stream_with_timed_action(
    cmd,
    *,
    cwd: str,
    env: dict,
    timeout: float,
    banner: str,
    action_at_s: float,
    action_cmd: list[str],
    action_banner: str,
):
    """
    Streams stdout like run_and_stream, but triggers action_cmd at action_at_s seconds
    after process start, regardless of stdout activity.
    """
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
    action_done = threading.Event()
    action_result = {"rc": None, "out": ""}

    def _timer_action():
        # Sleep independently of stdout streaming
        time.sleep(action_at_s)
        if proc.poll() is not None:
            # Process already finished; nothing to do.
            action_done.set()
            return

        log.info("=" * 100)
        log.info("TIMED ACTION: %s", action_banner)
        log.info("CMD: %s", " ".join(action_cmd))
        log.info("=" * 100)

        run = subprocess.run(
            action_cmd,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        action_result["rc"] = run.returncode
        action_result["out"] = run.stdout or ""

        log.info("TIMED ACTION FINISHED: rc=%s", action_result["rc"])
        if action_result["out"].strip():
            for l in action_result["out"].splitlines():
                log.info("[timed-action] %s", l)

        action_done.set()

    t = threading.Thread(target=_timer_action, name="e2e-timed-action", daemon=True)
    t.start()

    try:
        assert proc.stdout is not None
        start = time.time()

        for line in proc.stdout:
            output_lines.append(line)
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

    # Ensure the action thread has had a chance to run (or decide not to)
    action_done.wait(timeout=5)

    log.info("=" * 100)
    log.info("FINISHED: %s (rc=%s)", banner, rc)
    if action_result["rc"] is None:
        log.info("TIMED ACTION SUMMARY: %s was NOT run (process ended before %ss)", action_banner, action_at_s)
    else:
        log.info("TIMED ACTION SUMMARY: %s rc=%s", action_banner, action_result["rc"])
    log.info("=" * 100)

    # If the timed action ran and failed, fail the test even if the client returned 0
    if action_result["rc"] not in (None, 0):
        raise AssertionError(
            f"Timed action failed (rc={action_result['rc']}): {action_banner}\nOutput:\n{action_result['out']}"
        )

    return rc, "".join(output_lines)



def _assert_metrics(results_dir: Path, zipf_const: float, input_rate: int, client_threads: int):
    if zipf_const > 0:
        exp_name = f"ycsbt_zipf_{zipf_const}_{input_rate * client_threads}"
    else:
        exp_name = f"ycsbt_uni_{input_rate * client_threads}"

    metrics_json = results_dir / f"{exp_name}.json"
    log.info("Checking metrics json: %s", metrics_json)
    assert metrics_json.exists(), f"Missing metrics json: {metrics_json}"

    with metrics_json.open("r", encoding="utf-8") as f:
        metrics = json.load(f)

    log.info("==== METRICS JSON (%s) ====", metrics_json)
    log.info("%s", json.dumps(metrics, indent=2, sort_keys=True))
    log.info("==== END METRICS JSON ====")

    assert metrics.get("duplicate_requests") is False, metrics
    assert metrics.get("exactly_once_output") is True, metrics
    assert metrics.get("total_consistent") is True, metrics
    assert metrics.get("are_we_consistent") is True, metrics


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

    n_partitions = 4
    epoch_size = 1000
    threads_per_worker = 1
    enable_compression = "true"
    use_composite_keys = "true"
    use_fallback_cache = "true"

    client_threads = 2
    n_keys = 10_000
    zipf_const = 0.0
    input_rate = 200
    total_time = 10
    warmup_seconds = 1
    run_with_validation = "true"

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

        _assert_metrics(results_dir, zipf_const, input_rate, client_threads)

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


@pytest.mark.e2e
def test_styx_e2e_ycsb_kill_worker_midrun(tmp_path: Path):
    """
    Same scenario/params, but:
      - total_time = 60 seconds
      - at t=30s after client start: docker kill styx-worker-1
    """
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

    n_partitions = 4
    epoch_size = 1000
    threads_per_worker = 1
    enable_compression = "true"
    use_composite_keys = "true"
    use_fallback_cache = "true"

    client_threads = 2
    n_keys = 10_000
    zipf_const = 0.0
    input_rate = 200
    total_time = 60
    warmup_seconds = 1
    run_with_validation = "true"

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

        # Kill styx-worker-1 at 30s after client starts.
        kill_cmd = ["docker", "kill", "styx-worker-1"]

        rc, out = run_and_stream_with_timed_action(
            client_cmd,
            cwd=str(demo_dir),
            env=env,
            timeout=30 * 60,  # give headroom for recovery
            banner="RUN YCSB CLIENT (KILL worker at 30s)",
            action_at_s=40.0,
            action_cmd=kill_cmd,
            action_banner="docker kill styx-worker-1",
        )
        if rc != 0:
            raise AssertionError(f"client.py failed (rc={rc}). Output:\n{out}")

        client_csv = results_dir / "client_requests.csv"
        output_csv = results_dir / "output.csv"
        log.info("Checking artifacts: %s and %s", client_csv, output_csv)

        assert client_csv.exists(), f"Missing artifact: {client_csv}"
        assert output_csv.exists(), f"Missing artifact: {output_csv}"

        _assert_metrics(results_dir, zipf_const, input_rate, client_threads)

    finally:
        rc, out = run_and_stream(
            stop_cmd,
            cwd=str(repo_root),
            env=env,
            timeout=15 * 60,
            banner="STOP STYX CLUSTER",
        )
        if rc != 0:
            log.warning("stop_styx_cluster.sh failed (rc=%s). Output:\n%s", rc, out)
