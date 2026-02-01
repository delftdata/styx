from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import shutil

import pytest

from tests.helpers import run_and_stream, wait_port

log = logging.getLogger("e2e.tpcc")


def _assert_metrics(
    results_dir: Path,
    input_rate: int,
    client_threads: int,
) -> None:
    exp_name = f"tpcc_W1_{input_rate * client_threads}_ALL"
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
    assert int(metrics.get("missed messages")) == 0, metrics


@dataclass(frozen=True)
class _ClusterParams:
    n_partitions: int = 4
    epoch_size: int = 100
    threads_per_worker: int = 1
    enable_compression: str = "true"
    use_composite_keys: str = "true"
    use_fallback_cache: str = "true"


@dataclass(frozen=True)
class _ClientParams:
    client_threads: int = 1
    input_rate: int = 100
    total_time: int = 10
    warmup_seconds: int = 1
    n_keys: int = 1
    regenerate_tpcc_data: bool = True
    n_warehouses: int = 1
    kill_at: int = -1  # <0 disables killing; client checks `0 <= kill_at == second`


@dataclass(frozen=True)
class _Paths:
    repo_root: Path
    demo_dir: Path
    start_script: Path
    stop_script: Path


def _resolve_paths() -> _Paths:
    repo_root = Path(__file__).resolve().parents[1]
    demo_dir = repo_root / "demo" / "demo-tpc-c"
    start_script = repo_root / "scripts" / "start_styx_cluster.sh"
    stop_script = repo_root / "scripts" / "stop_styx_cluster.sh"

    assert demo_dir.exists(), f"Expected demo dir at {demo_dir}"
    assert start_script.exists(), f"Missing: {start_script}"
    assert stop_script.exists(), f"Missing: {stop_script}"

    return _Paths(
        repo_root=repo_root,
        demo_dir=demo_dir,
        start_script=start_script,
        stop_script=stop_script,
    )


def _make_results_dir(tmp_path: Path) -> Path:
    results_dir = tmp_path / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    log.info("Results dir: %s", results_dir)
    return results_dir


def _start_cmd(paths: _Paths, p: _ClusterParams) -> list[str]:
    return [
        "bash",
        str(paths.start_script),
        str(p.n_partitions),
        str(p.epoch_size),
        str(p.n_partitions),
        str(p.threads_per_worker),
        p.enable_compression,
        p.use_composite_keys,
        p.use_fallback_cache,
    ]


def _stop_cmd(paths: _Paths, p: _ClusterParams) -> list[str]:
    return ["bash", str(paths.stop_script), str(p.threads_per_worker)]


def _client_cmd(results_dir: Path, cluster: _ClusterParams, client: _ClientParams) -> list[str]:
    # Your TPCC script expects:
    #  1  SAVE_DIR
    #  2  threads
    #  3  N_PARTITIONS
    #  4  messages_per_second
    #  5  seconds
    #  6  warmup_seconds
    #  7  N_W
    #  8  enable_compression
    #  9  use_composite_keys
    # 10  use_fallback_cache
    # 11  kill_at (optional; default -1)
    return [
        "python",
        "pure_kafka_demo.py",
        str(results_dir),
        str(client.client_threads),
        str(cluster.n_partitions),
        str(client.input_rate),
        str(client.total_time),
        str(client.warmup_seconds),
        str(client.n_warehouses),
        cluster.enable_compression,
        cluster.use_composite_keys,
        cluster.use_fallback_cache,
        str(client.kill_at),
    ]


def _start_cluster_and_wait(paths: _Paths, env: dict, cluster: _ClusterParams) -> None:
    rc, out = run_and_stream(
        _start_cmd(paths, cluster),
        cwd=str(paths.repo_root),
        env=env,
        timeout=20 * 60,
        banner="START STYX CLUSTER",
        log=log,
    )
    if rc != 0:
        raise AssertionError(f"start_styx_cluster.sh failed (rc={rc}). Output:\n{out}")

    log.info("Waiting for Kafka external listener (127.0.0.1:9092)...")
    wait_port("127.0.0.1", 9092, timeout_s=180)
    log.info("Kafka port is up.")

    log.info("Waiting for Styx coordinator published port (127.0.0.1:8886)...")
    wait_port("127.0.0.1", 8886, timeout_s=240)
    log.info("Coordinator port is up.")


def _ensure_tpcc_dataset(paths: _Paths, env: dict, *, n_keys: int, regenerate_tpcc_data: bool) -> None:
    """
    Mirrors the bash logic you had for tpcc-generator:
      - dataset at demo/demo-tpc-c/data_{n_keys}
      - regenerate if forced or missing/incorrect
    """
    data_dir = paths.demo_dir / f"data_{n_keys}"
    generator_dir = paths.demo_dir / "tpcc-generator"
    generator_bin = generator_dir / "tpcc-generator"

    if regenerate_tpcc_data:
        log.info("regenerate_tpcc_data is true — forcing data regeneration.")
        regenerate = True
    elif not data_dir.is_dir():
        log.info("Data directory missing: %s", data_dir)
        regenerate = True
    else:
        n_files = sum(1 for p in data_dir.rglob("*") if p.is_file())
        if n_files != 9:
            log.info(
                "Data directory does not contain the exact TPC-C dataset (expected 9 files, got %s): %s",
                n_files,
                data_dir,
            )
            regenerate = True
        else:
            log.info("Skipping data generation: %s already contains the TPC-C dataset.", data_dir)
            regenerate = False

    if not regenerate:
        return

    for cmd, banner in [
        (["make", "clean", "-C", str(generator_dir)], "TPC-C GENERATOR: make clean"),
        (["make", "-C", str(generator_dir)], "TPC-C GENERATOR: make"),
    ]:
        rc, out = run_and_stream(
            cmd,
            cwd=str(paths.repo_root),
            env=env,
            timeout=10 * 60,
            banner=banner,
            log=log,
        )
        if rc != 0:
            raise AssertionError(f"{banner} failed (rc={rc}). Output:\n{out}")

    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    rc, out = run_and_stream(
        [str(generator_bin), str(n_keys), str(data_dir)],
        cwd=str(paths.repo_root),
        env=env,
        timeout=20 * 60,
        banner=f"TPC-C DATASET: generate data_{n_keys}",
        log=log,
    )
    if rc != 0:
        raise AssertionError(f"TPC-C dataset generation failed (rc={rc}). Output:\n{out}")


def _run_client(
    *,
    paths: _Paths,
    env: dict,
    results_dir: Path,
    cluster: _ClusterParams,
    client: _ClientParams,
    timeout_s: int = 20 * 60,
) -> None:
    rc, out = run_and_stream(
        _client_cmd(results_dir, cluster, client),
        cwd=str(paths.demo_dir),
        env=env,
        timeout=timeout_s,
        banner="RUN TPC-C CLIENT",
        log=log,
    )
    if rc != 0:
        raise AssertionError(f"pure_kafka_demo.py failed (rc={rc}). Output:\n{out}")


def _assert_artifacts_and_metrics(results_dir: Path, client: _ClientParams) -> None:
    client_csv = results_dir / "client_requests.csv"
    output_csv = results_dir / "output.csv"
    log.info("Checking artifacts: %s and %s", client_csv, output_csv)

    assert client_csv.exists(), f"Missing artifact: {client_csv}"
    assert output_csv.exists(), f"Missing artifact: {output_csv}"

    _assert_metrics(results_dir=results_dir, input_rate=client.input_rate, client_threads=client.client_threads)


def _stop_cluster(paths: _Paths, env: dict, cluster: _ClusterParams, *, timeout_s: int) -> None:
    rc, out = run_and_stream(
        _stop_cmd(paths, cluster),
        cwd=str(paths.repo_root),
        env=env,
        timeout=timeout_s,
        banner="STOP STYX CLUSTER",
        log=log,
    )
    if rc != 0:
        log.warning("stop_styx_cluster.sh failed (rc=%s). Output:\n%s", rc, out)


@pytest.mark.e2e
def test_styx_e2e_tpcc(tmp_path: Path):
    paths = _resolve_paths()
    results_dir = _make_results_dir(tmp_path)

    cluster = _ClusterParams()
    client = _ClientParams(total_time=10, n_keys=1, n_warehouses=1, regenerate_tpcc_data=True, kill_at=-1)

    env = os.environ.copy()

    try:
        _start_cluster_and_wait(paths, env, cluster)
        _ensure_tpcc_dataset(paths, env, n_keys=client.n_keys, regenerate_tpcc_data=client.regenerate_tpcc_data)
        _run_client(
            paths=paths,
            env=env,
            results_dir=results_dir,
            cluster=cluster,
            client=client,
            timeout_s=20 * 60,
        )
        _assert_artifacts_and_metrics(results_dir, client)
    finally:
        _stop_cluster(paths, env, cluster, timeout_s=10 * 60)


@pytest.mark.e2e
def test_styx_e2e_tpcc_kill_worker_midrun(tmp_path: Path):
    """
    Same scenario/params, but:
      - total_time = 60 seconds
      - kill at second=20 inside pure_kafka_demo.py (proc 0 only)
    """
    paths = _resolve_paths()
    results_dir = _make_results_dir(tmp_path)

    cluster = _ClusterParams()
    client = _ClientParams(total_time=60, n_keys=1, n_warehouses=1, regenerate_tpcc_data=True, kill_at=20)

    env = os.environ.copy()

    try:
        _start_cluster_and_wait(paths, env, cluster)
        _ensure_tpcc_dataset(paths, env, n_keys=client.n_keys, regenerate_tpcc_data=client.regenerate_tpcc_data)
        _run_client(
            paths=paths,
            env=env,
            results_dir=results_dir,
            cluster=cluster,
            client=client,
            timeout_s=30 * 60,  # headroom for recovery
        )
        _assert_artifacts_and_metrics(results_dir, client)
    finally:
        _stop_cluster(paths, env, cluster, timeout_s=15 * 60)
