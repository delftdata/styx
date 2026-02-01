from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import shutil
from typing import TYPE_CHECKING

import pytest

from tests.helpers import run_and_stream, run_and_stream_with_timed_action, wait_port

if TYPE_CHECKING:
    from collections.abc import Sequence

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
    # Mirrors:
    # python demo/demo-tpc-c/pure_kafka_demo.py \
    #   saving_dir client_threads n_part input_rate total_time warmup_seconds \
    #   n_keys enable_compression use_composite_keys use_fallback_cache
    return [
        "python",
        "pure_kafka_demo.py",
        str(results_dir),
        str(client.client_threads),
        str(cluster.n_partitions),
        str(client.input_rate),
        str(client.total_time),
        str(client.warmup_seconds),
        str(client.n_keys),
        cluster.enable_compression,
        cluster.use_composite_keys,
        cluster.use_fallback_cache,
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
    Mirrors the bash logic:

      DATA_DIR="demo/demo-tpc-c/data_${n_keys}"
      GENERATOR_DIR="demo/demo-tpc-c/tpcc-generator"
      GENERATOR_BIN="$GENERATOR_DIR/tpcc-generator"

      regenerate if:
        - regenerate_tpcc_data is true, OR
        - DATA_DIR missing, OR
        - DATA_DIR does not contain exactly 9 files

      if regenerate:
        make clean -C GENERATOR_DIR
        make -C GENERATOR_DIR
        rm -rf DATA_DIR
        mkdir -p DATA_DIR
        GENERATOR_BIN n_keys DATA_DIR
    """
    data_dir = paths.demo_dir / f"data_{n_keys}"
    generator_dir = paths.demo_dir / "tpcc-generator"
    generator_bin = generator_dir / "tpcc-generator"

    # Decide whether to regenerate.
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

    # Build generator
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

    # Recreate data dir
    if data_dir.exists():
        # fast/portable removal: use python to remove tree rather than shell rm -rf
        # (keeps it OS-independent inside CI containers)
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Generate
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
    timed_action_at_s: float | None = None,
    timed_action_cmd: Sequence[str] | None = None,
    timed_action_banner: str | None = None,
    timeout_s: int = 20 * 60,
) -> None:
    cmd = _client_cmd(results_dir, cluster, client)

    if timed_action_at_s is None:
        rc, out = run_and_stream(
            cmd,
            cwd=str(paths.demo_dir),
            env=env,
            timeout=timeout_s,
            banner="RUN TPC-C CLIENT",
            log=log,
        )
    else:
        if timed_action_cmd is None:
            msg = "timed_action_cmd must be provided when timed_action_at_s is set"
            raise ValueError(msg)
        rc, out = run_and_stream_with_timed_action(
            cmd,
            cwd=str(paths.demo_dir),
            env=env,
            timeout=timeout_s,
            banner="RUN TPC-C CLIENT (TIMED ACTION)",
            action_at_s=float(timed_action_at_s),
            action_cmd=list(timed_action_cmd),
            action_banner=timed_action_banner or "timed action",
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
    client = _ClientParams(total_time=10, n_keys=1, regenerate_tpcc_data=True)

    env = os.environ.copy()

    try:
        _start_cluster_and_wait(paths, env, cluster)
        _ensure_tpcc_dataset(
            paths,
            env,
            n_keys=client.n_keys,
            regenerate_tpcc_data=client.regenerate_tpcc_data,
        )
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
      - at t=20s after client start: docker kill styx-worker-1
    """
    paths = _resolve_paths()
    results_dir = _make_results_dir(tmp_path)

    cluster = _ClusterParams()
    client = _ClientParams(total_time=60, n_keys=1, regenerate_tpcc_data=True)

    env = os.environ.copy()

    try:
        _start_cluster_and_wait(paths, env, cluster)
        _ensure_tpcc_dataset(
            paths,
            env,
            n_keys=client.n_keys,
            regenerate_tpcc_data=client.regenerate_tpcc_data,
        )

        _run_client(
            paths=paths,
            env=env,
            results_dir=results_dir,
            cluster=cluster,
            client=client,
            timed_action_at_s=105,
            timed_action_cmd=["docker", "kill", "styx-worker-1"],
            timed_action_banner="docker kill styx-worker-1",
            timeout_s=30 * 60,  # headroom for recovery
        )
        _assert_artifacts_and_metrics(results_dir, client)
    finally:
        _stop_cluster(paths, env, cluster, timeout_s=15 * 60)
