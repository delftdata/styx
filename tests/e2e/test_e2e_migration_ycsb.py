from dataclasses import dataclass
import json
import logging
from pathlib import Path

import pytest

from tests.helpers import make_test_env, run_and_stream, wait_port

log = logging.getLogger("e2e.migration_ycsb")


def _assert_metrics(
    results_dir: Path,
    input_rate: int,
    client_threads: int,
) -> None:
    exp_name = f"ycsb_migration_{input_rate * client_threads}"
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
    epoch_size: int = 1000
    threads_per_worker: int = 1
    enable_compression: str = "true"
    use_composite_keys: str = "true"
    use_fallback_cache: str = "true"


@dataclass(frozen=True)
class _ClientParams:
    client_threads: int = 2
    n_entities: int = 10_000
    start_n_partitions: int = 4
    end_n_partitions: int = 8
    input_rate: int = 200
    total_time: int = 120
    warmup_seconds: int = 1


@dataclass(frozen=True)
class _Paths:
    repo_root: Path
    demo_dir: Path
    start_script: Path
    stop_script: Path


def _resolve_paths() -> _Paths:
    repo_root = Path(__file__).resolve().parents[2]
    demo_dir = repo_root / "demo" / "demo-migration-ycsb"
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
        str(p.threads_per_worker),
        p.enable_compression,
        p.use_composite_keys,
        p.use_fallback_cache,
    ]


def _stop_cmd(paths: _Paths, p: _ClusterParams) -> list[str]:
    return ["bash", str(paths.stop_script), str(p.threads_per_worker)]


def _client_cmd(results_dir: Path, client: _ClientParams) -> list[str]:
    # client.py expects:
    #  1  threads
    #  2  START_N_PARTITIONS
    #  3  END_N_PARTITIONS
    #  4  messages_per_second
    #  5  seconds
    #  6  SAVE_DIR
    #  7  warmup_seconds
    #  8  N_ENTITIES
    return [
        "python",
        "client.py",
        str(client.client_threads),
        str(client.start_n_partitions),
        str(client.end_n_partitions),
        str(client.input_rate),
        str(client.total_time),
        str(results_dir),
        str(client.warmup_seconds),
        str(client.n_entities),
        "../load_profiles/constant.yaml",
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


def _run_client(
    *,
    paths: _Paths,
    env: dict,
    results_dir: Path,
    client: _ClientParams,
    timeout_s: int = 30 * 60,
) -> None:
    rc, out = run_and_stream(
        _client_cmd(results_dir, client),
        cwd=str(paths.demo_dir),
        env=env,
        timeout=timeout_s,
        banner="RUN MIGRATION YCSB CLIENT",
        log=log,
    )
    if rc != 0:
        raise AssertionError(f"client.py failed (rc={rc}). Output:\n{out}")


def _assert_artifacts_and_metrics(results_dir: Path, client: _ClientParams) -> None:
    client_csv = results_dir / "client_requests.csv"
    output_csv = results_dir / "output.csv"
    log.info("Checking artifacts: %s and %s", client_csv, output_csv)

    assert client_csv.exists(), f"Missing artifact: {client_csv}"
    assert output_csv.exists(), f"Missing artifact: {output_csv}"

    _assert_metrics(results_dir, client.input_rate, client.client_threads)


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
def test_styx_e2e_migration_ycsb(tmp_path: Path):
    paths = _resolve_paths()
    results_dir = _make_results_dir(tmp_path)

    cluster = _ClusterParams()
    client = _ClientParams(total_time=120, start_n_partitions=8, end_n_partitions=4)

    env = make_test_env()

    try:
        _start_cluster_and_wait(paths, env, cluster)
        _run_client(
            paths=paths,
            env=env,
            results_dir=results_dir,
            client=client,
            timeout_s=30 * 60,
        )
        _assert_artifacts_and_metrics(results_dir, client)
    finally:
        _stop_cluster(paths, env, cluster, timeout_s=10 * 60)
