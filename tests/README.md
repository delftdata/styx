# Styx Test Suite

This directory contains all tests for the Styx project, organized into three tiers: **unit**, **integration**, and **end-to-end (e2e)**. Each tier trades speed for coverage — unit tests run in under a second with no infrastructure, while e2e tests spin up the full stack.

```
tests/
├── e2e/                    # Full-stack end-to-end tests
├── integration/            # Component tests with real containers (Kafka, MinIO)
└── unit/                   # Fast, isolated unit tests
    ├── styx_package/       # Client library & common modules
    ├── coordinator/        # Coordinator service
    └── worker/             # Worker service
```

---

## Unit Tests (`tests/unit/`)

Fast, isolated tests with no external dependencies. All infrastructure is mocked.

| Subfolder | Covers | Examples |
|-----------|--------|----------|
| `styx_package/` | `Operator`, `StateflowGraph`, `StatefulFunction`, serialization, TCP networking, clients | Partitioning logic, message types, state access API |
| `coordinator/` | `Coordinator`, `WorkerPool`, Aria barrier sync, migration metadata, snapshot compaction | Heartbeat detection, recovery state machine, round-robin scheduling |
| `worker/` | `Sequencer`, `InMemoryOperatorState`, Kafka ingress/egress, S3 snapshots, container monitor | Transaction ID assignment, conflict detection, state commit/fallback |

### How to run

```bash
# Install dependencies
pip install -e styx-package/.
pip install -r requirements.txt

# Run all unit tests
pytest tests/unit/ -q

# Run a specific subfolder
pytest tests/unit/worker/ -q
```

**Runtime:** ~1–2 seconds.

---

## Integration Tests (`tests/integration/`)

Component-level tests that use [testcontainers](https://testcontainers-python.readthedocs.io/) to spin up real Kafka (KRaft mode) and MinIO containers. These verify that Styx components interact correctly with real infrastructure — no mocks for Kafka or S3.

| File | What it tests |
|------|---------------|
| `test_async_client_kafka.py` | `AsyncStyxClient` producing messages to Kafka |
| `test_sync_client_kafka.py` | `SyncStyxClient` producing messages to Kafka |
| `test_kafka_ingress.py` | Worker consuming messages from Kafka ingress topics |
| `test_kafka_egress.py` | Worker publishing results to Kafka egress topics |
| `test_s3_snapshots.py` | Snapshot upload/download round-trips to S3 (MinIO) |

Containers are started once per session (via `conftest.py` fixtures) and shared across tests.

### How to run

```bash
# Requires Docker to be running
pip install -e styx-package/.
pip install -r requirements.txt

# Run integration tests
pytest tests/integration/ -m integration -q
```

**Runtime:** ~1–2 minutes (container startup dominates).

---

## End-to-End Tests (`tests/e2e/`)

Full-stack tests that start the entire Styx system (Kafka, S3, coordinator, workers) via `docker compose`, run a benchmark client, and validate the output.

| File | What it tests |
|------|---------------|
| `test_e2e_ycsb.py` | YCSB workload: basic run + worker failure recovery |
| `test_e2e_tpcc.py` | TPC-C benchmark transaction processing |
| `test_e2e_deathstar_hotel_reservation.py` | DeathStar hotel reservation microservice demo |
| `test_e2e_deathstar_movie_review.py` | DeathStar movie review microservice demo |

### How to run

```bash
# 1. Start Kafka
docker compose -f docker-compose-kafka.yml up -d

# 2. Start S3 (RustFS)
docker compose -f docker-compose-s3.yml up -d

# 3. Build Styx images
docker compose build

# 4. Run e2e tests
pytest tests/e2e/ -m e2e -q

# 5. Cleanup
docker compose -f docker-compose-kafka.yml down
docker compose -f docker-compose-s3.yml down
```

**Runtime:** ~10–20 minutes depending on the benchmark.

---

## Running All Tests

```bash
# Unit only (fastest, no Docker needed)
pytest tests/unit/ -q

# Unit + integration (needs Docker)
pytest tests/unit/ tests/integration/ -q

# Everything
pytest tests/ -q
```

## Pytest Markers

Tests are tagged with markers so you can run subsets selectively:

```bash
pytest -m e2e          # Only e2e tests
pytest -m integration  # Only integration tests
pytest -m "not e2e"    # Everything except e2e
```

Markers are defined in `pytest.ini`.

## Key Configuration Notes

- **`asyncio_mode = auto`** — async test functions run automatically via pytest-asyncio.
- **Environment variables** — Many Styx modules read env vars at import time. Test `conftest.py` files set these *before* importing Styx modules, and use `importlib.reload()` when values change after container startup.
- **`helpers.py`** — Provides `run_and_stream()` for streaming subprocess output with timeouts, `wait_port()` for TCP readiness checks, and `make_test_env()` for building env dicts with S3 defaults.
