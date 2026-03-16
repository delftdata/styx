# Running Experiments

This guide covers how to run Styx benchmark experiments in all supported deployment modes.

---

## Deployment Modes

| Mode | Description |
|------|-------------|
| `docker-compose` | Local cluster using Docker Compose. Default for migration/scalability scripts. |
| `k8s-minikube` | Kubernetes via minikube. Builds and loads images locally. |
| `k8s-cluster` | Generic Kubernetes cluster (e.g. on-prem or cloud). Uses pre-published images. |

The mode is controlled by the `DEPLOY_MODE` environment variable.

---

## Prerequisites

### All modes

- Python environment with dependencies installed:
  ```bash
  pip install styx-package/.
  pip install -r requirements.txt
  ```

### docker-compose mode

| Tool | Purpose |
|------|---------|
| Docker + Docker Compose | Running the full Styx stack locally |

### k8s-minikube mode

| Tool | Purpose |
|------|---------|
| [minikube](https://minikube.sigs.k8s.io/) | Local Kubernetes cluster |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | Cluster management |
| [helm](https://helm.sh/docs/intro/install/) | Chart deployment |
| [kubefwd](https://kubefwd.com/) | Service forwarding with DNS (see below) |
| Docker | Building local images |

### k8s-cluster mode

| Tool | Purpose |
|------|---------|
| kubectl | Configured for your target cluster (`~/.kube/config`) |
| helm | Chart deployment |
| [kubefwd](https://kubefwd.com/) | Service forwarding with DNS (see below) |

---

## Setting up kubefwd (k8s modes only)

`kubefwd` forwards all services in the Kubernetes namespace to your local machine and
adds `/etc/hosts` entries for service names and pod FQDNs. This is required because
Kafka's advertised-listener redirect mechanism makes plain `kubectl port-forward`
unusable: the broker returns its internal cluster DNS name in metadata responses,
which the host cannot resolve. `kubefwd` solves this transparently.

`kubefwd` must run as root because it writes to `/etc/hosts`. To avoid password
prompts during automated batch runs, grant passwordless sudo access scoped to the
`kubefwd` binary.

### Install kubefwd

```bash
# Download the latest release for your platform from https://github.com/txn2/kubefwd/releases
# Example for Linux amd64:
curl -Lo kubefwd.tar.gz https://github.com/txn2/kubefwd/releases/latest/download/kubefwd_linux_x86_64.tar.gz
tar -xzf kubefwd.tar.gz
sudo mv kubefwd /usr/local/bin/kubefwd
```

### Add sudoers rule (no-password)

```bash
sudo visudo -f /etc/sudoers.d/kubefwd
```

Add the following line, replacing `youruser` with your Linux username:

```
youruser ALL=(ALL) NOPASSWD:SETENV: /usr/local/bin/kubefwd
```

Verify:

```bash
sudo kubefwd --version
```

---

## Running a Single Experiment

**Script:** `scripts/run_experiment.sh`

### Usage

```bash
[DEPLOY_MODE=...] ./scripts/run_experiment.sh \
  <workload_name> <input_rate> <n_keys> <n_part> <zipf_const> \
  <client_threads> <total_time> <saving_dir> <warmup_seconds> <epoch_size> \
  [styx_threads_per_worker] [enable_compression] [use_composite_keys] \
  [use_fallback_cache] [regenerate_tpcc_data]
```

### Parameters

| Position | Parameter | Description |
|----------|-----------|-------------|
| 1 | `workload_name` | `ycsbt`, `dhr`, `dmr`, or `tpcc` |
| 2 | `input_rate` | Transactions per second per client thread |
| 3 | `n_keys` | Number of distinct keys (ignored for `dhr`/`dmr`) |
| 4 | `n_part` | Number of Styx partitions (= number of workers in docker-compose) |
| 5 | `zipf_const` | Zipf skew constant (`0.0` = uniform) |
| 6 | `client_threads` | Number of parallel client sender threads |
| 7 | `total_time` | Total benchmark duration in seconds |
| 8 | `saving_dir` | Directory where result CSVs are written |
| 9 | `warmup_seconds` | Seconds excluded from metrics |
| 10 | `epoch_size` | Max transactions per Aria epoch (e.g. `1000`) |
| 11 _(optional)_ | `styx_threads_per_worker` | Worker threads per container (default: `1`) |
| 12 _(optional)_ | `enable_compression` | `true`/`false` — ZSTD snapshot compression (default: `true`) |
| 13 _(optional)_ | `use_composite_keys` | `true`/`false` — composite key hashing (default: `true`) |
| 14 _(optional)_ | `use_fallback_cache` | `true`/`false` — fallback result cache (default: `true`) |
| 15 _(optional)_ | `regenerate_tpcc_data` | `true`/`false` — force TPC-C data regeneration (default: `false`) |

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DEPLOY_MODE` | `k8s-minikube` | Deployment mode: `docker-compose`, `k8s-minikube`, `k8s-cluster` |
| `RELEASE_NAME` | `styx-cluster` | Helm release name (k8s modes) |
| `NAMESPACE` | `styx` | Kubernetes namespace (k8s modes) |

### Examples

```bash
# docker-compose — YCSB-T, uniform, 1000 tx/s, 4 partitions, 60s run
DEPLOY_MODE=docker-compose ./scripts/run_experiment.sh \
  ycsbt 1000 1000000 4 0.0 1 60 results/ 10 1000

# k8s-minikube — TPC-C, 1 warehouse, 4 partitions
DEPLOY_MODE=k8s-minikube ./scripts/run_experiment.sh \
  tpcc 500 1 4 0.0 1 60 results/ 10 100

# k8s-cluster — Deathstar Hotel Reservation
DEPLOY_MODE=k8s-cluster RELEASE_NAME=styx NAMESPACE=styx \
  ./scripts/run_experiment.sh dhr 1000 -1 4 0.0 2 120 results/ 20 1000
```

### Workload notes

- **`ycsbt`**: YCSB-T. Set `zipf_const=0.0` for uniform, `>0` for Zipf skew.
- **`dhr`**: Deathstar Hotel Reservation. `n_keys` is ignored (pass any value, e.g. `-1`).
- **`dmr`**: Deathstar Movie Review. `n_keys` is ignored.
- **`tpcc`**: TPC-C. `n_keys` = number of warehouses. Dataset is auto-generated on first run under `demo/demo-tpc-c/data_<n_keys>/`; pass `regenerate_tpcc_data=true` to force rebuild.

---

## Running a Batch of Experiments

**Script:** `scripts/run_batch_experiments.sh`

Generates a CSV config with `scripts/create_config.py` then runs each row via `run_experiment.sh`.

### Usage

```bash
[DEPLOY_MODE=...] ./scripts/run_batch_experiments.sh \
  <config_csv> <saving_dir> <styx_threads_per_worker> \
  <partitions> <n_keys> <experiment_time> <warmup_time> \
  <scenarios...>
```

### Parameters

| Position | Parameter | Description |
|----------|-----------|-------------|
| 1 | `config_csv` | Path to the generated CSV (written by `create_config.py`) |
| 2 | `saving_dir` | Directory for result CSVs |
| 3 | `styx_threads_per_worker` | Worker threads per container |
| 4 | `partitions` | Number of Styx partitions |
| 5 | `n_keys` | Number of keys (for YCSB-T / TPC-C) |
| 6 | `experiment_time` | Duration per experiment (seconds) |
| 7 | `warmup_time` | Warmup per experiment (seconds) |
| 8+ | `scenarios` | Space-separated subset: `ycsbt_uni ycsbt_zipf dhr dmr tpcc` |

### Environment variables

Same as `run_experiment.sh` — set `DEPLOY_MODE`, `RELEASE_NAME`, `NAMESPACE` before calling the script.

### Example

```bash
mkdir -p results
DEPLOY_MODE=docker-compose ./scripts/run_batch_experiments.sh \
  scripts/styx_experiments_config.csv results/ 1 4 1000000 60 10 \
  ycsbt_uni tpcc
```

---

## Running Migration Experiments

**Script:** `scripts/run_migration_experiment.sh`

> **docker-compose only** — k8s migration mode is not yet supported via this script.

Runs a benchmark in which the number of partitions is scaled (up or down) mid-experiment.

### Usage

```bash
./scripts/run_migration_experiment.sh \
  <input_rate> <start_n_part> <end_n_part> <client_threads> \
  <total_time> <saving_dir> <warmup_seconds> <epoch_size> \
  <workload_name> <n_keys> \
  [regenerate_tpcc_data] [styx_threads_per_worker] \
  [enable_compression] [use_composite_keys] [use_fallback_cache]
```

### Workloads

| `workload_name` | Description |
|----------------|-------------|
| `ycsb` | YCSB-T migration |
| `tpcc` | TPC-C migration |

Migration is triggered at second 60 of the run. The coordinator stops Aria, repartitions state in parallel, and resumes with the new partition count.

### Example

```bash
# Scale from 4 → 8 partitions during a YCSB-T run
./scripts/run_migration_experiment.sh \
  1000 4 8 1 120 results/ 10 1000 ycsb 1000000
```

---

## Running Scalability Experiments

**Script:** `scripts/run_scalability_experiments.sh`

> **docker-compose only.**

Sweeps over partition counts and input rates defined in `scripts/create_scalability_config.py`.

### Usage

```bash
./scripts/run_scalability_experiments.sh \
  <config_csv> <saving_dir> <styx_threads_per_worker>
```

---

## Helm Cluster Management (k8s modes)

These scripts are called automatically by `run_experiment.sh` in k8s modes, but can also be used standalone.

| Script | Description |
|--------|-------------|
| `scripts/install_styx_cluster_with_helm.sh` | Install or upgrade the Helm release. Uses `dev_values.yaml` for minikube, `values.yaml` for k8s-cluster. Also builds and loads images for minikube. |
| `scripts/update_styx_cluster_with_helm.sh` | Re-deploy after code or values changes. |
| `scripts/uninstall_styx_cluster_with_helm.sh` | Uninstall the release and (by default) delete the namespace. Removes minikube images in k8s-minikube mode. |

All three read `DEPLOY_MODE`, `RELEASE_NAME`, and `NAMESPACE` from the environment.

```bash
# Example: manual install/uninstall
export DEPLOY_MODE=k8s-minikube
export RELEASE_NAME=styx-cluster
export NAMESPACE=styx

./scripts/install_styx_cluster_with_helm.sh
# ... run experiments ...
./scripts/uninstall_styx_cluster_with_helm.sh
```

---

## Results

Each experiment writes two CSV files to `<saving_dir>`:

| File | Contents |
|------|----------|
| `client_requests.csv` | Per-request timestamps and operation labels |
| `kafka_output.csv` | Kafka egress records with commit timestamps |

Metrics (throughput, latency percentiles) are computed and printed to stdout at the end of each client run.
