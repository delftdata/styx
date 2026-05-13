# Environment Variables

## Styx Coordinator

The following environment variables configure the behavior of the **Styx Coordinator**, including heartbeats, Kafka settings, snapshotting, and object storage.

---

### 🧭 Core Configuration

| Variable                  | Default Value           | Description                                 |
|---------------------------|-------------------------|---------------------------------------------|
| `KAFKA_URL`              | `KAFKA_HOST:KAFKA_PORT` | Kafka bootstrap server for messaging        |
| `HEARTBEAT_LIMIT`        | `5000` (ms)             | Max time before a worker is considered dead |
| `HEARTBEAT_CHECK_INTERVAL` | `500` (ms)              | How often to check worker heartbeats        |
| `MAX_OPERATOR_PARALLELISM` | `10`                    | Max number of operator partitions           |
| `PROTOCOL`               | `Protocols.Aria`        | Transaction execution protocol used by Styx |

---

### 🪣 Snapshot & State

| Variable                           | Default Value         | Description                                  |
|------------------------------------|------------------------|----------------------------------------------|
| `SNAPSHOT_BUCKET_NAME`            | `styx-snapshots`      | S3/MinIO bucket for storing snapshots        |
| `SNAPSHOT_FREQUENCY_SEC`          | `10` (seconds)        | How often to take a snapshot                 |
| `SNAPSHOT_COMPACTION_INTERVAL_SEC`| `10` (seconds)        | Interval for compacting snapshots            |

---

### ⚙️ MinIO / Object Storage

| Variable              | Source         | Description                        |
|-----------------------|----------------|------------------------------------|
| `S3_ENDPOINT`        | Required       | Full URL to connect to S3/MinIO    |
| `S3_ACCESS_KEY`      | Required       | Access key for the S3/MinIO user   |
| `S3_SECRET_KEY`      | Required       | Secret key for the S3/MinIO user   |
| `S3_REGION`          | `us-east-1`    | S3 region for the client           |
| `S3_INIT_RETRY_SEC`  | `2` (seconds)  | Sleep time between bucket init retries |
| `S3_INIT_MAX_RETRIES`| `30`           | Max retry attempts before coordinator exits (0 = infinite) |

---

### 🛡️ Fault Tolerance & Restart

| Variable                        | Default Value | Description                                                                                          |
|----------------------------------|----------------|------------------------------------------------------------------------------------------------------|
| `MAX_WAIT_FOR_RESTARTS_SEC`    | `0` (seconds)  | How long to wait for the failed container(s) to restart before Styx initiates the automatic recovery |

---
## Styx Worker

These environment variables configure the **Styx Worker**, including discovery, parallelism, heartbeat, snapshotting, and conflict resolution.

---

### 🧭 Discovery & Coordination

| Variable             | Required / Default | Description                          |
|----------------------|--------------------|--------------------------------------|
| `DISCOVERY_HOST`     | Required            | Hostname or IP of the Coordinator    |
| `DISCOVERY_PORT`     | Required            | Port used to communicate with Coordinator |

---

### ⚙️ Kafka & Heartbeat

| Variable           | Default Value | Description                                |
|--------------------|----------------|--------------------------------------------|
| `KAFKA_URL`        | Required       | Kafka broker address                        |
| `HEARTBEAT_INTERVAL` | `500` (ms)   | Frequency at which the worker sends heartbeats |

---

### 🧵 Parallelism & Threads

| Variable            | Default Value | Description                                                  |
|---------------------|----------------|--------------------------------------------------------------|
| `WORKER_THREADS` (`N_THREADS`) | `1`           | Number of Styx workers within the container |
| `SNAPSHOTTING_THREADS` | `4`       | Threads dedicated to snapshotting                            |

---

### 🪣 Snapshotting

| Variable              | Default Value     | Description                                 |
|------------------------|--------------------|---------------------------------------------|
| `SNAPSHOT_BUCKET_NAME` | `styx-snapshots`  | Bucket where snapshots are stored           |
| `SNAPSHOT_FREQUENCY`  | `10` (seconds)     | Snapshot frequency in epochs                |

---

### 📦 Object Storage (MinIO)

| Variable            | Source         | Description                        |
|---------------------|----------------|------------------------------------|
| `MINIO_URL`         | `MINIO_HOST:MINIO_PORT` | Address of the MinIO server     |
| `MINIO_ACCESS_KEY`  | Required        | MinIO access key                  |
| `MINIO_SECRET_KEY`  | Required        | MinIO secret key                  |

---

### 📐 Conflict Detection & Strategy

| Variable                     | Default Value | Description                                                                |
|------------------------------|----------------|----------------------------------------------------------------------------|
| `CONFLICT_DETECTION_METHOD` | `0`            | Styx's conflict detection strategy                                         |
| `FALLBACK_STRATEGY_PERCENTAGE` | `-0.1`       | % aborts before fallback logic triggers (negative enables it at all times) |

---

### ⏱️ Epoch & Sequence Control

| Variable             | Default Value     | Description                                       |
|----------------------|--------------------|---------------------------------------------------|
| `EPOCH_INTERVAL_MS`  | `1` (ms)          | Kafka polling rate                                |
| `SEQUENCE_MAX_SIZE`  | `1000`            | Max size of a transactional epoch per Styx worker |

---
