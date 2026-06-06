# Environment Variables

This page lists the environment variables read by Styx runtime components. Many values are read when Python modules are imported, so set them before starting the coordinator, workers, clients, or tests.

Boolean values are parsed with Python's `strtobool`, so values such as `true`, `false`, `1`, `0`, `yes`, and `no` are accepted.

Styx supports S3-compatible object storages for each snapshots. The local Docker Compose setup uses
RustFS, but any compatible endpoint can be used with the `S3_*` variables.

## Required Runtime Config

These variables are required for a usual Kafka-backed Styx deployment.

| Variable | Component | Default | Description |
|----------|-----------|---------|-------------|
| `KAFKA_URL` | Coordinator, worker | Required | Kafka bootstrap server used for metadata, ingress, and egress topics. |
| `S3_ENDPOINT` | Coordinator, worker | Required | Full URL for the S3-compatible object store. |
| `S3_ACCESS_KEY` | Coordinator, worker | Required | Access key for the S3-compatible object store. |
| `S3_SECRET_KEY` | Coordinator, worker | Required | Secret key for the S3-compatible object store. |
| `DISCOVERY_HOST` | Worker | Required | Coordinator hostname or IP used by workers. |
| `DISCOVERY_PORT` | Worker | Required | Coordinator control-plane port used by workers. |

## Common Execution Config

These variables affect shared Styx package behavior and should be kept consistent across the processes that participate in the same cluster.

| Variable | Default | Description |
|----------|---------|-------------|
| `ENABLE_COMPRESSION` | `true` | Compress MessagePack-serialized internal TCP messages larger than `COMPRESS_AFTER` with Zstandard. |
| `COMPRESS_AFTER` | `4096` | Serialized message size, in bytes, above which `ENABLE_COMPRESSION` can apply. |
| `USE_COMPOSITE_KEYS` | `true` | Honor operator `composite_key_hash_params` during partitioning. When disabled, Styx hashes the full key. |
| `SNAPSHOT_BUCKET_NAME` | `styx-snapshots` | Bucket used for worker, coordinator, and compactor snapshots. |
| `S3_REGION` | `us-east-1` | Region passed to the S3 client. |

## Coordinator Config

These variables configure the coordinator process.

### Kafka And Heartbeats

| Variable | Default | Description |
|----------|---------|-------------|
| `HEARTBEAT_LIMIT` | `5000` | Time in milliseconds before a worker is considered unhealthy. |
| `HEARTBEAT_CHECK_INTERVAL` | `1000` | Time in milliseconds between coordinator heartbeat checks. |
| `KAFKA_REPLICATION_FACTOR` | `3` | Replication factor used when the coordinator creates Styx Kafka topics. |

### Snapshots And Object Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `SNAPSHOT_FREQUENCY_SEC` | `30` | Time in seconds between coordinator snapshot attempts. |
| `COMPACT_SNAPSHOTS` | `false` | Whether the coordinator should trigger snapshot compaction after a new completed snapshot. |
| `S3_INIT_RETRY_SEC` | `2` | Time in seconds to wait between object-store initialization retries. |
| `S3_INIT_MAX_RETRIES` | `30` | Maximum object-store initialization attempts before exiting. Set to `0` to retry forever. |

### Recovery

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_WAIT_FOR_RESTARTS_SEC` | `0` | Time in seconds to wait for failed workers to restart before recovery begins. |

## Worker Config

These variables configure worker processes and the Aria execution protocol.

### Discovery, Ingress, And Heartbeats

| Variable | Default | Description |
|----------|---------|-------------|
| `INGRESS_TYPE` | None | Ingress backend. Set to `KAFKA` for Kafka topic assignment. The local Docker Compose deployment already sets this for workers. |
| `HEARTBEAT_INTERVAL` | `500` | Time in milliseconds between worker heartbeats. |
| `WORKER_THREADS` | `1` | Number of Styx worker processes started inside a worker container. |

### Epochs, Kafka, And Egress

| Variable | Default | Description |
|----------|---------|-------------|
| `SEQUENCE_MAX_SIZE` | `1000` | Maximum number of transactions sequenced in one Aria epoch. |
| `EPOCH_INTERVAL_MS` | `1` | Kafka egress polling interval in milliseconds while draining outputs during recovery. |

### Conflict Detection And Fallback

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFLICT_DETECTION_METHOD` | `0` | Aria conflict detection mode: `0` serializable, `1` deterministic reordering, `2` snapshot isolation. |
| `FALLBACK_STRATEGY_PERCENTAGE` | `-0.1` | Abort-rate threshold for running fallback. The default negative value enables fallback whenever an epoch has aborts. |

### Snapshotting And Migration

| Variable | Default | Description |
|----------|---------|-------------|
| `SNAPSHOTTING_THREADS` | `4` | Number of threads used by the Aria snapshotting executor. |
| `MIGRATION_THREADS` | `4` | Number of processes used for worker-side migration/repartitioning work. |
| `USE_ASYNC_MIGRATION` | `true` | Whether workers send migrating state asynchronously while the protocol is running after migration restart. |
| `ASYNC_MIGRATION_BATCH_SIZE` | `2000` | Maximum number of state items included in each async migration batch. |

## Advanced Networking And Queues

These variables tune socket buffers, socket pooling, and worker queue backpressure. They are primarily useful when profiling high-throughput deployments.

| Variable | Default | Description |
|----------|---------|-------------|
| `SOCKET_SND_BUF` | `4194304` | TCP send buffer size in bytes for Styx sockets. |
| `SOCKET_RCV_BUF` | `4194304` | TCP receive buffer size in bytes for Styx sockets. |
| `SOCKET_POOL_SIZE` | `16` | Number of pooled TCP connections per target `(host, port)` in the Styx networking manager. |
| `PROTOCOL_QUEUE_SIZE` | `10000` | Maximum number of queued protocol-plane messages per worker. |
| `CONTROL_QUEUE_SIZE` | `10000` | Maximum number of queued control-plane messages per worker. |
| `PROTOCOL_WORKERS` | `100` | Number of concurrent protocol queue workers handling protocol-plane messages. |

## Notes

`Protocols.Aria` is currently hardcoded and is not configured through an
environment variable.
