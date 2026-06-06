# Quickstart

<div class="under-construction">
  🚧 We are in the process of adding styx-package on PyPi, the dockerfiles on Dockerhub and a 
local runner so that you don't have to deploy a Styx cluster for debugging and streamline the development process. 🚧
</div>

Requirements: 

 - A `Python 3.14` environment
 - `Docker`
 - `Docker Compose`

To start clone the Styx repository:

```shell
git clone https://github.com/delftdata/styx
```

Install the styx-package:

```shell
pip install ./styx-package/
```

The local cluster script starts Kafka, RustFS, the coordinator, and workers with
Docker Compose. It also runs `docker system prune -f --volumes` before starting
the cluster, which removes unused Docker objects and volumes from your Docker
host.

Next start a Styx cluster by calling:


```shell
./scripts/start_styx_cluster.sh [scale_factor] [epoch_size] [threads_per_worker] [enable_compression] [use_composite_keys]
```

`scale_factor` is how many Styx workers you want deployed, `epoch_size` is the size of a transactional epoch in terms of number of transactions, and `threads_per_worker` controls how many threads run inside each worker container.

`enable_compression` controls whether Styx compresses large MessagePack-serialized internal TCP messages with Zstandard. Compression is enabled only for messages larger than the `COMPRESS_AFTER` threshold, which defaults to 4096 bytes. Leave this enabled unless you are explicitly comparing compression overhead.

`use_composite_keys` controls whether operators honor `composite_key_hash_params` during partitioning. When enabled, an operator can route a string key by one selected field, for example the first field in `warehouse:district:customer`; when disabled, Styx hashes the full key string. Leave this enabled for workloads such as TPC-C that define composite-key partitioning.

For example, to start four workers with epochs of 1000 transactions, one worker thread per container, compression enabled, and composite keys enabled:

```shell
./scripts/start_styx_cluster.sh 4 1000 1 true true
```

Now you are ready to submit your first stateful dataflow graph to the Styx cluster for processing!
