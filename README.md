# Styx: Transactional Stateful Functions on Streaming Dataflows

This repository contains the codebase of Styx described in: https://arxiv.org/abs/2312.06893.

## Preliminaries

This project requires an environment with *python 3.12* installed. 
Please install the styx-package and all the requirements of the coordinator
and the worker modules as well as pandas, numpy and matplotlib. 

You can use the following commands:

```
pip install styx-package/.  
pip install -r coordinator/requirements.txt
pip install -r worker/requirements.txt
pip install pandas numpy matplotlib
```

## Folder structure

*   [`coordinator`](https://github.com/delftdata/styx/tree/main/coordinator) 
    Styx coordinator.

*   [`demo`](https://github.com/delftdata/styx/tree/main/benchmark) 
    The YCSB-T, Deathstar, TPC-C and scalability benchmarks we used for the experiments.

*   [`env`](https://github.com/delftdata/styx/tree/main/env)
    env folder for the docker-compose Minio container.

*   [`grafana`](https://github.com/delftdata/styx/tree/main/grafana)
    The confinguration files for the deployment of our visualization dashboards.

*   [`styx-package`](https://github.com/delftdata/styx/tree/main/styx-package)
    The Styx framework Python package.

*   [`tests`](https://github.com/delftdata/styx/tree/main/tests)
    Tests for the worker components of Styx.

*   [`worker`](https://github.com/delftdata/styx/tree/main/styx-package)
    Styx worker.

## Running experiments

In the scripts directory, we provide a number of different scripts that can be used to run the experiments of Styx.

### Reproduce paper results

First, you have to generate the experiment config files by running `python create_config.py` and `python create_scalability_config.py` then:

From the project's root:
```
./scripts/run_batch_experiments.sh [config file path] [results path]
./scripts/run_scalability_experiments.sh [config file path] [results path]
```

### Run single experiment

To run a single experiment:

```
./scripts/run_experiment.sh [WORKLOAD_NAME] [INPUT_RATE] [N_KEYS] [N_PART] [ZIPF_CONST] [CLIENT_THREADS] [TOTAL_TIME] [SAVING_DIR] [WARMUP_SECONDS] [EPOCH_SIZE]
```

e.g. to run the YCSB-T workload with 1000000 keys at 1000 TPS, 4 partitions, 
0.0 zipfian coefficient, 1 client thread, for 60 seconds with 10 second warmup time,
a batch size of 1000 and save the results in the results folder: `./scripts/run_experiment.sh ycsbt 1000 1000000 4 0.0 1 60 results 10 1000`

The options for `[WORKLOAD_NAME]` are `ycsbt` for YCSB-T, `dhr` for deathstar hotel reservation,
`dmr` for deathstar movie review and `tpcc` for  TPC-C. `[ZIPF_CONST]` only affects the `ycsbt` workload.


> Note: If you want to change the number of CPUs per worker you have to go to the docker-compose.yml and change the WORKER_THREADS
> value + the resources along with the /scripts/start_styx_cluster.sh $threads_per_worker. In the paper experiments we used 8.

## Alternative way of execution

**Alternatively**, you can also handle the individual components of Styx as follows. First, you need to deploy 
the Kafka cluster and the MinIO storage. And use any of the clients in the `/demo` folder.

### Kafka

To run kafka: `docker compose -f docker-compose-kafka.yml up`

To clear kafka: `docker compose -f docker-compose-kafka.yml down --volumes`

---

### MinIO

To run MinIO: `docker-compose up -f docker-compose-minio.yml up`

To clear MinIO: `docker-compose -f docker-compose-minio.yml down --volumes`

---
  
Then, you can start the Styx engine and specify the desired scale.

### Styx Engine

To run the SE: `docker-compose up --build --scale worker=4`

To clear the SE: `docker-compose down --volumes`

##### Cite Styx

```bibtex
@inproceedings{psarakis2025styx,
author = {Psarakis, Kyriakos and Christodoulou, George and Siachamis, George and Fragkoulis, Marios and Katsifodimos, Asterios},
title = {Styx: Transactional Stateful Functions on Streaming Dataflows},
year = {2025},
publisher = {Association for Computing Machinery},
booktitle = {Proceedings of the 2025 International Conference on Management of Data},
series = {SIGMOD '25}
}
```

