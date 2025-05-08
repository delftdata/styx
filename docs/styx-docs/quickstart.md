# Quickstart

<div class="under-construction">
  🚧 We are in the process of adding styx-package on PyPi, the dockerfiles on Dockerhub and a 
local runner so that you don't have to deploy a Styx cluster for debugging and streamline the development process. 🚧
</div>

Requirements: 

 - A `Python 3.13` environment
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

Next start a Styx cluster by calling:


```shell
./scripts/start_styx_cluster.sh [scale_factor] [epoch_size]
```

`scale_factor` is how many Styx workers you want deployed and `epoch_size` the size of a transactional epoch in terms of number of transactions.

Now you are ready to submit your first stateful dataflow graph to the Styx cluster for processing!