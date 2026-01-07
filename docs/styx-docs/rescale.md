<div class="under-construction">
  🚧 This is part of a future release.<br>Stay tuned for updates! 🚧
</div>

Since Styx is a serverless system it needs to scale up or down to match the load at any moment in time.

The way to do that in Styx is the following, just simply resubmit the graph with a new partitioning:

```python
from styx.common.stateflow_graph import StateflowGraph
from styx.common.operator import Operator
from styx.common.local_state_backends import LocalStateBackend
from styx.client.sync_client import SyncStyxClient

styx_client = SyncStyxClient(styx_coordinator_adr="0.0.0.0", styx_coordinator_port="8080", kafka_url="KAFKA_URL")

# Lets assume that this has been already submitted previously
submitted_operator = Operator(name="example")

new_number_of_partitions: int = 2
submitted_operator.set_n_partitions(new_number_of_partitions)
updated_graph = StateflowGraph(name="example", operator_state_backend=LocalStateBackend.DICT)
updated_graph.add_operator(submitted_operator)

styx_client.submit_dataflow(updated_graph)
```

In the future we plan to add an autoscaler to Styx's coordinator to automate this process.