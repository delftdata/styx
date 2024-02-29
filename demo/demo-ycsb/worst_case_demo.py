import asyncio
import time

import uvloop
import pandas as pd

from styx.common.local_state_backends import LocalStateBackend
from styx.common.stateflow_graph import StateflowGraph
from styx.client import AsyncStyxClient

from ycsb import ycsb_operator

N_ROWS = 1000

STYX_HOST: str = 'localhost'
STYX_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


keys: list[int] = [0]


async def main():
    styx = AsyncStyxClient(STYX_HOST, STYX_PORT,
                           kafka_url=KAFKA_URL)
    await styx.open()
    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    g = StateflowGraph('ycsb-benchmark', operator_state_backend=LocalStateBackend.DICT)
    ####################################################################################################################
    g.add_operators(ycsb_operator)
    await styx.submit_dataflow(g)

    print('Graph submitted')

    timestamped_request_ids = {}

    time.sleep(1)

    # INSERT
    tasks = []
    for i in keys:
        tasks.append(styx.send_event(operator=ycsb_operator,
                                     key=i,
                                     function='insert'))
    await asyncio.gather(*tasks)
    await styx.flush()

    time.sleep(1)

    tasks = []
    for _ in range(N_ROWS):
        tasks.append(styx.send_event(ycsb_operator, keys[0], 'update'))
    responses = await asyncio.gather(*tasks)
    for request_id in responses:
        timestamped_request_ids[request_id] = {}

    await styx.close()

    for key, metadata in styx.delivery_timestamps.items():
        if key in timestamped_request_ids:
            timestamped_request_ids[key]["timestamp"] = metadata

    pd.DataFrame({"request_id": list(timestamped_request_ids.keys()),
                  "timestamp": [res["timestamp"] for res in timestamped_request_ids.values()]
                  }).sort_values("timestamp").to_csv('client_requests.csv', index=False)

if __name__ == "__main__":
    uvloop.run(main())
