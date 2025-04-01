import asyncio
import os
import socket
import time
import concurrent.futures
from asyncio import StreamReader, StreamWriter
from struct import unpack

from timeit import default_timer as timer

import uvloop
from minio import Minio
import minio.error

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer
from styx.common.util.aio_task_scheduler import AIOTaskScheduler

from coordinator import Coordinator

SERVER_PORT = 8080
PROTOCOL_PORT = 8889

PROTOCOL = Protocols.Aria


class QueryStateService(object):

    def __init__(self):

        self.total_workers = 4  # Total number of workers
        self.epoch_deltas = {}  # Stores {epoch: {worker_id: delta}}
        self.epoch_count = {}  # Tracks number of deltas received per epoch
        self.state_store = {}  #global state store
        self.state_lock = asyncio.Lock()

        self.networking = NetworkingManager(SERVER_PORT)
        self.protocol_networking = NetworkingManager(PROTOCOL_PORT, size=4, mode=MessagingMode.PROTOCOL_PROTOCOL)
        self.coordinator = Coordinator(self.networking)
        self.aio_task_scheduler = AIOTaskScheduler()
        self.background_tasks = set()
        self.healthy_workers: set[int] = set()
        self.worker_is_healthy: dict[int, asyncio.Event] = {}

        self.puller_task: asyncio.Task = ...

        self.coor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.coor_socket.bind(('0.0.0.0', SERVER_PORT))
        self.coor_socket.setblocking(False)

        self.protocol_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.protocol_socket.bind(('0.0.0.0', SERVER_PORT + 1))
        self.protocol_socket.setblocking(False)

    async def query_state_controller(self, writer: StreamWriter, data):
        """Handles incoming messages related to query state updates."""
        message_type: int = self.networking.get_msg_type(data)

        match message_type:
            case MessageType.QueryMsg:
                # Decode the message
                worker_id, epoch_counter, state_delta = self.networking.decode_message(data)
                logging.info(f"Received state delta from worker {worker_id} for epoch {epoch_counter}")

                await self.receive_delta(worker_id,epoch_counter,state_delta);

            case _:  # Handle unsupported message types
                logging.error(f"QUERY STATE SERVER: Unsupported message type: {message_type}")

    async def receive_delta(self, worker_id, epoch_counter, state_delta):
        """add workerid , epoch counter , state delta to a dictionary
           maintain a [workerid] [epoch] count , and once count reaches the predefined number of workers,
           lock state and merge all deltas and update the state store.
        """

        if epoch_counter not in self.epoch_deltas:
            self.epoch_deltas[epoch_counter]={}
            self.epoch_count[epoch_counter] = 0

        self.epoch_deltas[epoch_counter][worker_id] = state_delta
        self.epoch_count[epoch_counter] += 1

        if self.epoch_count[epoch_counter]==self.total_workers:
            await self.mergeDeltas_and_updateState(epoch_counter)

    async def mergeDeltas_and_updateState(self, epoch_counter):
        async with self.state_lock:
            deltas = self.epoch_deltas[epoch_counter]

            # Merge deltas directly into state_store
            for worker_delta in deltas.values():
                for operator_partition, kv_pairs in worker_delta.items():
                    if operator_partition not in self.state_store:
                        self.state_store[operator_partition] = {}  # Initialize if missing

                    for key, value in kv_pairs.items():
                        self.state_store[operator_partition][key] = (
                                self.state_store[operator_partition].get(key, 0) + value
                        )
            print(f"Epoch {epoch_counter} state updated:{self.state_store}")

            del self.epoch_deltas[epoch_counter]
            del self.epoch_count[epoch_counter]


    async def main(self):
        self.query_state_controller()

if __name__ == "__main__":
    query_state_service = QueryStateService()
    uvloop.run(query_state_service.main())
