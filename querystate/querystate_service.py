import asyncio
import socket
import uuid
import os

from asyncio import StreamReader, StreamWriter


import uvloop
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import UnknownTopicOrPartitionError, KafkaConnectionError
from styx.common import networking

from styx.common.logging import logging
from styx.common.message_types import MessageType
from styx.common.tcp_networking import NetworkingManager, MessagingMode
from styx.common.protocols import Protocols
from styx.common.serialization import Serializer
from styx.common.util.aio_task_scheduler import AIOTaskScheduler
from struct import unpack


SERVER_PORT = 8080

PROTOCOL = Protocols.Aria
KAFKA_URL: str = os.getenv('KAFKA_URL', None)
KAFKA_CONSUME_TIMEOUT_MS = 10 # ms
KAFKA_QUERY_RESPONSE_TOPIC="query_state_response"


class QueryStateService(object):

    def __init__(self):

        self.total_workers = 4  # Total number of workers
        self.epoch_deltas = {}  # Stores {epoch: {worker_id: delta}}
        self.epoch_count = {}  # Tracks number of deltas received per epoch
        self.state_store = {}  #global state store
        self.latest_epoch_count = 0
        self.state_lock = asyncio.Lock()

        self.server_port = SERVER_PORT

        self.query_state_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.query_state_socket.bind(('0.0.0.0', self.server_port))
        self.query_state_socket.setblocking(False)

        self.networking = NetworkingManager(self.server_port)

        self.aio_task_scheduler = AIOTaskScheduler()

        self.query_processing_task: asyncio.Task | None = None

        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)


    async def query_state_controller(self, data: bytes):
        """Handles incoming messages related to query state updates."""
        message_type: int = self.networking.get_msg_type(data)

        match message_type:
            case MessageType.QueryMsg:
                # Decode the message
                worker_id, epoch_counter, state_delta = self.networking.decode_message(data)
                logging.info(f"Received state delta from worker {worker_id} for epoch {epoch_counter}")

                await self.receive_delta(worker_id,epoch_counter,state_delta)

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
            self.latest_epoch_count += 1

    async def start_tcp_service(self):

        async def request_handler(reader: StreamReader, writer: StreamWriter):
            try:
                while True:
                    data = await reader.readexactly(8)
                    (size,) = unpack('>Q', data)
                    message = await reader.readexactly(size)
                    self.aio_task_scheduler.create_task(self.query_state_controller(message))
            except asyncio.IncompleteReadError as e:
                logging.warning(f"Client disconnected unexpectedly: {e}")
            except asyncio.CancelledError:
                pass
            finally:
                logging.warning("Closing the connection")
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(request_handler, sock=self.query_state_socket, limit=2**32)
        async with server:
            await server.serve_forever()

    async def start_query_processing(self):
        kafka_consumer = AIOKafkaConsumer(bootstrap_servers=[KAFKA_URL],
                                          enable_auto_commit=False,
                                          client_id=f"{uuid.uuid4()}")
        kafka_consumer.assign(['query_processing'])
        while True:
            # start the kafka consumer
            try:
                await kafka_consumer.start()
                await self.kafka_producer.start()
            except (UnknownTopicOrPartitionError, KafkaConnectionError):
                await asyncio.sleep(1)
                logging.warning(f'Kafka at {KAFKA_URL} not ready yet, sleeping for 1 second')
                continue
            break
        # Kafka Consumer ready to consume
        while True:
            # TODO fix for freshness
            if self.epoch_count[self.latest_epoch_count]==self.total_workers:
                await self.mergeDeltas_and_updateState(self.latest_epoch_count)
            try:
                async with asyncio.timeout(KAFKA_CONSUME_TIMEOUT_MS / 1000):
                    msg = await kafka_consumer.getone()
                    query= self.networking.decode_message(msg)
                    response =  await self.get_query_state_response(query)
                    await self.send_response(response)

            except TimeoutError:
                print(f"No queries for {KAFKA_CONSUME_TIMEOUT_MS} ms")
            await asyncio.sleep(0.01)


    async def get_query_state_response(self, query):
        query_type = query['type']
        query_uuid = query['uuid']
        response={"uuid": query_uuid}

        if query_type =="GET_STATE":
            response["state"] = self.state_store
        elif query_type == "GET_OPERATOR_STATE":
            operator = query['operator']
            response["operator_state"] = self.state_store.get(operator,"operator not found")
        elif query_type == "GET_KEY_STATE":
            operator = query['operator']
            key = query['key']
            response["key_state"] = self.state_store.get(operator,{}).get(key,"key not found")
        elif query_type =="GET_ALL_KEYS_FOR_OPERATOR":
            operator = query['operator']
            if operator in self.state_store:
                response["keys"] = list(self.state_store[operator].keys())
            else:
                response["error"] = "Operator not found"
        else:
            response["error"] = "Invalid query type"

        return response

    async def send_response(self, response):
        await self.kafka_producer.send_and_wait(KAFKA_QUERY_RESPONSE_TOPIC,self.networking.encode_message(msg=response,
                                                       msg_type=MessageType.QueryMsg,
                                                       serializer=Serializer.MSGPACK))

    def start_networking_tasks(self):
        self.networking.start_networking_tasks()

    async def main(self):
        self.start_networking_tasks()
        self.query_processing_task = asyncio.create_task(self.start_query_processing())
        await self.start_tcp_service()

