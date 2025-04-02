import asyncio
import json
import os

from aiokafka import AIOKafkaProducer
from styx.common import networking
from styx.common.message_types import MessageType
from styx.common.serialization import Serializer
from styx.common.tcp_networking import NetworkingManager

KAFKA_QUERY_TOPIC = "query_processing"
KAFKA_URL: str = os.getenv('KAFKA_URL', None)
SERVER_PORT=8080

class ClientQueries:
    def __init__(self):
        self.server_port = SERVER_PORT
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)
        self.networking = NetworkingManager(self.server_port)

    async def start(self):
        """Start Kafka Producer"""
        await self.kafka_producer.start()
        await asyncio.create_task(self.publish_client_queries())

    async def stop(self):
        """Shutdown Kafka"""
        await self.kafka_producer.stop()

    async def publish_client_queries(self):

        '''api queries with uuid ..TO BE CHANGED'''
        queries = [
            {"type": "GET_STATE", "uuid": "1234-uuid"},
            {"type": "GET_OPERATOR_STATE", "uuid": "5678-uuid", "operator": "operator1"},
            {"type": "GET_KEY_STATE", "uuid": "91011-uuid", "operator": "operator2", "key": "key1"},
            {"type": "GET_ALL_KEYS_FOR_OPERATOR", "uuid": "1213-uuid", "operator": "operator3"},
        ]

        # Send each query to Kafka topic
        for query in queries:
            await self.kafka_producer.send_and_wait(KAFKA_QUERY_TOPIC, self.networking.encode_message(msg=query,
                                                       msg_type=MessageType.QueryMsg,
                                                       serializer=Serializer.MSGPACK))

    async def main(self):
        await self.start()

