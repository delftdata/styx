import asyncio
import os
import uuid

from aiokafka import AIOKafkaProducer
from aiokafka.structs import RecordMetadata
from kafka.errors import KafkaConnectionError
from styx.common.logging import logging


EGRESS_TOPIC_NAME: str = os.getenv('EGRESS_TOPIC_NAME', 'styx-egress')


class KafkaRateLimitedProducer(object):

    def __init__(self, kafka_url: str, rate: int = 150):
        self.kafka_url = kafka_url
        self.producer: AIOKafkaProducer = ...
        self.rate_limier: asyncio.Semaphore = asyncio.Semaphore(rate)
        self.worker_id: int = -1

    async def start(self, worker_id: int):
        self.worker_id = worker_id
        self.producer = AIOKafkaProducer(
            bootstrap_servers=[self.kafka_url],
            client_id=f"W{worker_id}_{uuid.uuid4()}",
            enable_idempotence=True
        )
        while True:
            try:
                await self.producer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break

    async def close(self):
        await self.producer.stop()

    async def send_message(self, key, value):
        async with self.rate_limier:
            res: RecordMetadata = await self.producer.send_and_wait(EGRESS_TOPIC_NAME,
                                                                    key=key,
                                                                    value=value,
                                                                    partition=self.worker_id-1)
        return res.offset
