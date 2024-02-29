import asyncio
import uuid

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from styx.common.logging import logging


class KafkaProducerPool(object):

    def __init__(self, kafka_url: str, size: int = 64):
        self.kafka_url = kafka_url
        self.size = size
        self.producer_pool: list[AIOKafkaProducer] = []
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self) -> AIOKafkaProducer:
        conn = self.producer_pool[self.index]
        next_idx = self.index + 1
        self.index = 0 if next_idx == self.size else next_idx
        return conn

    async def start(self, worker_id):
        for _ in range(self.size):
            self.producer_pool.append(await self.start_kafka_egress_producer(worker_id))

    async def close(self):
        for producer in self.producer_pool:
            await producer.stop()

    async def start_kafka_egress_producer(self, worker_id):
        kafka_egress_producer = AIOKafkaProducer(
            bootstrap_servers=[self.kafka_url],
            client_id=f"W{worker_id}_{uuid.uuid4()}",
            enable_idempotence=True
        )
        while True:
            try:
                await kafka_egress_producer.start()
            except KafkaConnectionError:
                await asyncio.sleep(1)
                logging.info("Waiting for Kafka")
                continue
            break
        return kafka_egress_producer
