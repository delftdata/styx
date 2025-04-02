import asyncio
import json
from aiokafka import AIOKafkaConsumer

KAFKA_BROKER_URL = "localhost:9092"
KAFKA_RESPONSE_TOPIC = "state_query_response"


async def consume_responses():
    """Consume responses from Kafka."""
    consumer = AIOKafkaConsumer(
        KAFKA_RESPONSE_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id="query_response_group",
        auto_offset_reset="earliest"
    )

    await consumer.start()
    try:
        async for msg in consumer:
            response = json.loads(msg.value)
            print(f"Received response: {response}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume_responses())
