import sys

from aiokafka import AIOKafkaConsumer
import pandas as pd

import uvloop
from styx.common.serialization import msgpack_deserialization


SAVE_DIR: str = sys.argv[1]


async def consume():
    records = []
    consumer = AIOKafkaConsumer(
        'styx-egress',
        auto_offset_reset='earliest',
        value_deserializer=msgpack_deserialization,
        bootstrap_servers='localhost:9092')
    await consumer.start()
    try:
        # Consume messages
        while True:
            data = await consumer.getmany(timeout_ms=10_000)
            if not data:
                break
            for messages in data.values():
                for msg in messages:
                    # print("consumed: ", msg.key, msg.value, msg.timestamp)
                    records.append((msg.key, msg.value, msg.timestamp))
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()
        pd.DataFrame.from_records(records,
                                  columns=['request_id', 'response', 'timestamp']).to_csv(f'{SAVE_DIR}/output.csv',
                                                                                          index=False)

uvloop.run(consume())
