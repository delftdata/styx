import asyncio
import logging

import aiohttp

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
                    datefmt='%I:%M:%S')
logger = logging.getLogger(__name__)

NUMBER_0F_ITEMS = 100_000
ITEM_STARTING_STOCK = 1_000_000
ITEM_PRICE = 1
NUMBER_OF_USERS = 100_000
USER_STARTING_CREDIT = 1_000_000
NUMBER_OF_ORDERS = 100_000
N_PARTITIONS = 4

ORDER_URL = PAYMENT_URL = STOCK_URL ="http://localhost:8000"


async def populate_databases():
    async with aiohttp.ClientSession() as session:
        logger.info("Init graph ...")
        url: str = f"{ORDER_URL}/submit/{N_PARTITIONS}"
        async with session.post(url) as resp:
            await resp.json()
        logger.info("Graph initialization complete. Waiting 10 sec...")
        await asyncio.sleep(10)
        logger.info("Batch creating users ...")
        url: str = (f"{PAYMENT_URL}/payment/batch_init/"
                    f"{NUMBER_OF_USERS}/{USER_STARTING_CREDIT}")
        async with session.post(url) as resp:
            await resp.json()
        logger.info("Users created")
        logger.info("Batch creating items ...")
        url: str = (f"{STOCK_URL}/stock/batch_init/"
                    f"{NUMBER_0F_ITEMS}/{ITEM_STARTING_STOCK}/{ITEM_PRICE}")
        async with session.post(url) as resp:
            await resp.json()
        logger.info("Items created")
        logger.info("Batch creating orders ...")
        url: str = (f"{ORDER_URL}/orders/batch_init/"
                    f"{NUMBER_OF_ORDERS}/{NUMBER_0F_ITEMS}/{NUMBER_OF_USERS}/{ITEM_PRICE}")
        async with session.post(url) as resp:
            await resp.json()
        logger.info("Orders created")


if __name__ == "__main__":
    asyncio.run(populate_databases())
