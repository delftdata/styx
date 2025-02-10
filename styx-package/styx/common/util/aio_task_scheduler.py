import asyncio

from styx.common.logging import logging


class AIOTaskScheduler(object):

    def __init__(self):
        # background task references
        self.background_tasks: set[asyncio.Future] = set()
        self.closed = False

    def create_task(self, coroutine):
        if self.closed:
            logging.warning("Trying to create a task in a closed AIOTaskScheduler!")
            return
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def close(self):
        self.closed = True
        for task in self.background_tasks:
            task.cancel()
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()

    async def wait_all(self):
        await asyncio.gather(*self.background_tasks)
