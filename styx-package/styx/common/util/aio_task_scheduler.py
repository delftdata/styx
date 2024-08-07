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
        while self.background_tasks:
            task = self.background_tasks.pop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                logging.error(f"Could not cancel task: {task}")
        self.background_tasks = set()

    async def wait_all(self):
        while self.background_tasks:
            await self.background_tasks.pop()
        self.background_tasks = set()
