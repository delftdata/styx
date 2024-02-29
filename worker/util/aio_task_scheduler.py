import asyncio


class AIOTaskScheduler(object):

    def __init__(self):
        # background task references
        self.background_tasks: set[asyncio.Future] = set()

    def create_task(self, coroutine):
        task = asyncio.shield(asyncio.create_task(coroutine))
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def close(self):
        for task in self.background_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.background_tasks = set()

    async def wait_all(self):
        while self.background_tasks:
            await self.background_tasks.pop()
        self.background_tasks = set()
