import asyncio
from typing import Awaitable, Optional

from styx.common.logging import logging


class AIOTaskScheduler:
    """
    Small task scheduler that:
      - tracks background tasks
      - logs task exceptions
      - supports graceful close/cancel
      - optionally limits concurrent task execution (backpressure)

    Notes:
      - Concurrency limiting bounds *execution*, not task creation. If you also want to
        bound task creation, use a bounded queue upstream (e.g., your control_queue maxsize).
    """

    def __init__(self, max_concurrency: int = 64):
        self.background_tasks: set[asyncio.Task] = set()
        self.closed: bool = False
        self._sem = asyncio.Semaphore(max_concurrency)

    def _on_done(self, task: asyncio.Task) -> None:
        self.background_tasks.discard(task)
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        except Exception as e:
            logging.exception("Failed to retrieve task exception", exc_info=e)
            return

        if exc is not None:
            logging.exception("Background task failed", exc_info=exc)

    def create_task(self, coroutine: Awaitable) -> Optional[asyncio.Task]:
        """
        Schedule a coroutine to run in the background.
        Returns the created task (or None if scheduler is closed).
        """
        if self.closed:
            logging.warning("Trying to create a task in a closed AIOTaskScheduler!")
            return None

        async def runner():
            async with self._sem:
                return await coroutine

        task = asyncio.create_task(runner())
        self.background_tasks.add(task)
        task.add_done_callback(self._on_done)
        return task

    async def close(self) -> None:
        """
        Stop accepting new tasks, cancel all running/queued tasks, and wait for them.
        """
        self.closed = True
        tasks = list(self.background_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.background_tasks.clear()

    async def wait_all(self) -> None:
        """
        Wait until all currently scheduled tasks (and any tasks added while waiting)
        have completed.
        """
        while self.background_tasks:
            tasks = list(self.background_tasks)
            await asyncio.gather(*tasks, return_exceptions=True)
