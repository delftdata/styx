"""Unit tests for styx/common/util/aio_task_scheduler.py"""

import asyncio

import pytest
from styx.common.util.aio_task_scheduler import AIOTaskScheduler


class TestAIOTaskSchedulerInit:
    def test_defaults(self):
        s = AIOTaskScheduler()
        assert s.closed is False
        assert len(s.background_tasks) == 0

    def test_custom_concurrency(self):
        s = AIOTaskScheduler(max_concurrency=10)
        assert s._sem._value == 10


class TestAIOTaskSchedulerCreateTask:
    @pytest.mark.asyncio
    async def test_create_and_run_task(self):
        s = AIOTaskScheduler()
        results = []

        async def worker():
            results.append(1)

        s.create_task(worker())
        await s.wait_all()
        assert results == [1]

    @pytest.mark.asyncio
    async def test_multiple_tasks(self):
        s = AIOTaskScheduler()
        results = []

        for i in range(5):

            async def worker(val=i):
                results.append(val)

            s.create_task(worker())

        await s.wait_all()
        assert sorted(results) == [0, 1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_closed_scheduler_ignores_new_tasks(self):
        s = AIOTaskScheduler()
        await s.close()
        assert s.closed is True

        results = []

        async def worker():
            results.append(1)

        s.create_task(worker())
        # Give a small window for any accidental execution
        await asyncio.sleep(0.05)
        assert results == []

    @pytest.mark.asyncio
    async def test_concurrency_limit(self):
        s = AIOTaskScheduler(max_concurrency=2)
        active = []
        max_active = [0]

        async def worker():
            active.append(1)
            max_active[0] = max(max_active[0], len(active))
            await asyncio.sleep(0.01)
            active.pop()

        for _ in range(5):
            s.create_task(worker())

        await s.wait_all()
        assert max_active[0] <= 2


class TestAIOTaskSchedulerClose:
    @pytest.mark.asyncio
    async def test_close_cancels_tasks(self):
        s = AIOTaskScheduler()

        async def long_task():
            await asyncio.sleep(100)

        s.create_task(long_task())
        await asyncio.sleep(0.01)  # let it start
        await s.close()
        assert s.closed is True
        assert len(s.background_tasks) == 0

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        s = AIOTaskScheduler()
        await s.close()
        await s.close()  # should not raise


class TestAIOTaskSchedulerWaitAll:
    @pytest.mark.asyncio
    async def test_wait_all_empty(self):
        s = AIOTaskScheduler()
        await s.wait_all()  # should not block

    @pytest.mark.asyncio
    async def test_task_exception_logged_not_raised(self):
        s = AIOTaskScheduler()

        async def failing():
            raise ValueError("boom")

        s.create_task(failing())
        await s.wait_all()
        # Should complete without raising (exception is logged)
        assert len(s.background_tasks) == 0
