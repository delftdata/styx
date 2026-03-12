"""Unit tests for styx/client/styx_future.py"""

import asyncio
import threading

import pytest
from styx.client.styx_future import (
    StyxAsyncFuture,
    StyxFuture,
    StyxResponse,
)
from styx.common.exceptions import FutureAlreadySetError, FutureTimedOutError

# ---------------------------------------------------------------------------
# StyxResponse
# ---------------------------------------------------------------------------


class TestStyxResponse:
    def test_defaults(self):
        r = StyxResponse(request_id=b"abc")
        assert r.request_id == b"abc"
        assert r.in_timestamp == -1
        assert r.out_timestamp == -1
        assert r.response is None

    def test_latency(self):
        r = StyxResponse(request_id=b"x", in_timestamp=100, out_timestamp=150)
        assert r.styx_latency_ms == 50.0

    def test_latency_negative_when_defaults(self):
        r = StyxResponse(request_id=b"x")
        assert r.styx_latency_ms == 0  # -1 - (-1) == 0

    def test_equality(self):
        r1 = StyxResponse(request_id=b"a", in_timestamp=1, out_timestamp=2, response="ok")
        r2 = StyxResponse(request_id=b"a", in_timestamp=1, out_timestamp=2, response="ok")
        assert r1 == r2


# ---------------------------------------------------------------------------
# BaseFuture (through StyxFuture)
# ---------------------------------------------------------------------------


class TestBaseFuture:
    def test_request_id_property(self):
        f = StyxFuture(request_id=b"req1")
        assert f.request_id == b"req1"

    def test_done_initially_false(self):
        f = StyxFuture(request_id=b"r")
        assert f.done() is False

    def test_set_marks_done(self):
        f = StyxFuture(request_id=b"r")
        f.set(response_val="hello", out_timestamp=999)
        assert f.done() is True

    def test_set_stores_values(self):
        f = StyxFuture(request_id=b"r")
        f.set(response_val=42, out_timestamp=500)
        resp = f.get()
        assert resp.response == 42
        assert resp.out_timestamp == 500

    def test_set_in_timestamp(self):
        f = StyxFuture(request_id=b"r")
        f.set_in_timestamp(100)
        f.set(response_val="ok", out_timestamp=200)
        resp = f.get()
        assert resp.in_timestamp == 100

    def test_double_set_raises(self):
        f = StyxFuture(request_id=b"r")
        f.set(response_val="first", out_timestamp=1)
        with pytest.raises(FutureAlreadySetError):
            f.set(response_val="second", out_timestamp=2)

    def test_sync_uses_threading_event(self):
        f = StyxFuture(request_id=b"r")
        assert isinstance(f._condition, threading.Event)

    def test_async_uses_asyncio_event(self):
        f = StyxAsyncFuture(request_id=b"r")
        assert isinstance(f._condition, asyncio.Event)


# ---------------------------------------------------------------------------
# StyxFuture (synchronous)
# ---------------------------------------------------------------------------


class TestStyxFuture:
    def test_get_returns_response(self):
        f = StyxFuture(request_id=b"r", timeout_sec=5)
        f.set(response_val="data", out_timestamp=100)
        result = f.get()
        assert result.response == "data"
        assert result.request_id == b"r"

    def test_get_timeout_raises(self):
        f = StyxFuture(request_id=b"r", timeout_sec=0)
        with pytest.raises(FutureTimedOutError):
            f.get()

    def test_get_blocks_until_set(self):
        f = StyxFuture(request_id=b"r", timeout_sec=5)

        def setter():
            f.set(response_val="delayed", out_timestamp=200)

        t = threading.Thread(target=setter)
        t.start()
        result = f.get()
        t.join()
        assert result.response == "delayed"

    def test_default_timeout_is_30(self):
        f = StyxFuture(request_id=b"r")
        assert f._timeout == 30


# ---------------------------------------------------------------------------
# StyxAsyncFuture
# ---------------------------------------------------------------------------


class TestStyxAsyncFuture:
    @pytest.mark.asyncio
    async def test_get_returns_response(self):
        f = StyxAsyncFuture(request_id=b"r", timeout_sec=5)
        f.set(response_val="async_data", out_timestamp=300)
        result = await f.get()
        assert result.response == "async_data"

    @pytest.mark.asyncio
    async def test_get_timeout_raises(self):
        f = StyxAsyncFuture(request_id=b"r", timeout_sec=0)
        with pytest.raises(FutureTimedOutError):
            await f.get()

    @pytest.mark.asyncio
    async def test_get_waits_for_set(self):
        f = StyxAsyncFuture(request_id=b"r", timeout_sec=5)

        async def setter():
            await asyncio.sleep(0.01)
            f.set(response_val="later", out_timestamp=400)

        asyncio.create_task(setter())
        result = await f.get()
        assert result.response == "later"

    def test_default_timeout_is_30(self):
        f = StyxAsyncFuture(request_id=b"r")
        assert f._timeout == 30

    @pytest.mark.asyncio
    async def test_latency_calculation(self):
        f = StyxAsyncFuture(request_id=b"r", timeout_sec=5)
        f.set_in_timestamp(100)
        f.set(response_val="ok", out_timestamp=250)
        result = await f.get()
        assert result.styx_latency_ms == 150.0
