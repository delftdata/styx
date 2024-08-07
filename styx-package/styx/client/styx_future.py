import asyncio
import dataclasses
import threading
from abc import ABC

from styx.common.exceptions import FutureAlreadySet, FutureTimedOut


@dataclasses.dataclass
class StyxResponse(object):
    request_id: bytes
    in_timestamp: int = -1
    out_timestamp: int = -1
    response: any = None

    @property
    def styx_latency_ms(self) -> float:
        return self.out_timestamp - self.in_timestamp


class BaseFuture(ABC):
    _timeout: int
    _condition: threading.Event | asyncio.Event
    _val: StyxResponse

    def __init__(self, request_id: bytes, timeout_sec: int, is_async: bool = False):
        self._val: StyxResponse = StyxResponse(request_id=request_id)
        self._condition: threading.Event | asyncio.Event = asyncio.Event() if is_async else threading.Event()
        self._timeout = timeout_sec

    @property
    def request_id(self):
        return self._val.request_id

    def set_in_timestamp(self, in_timestamp: int) -> None:
        self._val.in_timestamp = in_timestamp

    def done(self) -> bool:
        return self._condition.is_set()

    def set(self, response_val: any, out_timestamp: int):
        if self._condition.is_set():
            raise FutureAlreadySet(f"{self._val.request_id} Trying to set Future to |{response_val}|"
                                   f" Future has already been set to |{self._val.response}|")
        self._val.response = response_val
        self._val.out_timestamp = out_timestamp
        self._condition.set()


class StyxFuture(BaseFuture):
    def __init__(self, request_id: bytes, timeout_sec: int = 30):
        super().__init__(request_id, timeout_sec)

    def get(self) -> StyxResponse | None:
        success = self._condition.wait(timeout=self._timeout)
        if not success:
            raise FutureTimedOut(f"Future for request: {self._val.request_id}"
                                 f" timed out after {self._timeout} seconds.")
        return self._val


class StyxAsyncFuture(BaseFuture):
    def __init__(self, request_id: bytes, timeout_sec: int = 30):
        super().__init__(request_id, timeout_sec, is_async=True)

    async def get(self) -> StyxResponse | None:
        try:
            await asyncio.wait_for(self._condition.wait(), self._timeout)
        except asyncio.TimeoutError:
            raise FutureTimedOut(f"Future for request: {self._val.request_id}"
                                 f" timed out after {self._timeout} seconds.")
        return self._val
