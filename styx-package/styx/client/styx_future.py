import asyncio
import dataclasses
import threading
from abc import ABC

from styx.common.exceptions import FutureAlreadySet, FutureTimedOut


@dataclasses.dataclass
class StyxResponse(object):
    """Encapsulates a response from a Styx function invocation.

    Attributes:
        request_id (bytes): The unique identifier of the request.
        in_timestamp (int): The time the request was sent, in ms.
        out_timestamp (int): The time the response was received, in ms.
        response (any): The actual response value returned.
    """
    request_id: bytes
    in_timestamp: int = -1
    out_timestamp: int = -1
    response: any = None

    @property
    def styx_latency_ms(self) -> float:
        """float: The measured latency of the function in milliseconds."""
        return self.out_timestamp - self.in_timestamp


class BaseFuture(ABC):
    """Abstract base class for representing asynchronous or blocking Styx results.

    Tracks a future-style response using either `threading.Event` or `asyncio.Event`
    based on the execution context.

    Attributes:
        _timeout (int): Timeout duration in seconds.
        _val (StyxResponse): Encapsulated response and timestamps.
        _condition: Event object (threading or asyncio) to signal completion.
    """
    _timeout: int
    _condition: threading.Event | asyncio.Event
    _val: StyxResponse

    def __init__(self, request_id: bytes, timeout_sec: int, is_async: bool = False):
        """Initializes a future for a given request.

        Args:
            request_id (bytes): Unique ID for the request.
            timeout_sec (int): Timeout in seconds.
            is_async (bool, optional): Whether to use asyncio. Defaults to False.
        """
        self._val: StyxResponse = StyxResponse(request_id=request_id)
        self._condition: threading.Event | asyncio.Event = asyncio.Event() if is_async else threading.Event()
        self._timeout = timeout_sec

    @property
    def request_id(self):
        """bytes: The request ID associated with this future."""
        return self._val.request_id

    def set_in_timestamp(self, in_timestamp: int) -> None:
        """Sets the time the request was issued.

        Args:
            in_timestamp (int): Timestamp in milliseconds.
        """
        self._val.in_timestamp = in_timestamp

    def done(self) -> bool:
        """Checks if the future has been fulfilled.

        Returns:
            bool: True if the result is available, False otherwise.
        """
        return self._condition.is_set()

    def set(self, response_val: any, out_timestamp: int):
        """Fulfills the future with a response.

        Args:
            response_val (any): The value to store as the response.
            out_timestamp (int): The time the response was received.

        Raises:
            FutureAlreadySet: If the future was already fulfilled.
        """
        if self._condition.is_set():
            raise FutureAlreadySet(f"{self._val.request_id} Trying to set Future to |{response_val}|"
                                   f" Future has already been set to |{self._val.response}|")
        self._val.response = response_val
        self._val.out_timestamp = out_timestamp
        self._condition.set()


class StyxFuture(BaseFuture):
    """Blocking future for retrieving Styx function results synchronously."""

    def __init__(self, request_id: bytes, timeout_sec: int = 30):
        """Initializes a synchronous future.

        Args:
            request_id (bytes): Unique request ID.
            timeout_sec (int, optional): Timeout in seconds. Defaults to 30.
        """
        super().__init__(request_id, timeout_sec)

    def get(self) -> StyxResponse | None:
        """Blocks until the result is available or times out.

        Returns:
            StyxResponse | None: The received response.

        Raises:
            FutureTimedOut: If the future is not fulfilled within the timeout.
        """
        success = self._condition.wait(timeout=self._timeout)
        if not success:
            raise FutureTimedOut(f"Future for request: {self._val.request_id}"
                                 f" timed out after {self._timeout} seconds.")
        return self._val


class StyxAsyncFuture(BaseFuture):
    """Async future for retrieving Styx function results with asyncio."""

    def __init__(self, request_id: bytes, timeout_sec: int = 30):
        """Initializes an asynchronous future.

        Args:
            request_id (bytes): Unique request ID.
            timeout_sec (int, optional): Timeout in seconds. Defaults to 30.
        """
        super().__init__(request_id, timeout_sec, is_async=True)

    async def get(self) -> StyxResponse | None:
        """Awaits the result with a timeout.

        Returns:
            StyxResponse | None: The received response.

        Raises:
            FutureTimedOut: If the future is not fulfilled within the timeout.
        """
        try:
            await asyncio.wait_for(self._condition.wait(), self._timeout)
        except asyncio.TimeoutError:
            raise FutureTimedOut(f"Future for request: {self._val.request_id}"
                                 f" timed out after {self._timeout} seconds.")
        return self._val
