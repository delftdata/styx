from abc import ABC, abstractmethod


class BaseTransactionalProtocol(ABC):
    @abstractmethod
    async def run_function(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def run_fallback_function(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def function_scheduler(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def communication_protocol(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def take_snapshot(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def start(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError
