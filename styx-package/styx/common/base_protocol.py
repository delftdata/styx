from abc import ABC, abstractmethod


class BaseTransactionalProtocol(ABC):
    @abstractmethod
    async def run_function(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError

    @abstractmethod
    async def run_fallback_function(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError

    @abstractmethod
    async def function_scheduler(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError

    @abstractmethod
    async def communication_protocol(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def take_snapshot(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError

    @abstractmethod
    async def stop(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError

    @abstractmethod
    async def start(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError
