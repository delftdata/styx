from abc import ABC, abstractmethod


class BaseEgress(ABC):
    @abstractmethod
    def clear_messages_sent_before_recovery(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def start(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError

    @abstractmethod
    async def stop(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError

    @abstractmethod
    async def send(
        self,
        key: bytes,
        value: bytes,
        operator_name: str,
        partition: int,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_messages_sent_before_recovery(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError
