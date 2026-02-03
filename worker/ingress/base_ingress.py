from abc import ABC, abstractmethod


class BaseIngress(ABC):
    @abstractmethod
    def start(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError

    @abstractmethod
    def stop(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError
