from abc import ABC, abstractmethod


class BaseIngress(ABC):
    @abstractmethod
    def start(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError
