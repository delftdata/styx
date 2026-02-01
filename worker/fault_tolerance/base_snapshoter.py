from abc import ABC, abstractmethod


class BaseSnapshotter(ABC):
    @abstractmethod
    def store_snapshot(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def retrieve_snapshot(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError
