from abc import ABC, abstractmethod


class BaseSnapshotter(ABC):
    @abstractmethod
    def store_snapshot(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError

    @abstractmethod
    def retrieve_snapshot(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError
