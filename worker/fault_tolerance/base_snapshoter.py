from abc import abstractmethod, ABC


class BaseSnapshotter(ABC):

    @abstractmethod
    def store_snapshot(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def retrieve_snapshot(self, *args, **kwargs):
        raise NotImplementedError
