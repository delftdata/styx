from abc import abstractmethod, ABC


class BaseIngress(ABC):

    @abstractmethod
    def start(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def stop(self, *args, **kwargs):
        raise NotImplementedError
