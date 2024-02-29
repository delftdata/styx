from abc import abstractmethod, ABC


class BaseEgress(ABC):

    @abstractmethod
    def clear_messages_sent_before_recovery(self):
        raise NotImplementedError

    @abstractmethod
    async def start(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    async def stop(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    async def send(self, key, value):
        raise NotImplementedError

    @abstractmethod
    async def get_messages_sent_before_recovery(self, *args, **kwargs):
        raise NotImplementedError
