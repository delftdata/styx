from abc import ABC, abstractmethod


class Function(ABC):

    def __init__(self, name: str):
        self.name = name

    def __call__(self, *args, **kwargs):
        return self.run(*args)

    @abstractmethod
    def run(self, *args):
        raise NotImplementedError
