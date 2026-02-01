from abc import ABC, abstractmethod


class Function(ABC):
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: dict, **kwargs: dict) -> None:
        return self.run(*args, **kwargs)

    @abstractmethod
    def run(self, *args: dict, **kwargs: dict) -> None:
        raise NotImplementedError
