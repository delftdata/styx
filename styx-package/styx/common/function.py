from abc import ABC, abstractmethod


class Function(ABC):
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return self.run(*args, **kwargs)

    @abstractmethod
    def run(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        raise NotImplementedError
