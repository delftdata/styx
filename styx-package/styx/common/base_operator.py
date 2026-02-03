from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from styx.common.types import K


class BaseOperator:
    """Abstract base class for Styx operators.

    This class defines the required interface for any operator used in a Styx dataflow.
    Concrete subclasses must implement partitioning logic.

    Attributes:
        name (str): Name of the operator.
        n_partitions (int): Number of partitions used by the operator.
    """

    def __init__(self, name: str, n_partitions: int = 1) -> None:
        """Initializes a base operator with a name and number of partitions.

        Args:
            name (str): The name of the operator.
            n_partitions (int, optional): Number of partitions. Defaults to 1.
        """
        # operator's name
        self.name: str = name
        # number of partitions
        self.n_partitions: int = n_partitions

    @abstractmethod
    def which_partition(self, key: K) -> int:
        """Determines the partition index for a given key.

        This method must be implemented by subclasses.

        Args:
            key: A key used to determine the partition.

        Returns:
            int: The partition number corresponding to the key.

        Raises:
            NotImplementedError: If not overridden by a subclass.
        """
        raise NotImplementedError

    @abstractmethod
    def make_shadow(self) -> None:
        """Marks this operator instance as a shadow partition."""
        raise NotImplementedError
