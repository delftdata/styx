class BaseOperator(object):

    def __init__(self, name: str, n_partitions: int = 1):
        # operator's name
        self.name: str = name
        # number of partitions
        self.n_partitions: int = n_partitions
