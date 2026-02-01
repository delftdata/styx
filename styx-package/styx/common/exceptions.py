class SerializerNotSupportedError(Exception):
    """
    Exception thrown when the selected serializer is not supported
    """


class OperatorDoesNotContainFunctionError(Exception):
    """
    If the operator does not contain the specific function
    """


class NonSupportedKeyTypeError(Exception):
    """
    If the key type is not supported
    """


class NotAStateflowGraphError(Exception):
    """
    When the client did not submit a correct graph
    """


class FutureAlreadySetError(Exception):
    """
    When the client attempts to set the same future twice
    """


class FutureTimedOutError(Exception):
    """
    When a Styx future times out
    """


class InvalidRangePartitioningError(Exception):
    pass


class GraphNotSerializableError(Exception):
    pass
