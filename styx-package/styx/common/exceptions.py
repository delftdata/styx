class SerializerNotSupported(Exception):
    """
    Exception thrown when the selected serializer is not supported
    """
    pass


class OperatorDoesNotContainFunction(Exception):
    """
    If the operator does not contain the specific function
    """
    pass


class NonSupportedKeyType(Exception):
    """
    If the key type is not supported
    """
    pass


class NotAStateflowGraph(Exception):
    """
    When the client did not submit a correct graph
    """
    pass


class FutureAlreadySet(Exception):
    """
    When the client attempts to set the same future twice
    """
    pass


class FutureTimedOut(Exception):
    """
    When a Styx future times out
    """
    pass

class InvalidRangePartitioning(Exception):
    pass
