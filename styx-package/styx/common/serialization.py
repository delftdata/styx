import pickle
from enum import Enum, auto

import msgspec
import cloudpickle
import gzip


class Serializer(Enum):
    """Enumeration of supported serialization formats.

    Values:
        CLOUDPICKLE: Uses `cloudpickle` for serialization.
        MSGPACK: Uses `msgspec.msgpack` for serialization.
        PICKLE: Uses Python’s built-in `pickle`.
        NONE: No serialization applied.
    """
    CLOUDPICKLE = auto()
    MSGPACK = auto()
    PICKLE = auto()
    NONE = auto()


def msgpack_serialization(serializable_object: object) -> bytes:
    """Serializes an object using MessagePack.

    Args:
        serializable_object (object): Object to serialize.

    Returns:
        bytes: Serialized byte representation.
    """
    return msgspec.msgpack.encode(serializable_object)


def msgpack_deserialization(serialized_object: bytes) -> object:
    """Deserializes a MessagePack-encoded byte object.

    Args:
        serialized_object (bytes): Serialized MessagePack bytes.

    Returns:
        object: The deserialized object.
    """
    return msgspec.msgpack.decode(serialized_object)


def compressed_msgpack_serialization(serializable_object: object) -> bytes:
    """Serializes and compresses an object using MessagePack and gzip.

    Args:
        serializable_object (object): Object to serialize.

    Returns:
        bytes: Compressed and serialized bytes.
    """
    return gzip.compress(msgpack_serialization(serializable_object))


def compressed_msgpack_deserialization(serialized_object: bytes)-> object:
    """Decompresses and deserializes a gzip-compressed MessagePack object.

    Args:
        serialized_object (bytes): Compressed serialized bytes.

    Returns:
        object: The deserialized object.
    """
    return msgpack_deserialization(gzip.decompress(serialized_object))


def cloudpickle_serialization(serializable_object: object) -> bytes:
    """Serializes an object using cloudpickle.

    Args:
        serializable_object (object): Object to serialize.

    Returns:
        bytes: Serialized byte representation.
    """
    return cloudpickle.dumps(serializable_object)


def cloudpickle_deserialization(serialized_object: bytes)-> object:
    """Deserializes a cloudpickle-encoded byte object.

    Args:
        serialized_object (bytes): Serialized cloudpickle bytes.

    Returns:
        object: The deserialized object.
    """
    return cloudpickle.loads(serialized_object)


def compressed_cloudpickle_serialization(serializable_object: object) -> bytes:
    """Serializes and compresses an object using cloudpickle and gzip.

    Args:
        serializable_object (object): Object to serialize.

    Returns:
        bytes: Compressed and serialized bytes.
    """
    return gzip.compress(cloudpickle.dumps(serializable_object))


def compressed_cloudpickle_deserialization(serialized_object: bytes)-> object:
    """Decompresses and deserializes a gzip-compressed cloudpickle object.

    Args:
        serialized_object (bytes): Compressed serialized bytes.

    Returns:
        object: The deserialized object.
    """
    return cloudpickle.loads(gzip.decompress(serialized_object))


def pickle_serialization(serializable_object: object) -> bytes:
    """Serializes an object using Python's built-in pickle module.

    Args:
        serializable_object (object): Object to serialize.

    Returns:
        bytes: Serialized byte representation.
    """
    return pickle.dumps(serializable_object)


def pickle_deserialization(serialized_object: bytes)-> object:
    """Deserializes a pickle-encoded byte object.

    Args:
        serialized_object (bytes): Serialized pickle bytes.

    Returns:
        object: The deserialized object.
    """
    return pickle.loads(serialized_object)


def compressed_pickle_serialization(serializable_object: object) -> bytes:
    """Serializes and compresses an object using pickle and gzip.

    Args:
        serializable_object (object): Object to serialize.

    Returns:
        bytes: Compressed and serialized bytes.
    """
    return gzip.compress(pickle.dumps(serializable_object))


def compressed_pickle_deserialization(serialized_object: bytes)-> object:
    """Decompresses and deserializes a gzip-compressed pickle object.

    Args:
        serialized_object (bytes): Compressed serialized bytes.

    Returns:
        object: The deserialized object.
    """
    return pickle.loads(gzip.decompress(serialized_object))
