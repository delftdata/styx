import pickle
from enum import Enum, auto

import msgspec
import cloudpickle
import gzip


class Serializer(Enum):
    CLOUDPICKLE = auto()
    MSGPACK = auto()
    PICKLE = auto()
    NONE = auto()


def msgpack_serialization(serializable_object: object) -> bytes:
    return msgspec.msgpack.encode(serializable_object)


def msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgspec.msgpack.decode(serialized_object)


def compressed_msgpack_serialization(serializable_object: object) -> bytes:
    return gzip.compress(msgpack_serialization(serializable_object))


def compressed_msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgpack_deserialization(gzip.decompress(serialized_object))


def cloudpickle_serialization(serializable_object: object) -> bytes:
    return cloudpickle.dumps(serializable_object)


def cloudpickle_deserialization(serialized_object: bytes) -> dict:
    return cloudpickle.loads(serialized_object)


def compressed_cloudpickle_serialization(serializable_object: object) -> bytes:
    return gzip.compress(cloudpickle.dumps(serializable_object))


def compressed_cloudpickle_deserialization(serialized_object: bytes) -> dict:
    return cloudpickle.loads(gzip.decompress(serialized_object))


def pickle_serialization(serializable_object: object) -> bytes:
    return pickle.dumps(serializable_object)


def pickle_deserialization(serialized_object: bytes) -> dict:
    return pickle.loads(serialized_object)


def compressed_pickle_serialization(serializable_object: object) -> bytes:
    return gzip.compress(pickle.dumps(serializable_object))


def compressed_pickle_deserialization(serialized_object: bytes) -> dict:
    return pickle.loads(gzip.decompress(serialized_object))
