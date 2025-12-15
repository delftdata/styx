import pickle
from enum import Enum, auto

import msgspec
import cloudpickle
import gzip
import zstandard as zstd

class Serializer(Enum):
    CLOUDPICKLE = auto()
    MSGPACK = auto()
    COMPRESSED_MSGPACK = auto()
    PICKLE = auto()
    NONE = auto()

zstd_cctx = zstd.ZstdCompressor(
    level=0,
    write_checksum=False,
    write_content_size=True,
    write_dict_id=False
)
zstd_dctx = zstd.ZstdDecompressor()

def msgpack_serialization(serializable_object: object) -> bytes:
    return msgspec.msgpack.encode(serializable_object)


def msgpack_deserialization(serialized_object: bytes):
    return msgspec.msgpack.decode(serialized_object)


def compressed_msgpack_serialization(serializable_object: object) -> bytes:
    return gzip.compress(msgpack_serialization(serializable_object))


def compressed_msgpack_deserialization(serialized_object: bytes):
    return msgpack_deserialization(gzip.decompress(serialized_object))


def zstd_msgpack_serialization(serializable_object: object | bytes, already_ser: bool = False) -> bytes:
    if already_ser:
        return zstd_cctx.compress(serializable_object)
    else:
        return zstd_cctx.compress(msgpack_serialization(serializable_object))


def zstd_msgpack_deserialization(serialized_object: bytes):
    return msgpack_deserialization(zstd_dctx.decompress(serialized_object))


def cloudpickle_serialization(serializable_object: object) -> bytes:
    return cloudpickle.dumps(serializable_object)


def cloudpickle_deserialization(serialized_object: bytes):
    return cloudpickle.loads(serialized_object)


def compressed_cloudpickle_serialization(serializable_object: object) -> bytes:
    return gzip.compress(cloudpickle.dumps(serializable_object))


def compressed_cloudpickle_deserialization(serialized_object: bytes):
    return cloudpickle.loads(gzip.decompress(serialized_object))


def pickle_serialization(serializable_object: object) -> bytes:
    return pickle.dumps(serializable_object)


def pickle_deserialization(serialized_object: bytes):
    return pickle.loads(serialized_object)


def compressed_pickle_serialization(serializable_object: object) -> bytes:
    return gzip.compress(pickle.dumps(serializable_object))


def compressed_pickle_deserialization(serialized_object: bytes):
    return pickle.loads(gzip.decompress(serialized_object))
