"""Unit tests for styx/common/serialization.py"""

import dataclasses

from styx.common.serialization import (
    cloudpickle_deserialization,
    cloudpickle_serialization,
    compressed_cloudpickle_deserialization,
    compressed_cloudpickle_serialization,
    compressed_msgpack_deserialization,
    compressed_msgpack_serialization,
    compressed_pickle_deserialization,
    compressed_pickle_serialization,
    msgpack_deserialization,
    msgpack_serialization,
    pickle_deserialization,
    pickle_serialization,
    zstd_msgpack_deserialization,
    zstd_msgpack_serialization,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _Point:
    x: int
    y: int


_SCALARS = [0, -1, 42, 3.14, "hello", b"bytes", True, None]
_COLLECTIONS = [[], [1, 2, 3], {"a": 1, "b": [2, 3]}, (1, "x")]


# ---------------------------------------------------------------------------
# msgpack
# ---------------------------------------------------------------------------


class TestMsgpack:
    def test_roundtrip_int(self):
        assert msgpack_deserialization(msgpack_serialization(42)) == 42

    def test_roundtrip_string(self):
        assert msgpack_deserialization(msgpack_serialization("styx")) == "styx"

    def test_roundtrip_none(self):
        assert msgpack_deserialization(msgpack_serialization(None)) is None

    def test_roundtrip_list(self):
        data = [1, 2, 3]
        assert msgpack_deserialization(msgpack_serialization(data)) == data

    def test_roundtrip_dict(self):
        data = {"key": 1, "nested": [1, 2]}
        assert msgpack_deserialization(msgpack_serialization(data)) == data

    def test_returns_bytes(self):
        assert isinstance(msgpack_serialization(1), bytes)


# ---------------------------------------------------------------------------
# compressed msgpack (gzip)
# ---------------------------------------------------------------------------


class TestCompressedMsgpack:
    def test_roundtrip(self):
        data = {"x": [1, 2, 3], "y": "hello"}
        assert compressed_msgpack_deserialization(compressed_msgpack_serialization(data)) == data

    def test_returns_bytes(self):
        assert isinstance(compressed_msgpack_serialization(42), bytes)

    def test_smaller_than_raw_for_large_data(self):
        large = list(range(1000))
        raw = msgpack_serialization(large)
        compressed = compressed_msgpack_serialization(large)
        assert len(compressed) < len(raw)


# ---------------------------------------------------------------------------
# zstd + msgpack
# ---------------------------------------------------------------------------


class TestZstdMsgpack:
    def test_roundtrip(self):
        data = [1, "two", {"three": 3}]
        assert zstd_msgpack_deserialization(zstd_msgpack_serialization(data)) == data

    def test_returns_bytes(self):
        assert isinstance(zstd_msgpack_serialization("hello"), bytes)

    def test_already_ser_flag(self):
        raw_bytes = msgpack_serialization({"a": 1})
        compressed = zstd_msgpack_serialization(raw_bytes, already_ser=True)
        assert zstd_msgpack_deserialization(compressed) == {"a": 1}

    def test_already_ser_false_default(self):
        data = [10, 20, 30]
        serialized = zstd_msgpack_serialization(data)
        assert zstd_msgpack_deserialization(serialized) == data


# ---------------------------------------------------------------------------
# cloudpickle
# ---------------------------------------------------------------------------


class TestCloudpickle:
    def test_roundtrip_dict(self):
        data = {"a": 1}
        assert cloudpickle_deserialization(cloudpickle_serialization(data)) == data

    def test_roundtrip_dataclass(self):
        p = _Point(3, 7)
        result = cloudpickle_deserialization(cloudpickle_serialization(p))
        assert result.x == 3 and result.y == 7

    def test_roundtrip_lambda(self):
        fn = lambda x: x * 2  # noqa: E731
        result = cloudpickle_deserialization(cloudpickle_serialization(fn))
        assert result(5) == 10

    def test_returns_bytes(self):
        assert isinstance(cloudpickle_serialization(42), bytes)


# ---------------------------------------------------------------------------
# compressed cloudpickle (gzip)
# ---------------------------------------------------------------------------


class TestCompressedCloudpickle:
    def test_roundtrip(self):
        data = {"nested": [1, 2, {"deep": True}]}
        result = compressed_cloudpickle_deserialization(compressed_cloudpickle_serialization(data))
        assert result == data

    def test_returns_bytes(self):
        assert isinstance(compressed_cloudpickle_serialization("x"), bytes)


# ---------------------------------------------------------------------------
# pickle
# ---------------------------------------------------------------------------


class TestPickle:
    def test_roundtrip_int(self):
        assert pickle_deserialization(pickle_serialization(99)) == 99

    def test_roundtrip_dataclass(self):
        p = _Point(1, 2)
        result = pickle_deserialization(pickle_serialization(p))
        assert result.x == 1 and result.y == 2

    def test_roundtrip_list(self):
        data = [10, 20, 30]
        assert pickle_deserialization(pickle_serialization(data)) == data

    def test_returns_bytes(self):
        assert isinstance(pickle_serialization("hello"), bytes)


# ---------------------------------------------------------------------------
# compressed pickle (gzip)
# ---------------------------------------------------------------------------


class TestCompressedPickle:
    def test_roundtrip(self):
        data = {"a": [1, 2], "b": "hello"}
        result = compressed_pickle_deserialization(compressed_pickle_serialization(data))
        assert result == data

    def test_returns_bytes(self):
        assert isinstance(compressed_pickle_serialization(1), bytes)
