# cython: language_level=3, boundscheck=False, wraparound=False
"""Cython-accelerated hash partitioning for Styx keys.

Replaces the pure-Python ``make_key_hashable`` and ``get_partition_no_cache``
with C-level type dispatch, avoiding Python isinstance/isdigit overhead on
the ~97k calls per TPC-C epoch.
"""

import cityhash
from styx.common.exceptions import NonSupportedKeyTypeError


cpdef object make_key_hashable(object key):
    """Convert a key to a hashable integer at C speed.

    - int keys: returned as-is (zero overhead).
    - str keys that are all-digit: converted via int().
    - str keys: hashed via CityHash64.
    - bytes keys: hashed via CityHash64.
    - Everything else: attempt CityHash64, raise on failure.
    """
    cdef type tk = type(key)

    if tk is int:
        return key

    if tk is str:
        if (<str>key).isdigit():
            return int(key)
        return cityhash.CityHash64(key)

    if tk is bytes:
        return cityhash.CityHash64(key)

    try:
        return cityhash.CityHash64(key)
    except Exception as e:
        raise NonSupportedKeyTypeError from e


cpdef object get_partition_no_cache(
    object key,
    int partitions,
    object composite_key_hash_parameters,
):
    """Compute partition for a key without LRU cache overhead.

    Handles None keys (returns None) and composite key splitting inline,
    eliminating a Python method wrapper layer.
    """
    if key is None:
        return None

    if composite_key_hash_parameters is not None:
        field = composite_key_hash_parameters[0]
        delim = composite_key_hash_parameters[1]
        key = (<str>key).split(delim)[field]

    return make_key_hashable(key) % partitions
