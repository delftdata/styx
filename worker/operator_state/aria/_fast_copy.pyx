# cython: language_level=3, boundscheck=False, wraparound=False
"""Cython-accelerated deep copy for Aria state isolation.

Provides C-speed type dispatch for the common value types that flow through
operator state (scalars, flat containers of scalars, and one-level-nested
containers).  Falls back to ``copy.deepcopy`` only for deeply nested structures.
"""

from copy import deepcopy


cdef inline bint _is_scalar(object v):
    """Check if v is an immutable scalar that needs no copying."""
    cdef type tv = type(v)
    return tv is int or tv is float or tv is str or tv is bytes or tv is bool or v is None


cdef inline bint _is_scalar_tuple(object v):
    """Check if v is a tuple containing only scalars (immutable, safe to share)."""
    if type(v) is not tuple:
        return False
    cdef object x
    for x in <tuple>v:
        if not _is_scalar(x):
            return False
    return True


cdef inline bint _is_alias_safe(object v):
    """Scalar or tuple-of-scalars — fully immutable, no copy needed."""
    return _is_scalar(v) or _is_scalar_tuple(v)


cdef inline bint _all_alias_safe_list(list lst):
    """Check if every element of *lst* is alias-safe (scalar or scalar-tuple)."""
    cdef object v
    for v in lst:
        if not _is_alias_safe(v):
            return False
    return True


cdef inline bint _all_alias_safe_dict_values(dict d):
    """Check if every value of *d* is alias-safe."""
    cdef object v
    for v in d.values():
        if not _is_alias_safe(v):
            return False
    return True


cdef object _copy_one_level(object value):
    """Copy a non-scalar value that is known to be a flat container of scalars.

    For containers whose elements may themselves be flat containers of scalars,
    perform a two-level copy.
    """
    cdef type tv = type(value)
    cdef object v
    cdef dict result_dict
    cdef list result_list

    if tv is dict:
        # Fast path: dict whose values are all alias-safe (scalars or scalar-tuples)
        if _all_alias_safe_dict_values(<dict>value):
            return (<dict>value).copy()
        # Two-level: dict of (flat containers / scalar-tuples / scalars)
        result_dict = {}
        for k, v in (<dict>value).items():
            result_dict[k] = _shallow_or_deepcopy(v)
        return result_dict

    if tv is list:
        if _all_alias_safe_list(<list>value):
            return (<list>value).copy()
        # Two-level: list of flat containers / scalar-tuples / scalars
        result_list = []
        for v in <list>value:
            result_list.append(_shallow_or_deepcopy(v))
        return result_list

    if tv is tuple:
        if _is_scalar_tuple(value):
            return value  # immutable, safe to share
        return tuple(_shallow_or_deepcopy(v) for v in <tuple>value)

    return deepcopy(value)


cdef inline object _shallow_or_deepcopy(object v):
    """Copy a single element: alias-safe -> as-is, flat container -> .copy(), else deepcopy."""
    if _is_alias_safe(v):
        return v
    cdef type tv = type(v)
    if tv is dict:
        if _all_alias_safe_dict_values(<dict>v):
            return (<dict>v).copy()
        return deepcopy(v)
    if tv is list:
        if _all_alias_safe_list(<list>v):
            return (<list>v).copy()
        return deepcopy(v)
    if tv is tuple:
        # Already handled by _is_alias_safe for scalar-tuples; here it's a tuple
        # containing non-scalar elements → deep copy.
        return deepcopy(v)
    return deepcopy(v)


cpdef object fast_deepcopy(object value):
    """Return an isolated copy of *value* suitable for user-function reads.

    Handles up to two levels of nesting without falling back to copy.deepcopy:

    * Immutable scalars (int, float, str, bytes, bool, None) -> returned as-is.
    * Flat list/dict/tuple of scalars -> shallow copy.
    * Dict/list of flat dicts/lists of scalars -> two-level copy.
    * Anything deeper -> ``copy.deepcopy``.
    """
    if _is_scalar(value):
        return value
    return _copy_one_level(value)
