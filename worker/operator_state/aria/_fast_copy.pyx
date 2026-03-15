# cython: language_level=3, boundscheck=False, wraparound=False
"""Cython-accelerated deep copy for Aria state isolation.

Provides C-speed type dispatch for the common value types that flow through
operator state (scalars, flat lists/dicts of scalars).  Falls back to
``copy.deepcopy`` for anything complex.
"""

from cpython.ref cimport PyObject
from cpython.object cimport PyObject_TypeCheck
from copy import deepcopy


cpdef object fast_deepcopy(object value):
    """Return an isolated copy of *value* suitable for user-function reads.

    * Immutable scalars (int, float, str, bytes, bool, None) → returned as-is.
    * Flat list/dict/tuple of scalars → shallow copy (one level).
    * Anything else → ``copy.deepcopy``.
    """
    cdef type tv = type(value)

    # --- Immutable scalars: no copy needed ---
    if tv is int or tv is float or tv is str or tv is bytes or tv is bool or value is None:
        return value

    cdef object v
    cdef bint all_scalar

    # --- list ---
    if tv is list:
        all_scalar = True
        for v in <list>value:
            tv_inner = type(v)
            if not (tv_inner is int or tv_inner is float or tv_inner is str
                    or tv_inner is bytes or tv_inner is bool or v is None):
                all_scalar = False
                break
        if all_scalar:
            return (<list>value).copy()
        return deepcopy(value)

    # --- dict ---
    if tv is dict:
        all_scalar = True
        for v in (<dict>value).values():
            tv_inner = type(v)
            if not (tv_inner is int or tv_inner is float or tv_inner is str
                    or tv_inner is bytes or tv_inner is bool or v is None):
                all_scalar = False
                break
        if all_scalar:
            return (<dict>value).copy()
        return deepcopy(value)

    # --- tuple (immutable container) ---
    if tv is tuple:
        all_scalar = True
        for v in <tuple>value:
            tv_inner = type(v)
            if not (tv_inner is int or tv_inner is float or tv_inner is str
                    or tv_inner is bytes or tv_inner is bool or v is None):
                all_scalar = False
                break
        if all_scalar:
            return value  # tuple is immutable
        return deepcopy(value)

    # --- Fallback ---
    return deepcopy(value)
