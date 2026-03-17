"""Fast deep-copy replacement for the msgpack encode/decode roundtrip.

The Aria protocol requires isolation copies in get() so user functions cannot
mutate shared state.  The previous approach -- ``msgpack.decode(msgpack.encode(v))``
-- is correct but extremely expensive (~40 us per call).

``fast_deepcopy`` exploits the fact that most values flowing through state are
immutable scalars (int, float, str, bytes, bool, None) which need no copy at
all, or shallow containers (list/dict/tuple) of scalars that can be copied
cheaply.  Only truly nested structures fall back to ``copy.deepcopy``.

Requires the Cython extension ``_fast_copy`` (compiled via
``python worker/setup.py build_ext --inplace``).
"""

from worker.operator_state.aria._fast_copy import fast_deepcopy

__all__ = ["fast_deepcopy"]
