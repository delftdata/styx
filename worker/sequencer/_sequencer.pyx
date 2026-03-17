# cython: language_level=3, boundscheck=False, wraparound=False
"""Cython-accelerated sequencer hot-path helpers.

The ``sequence()`` and ``get_t_id()`` methods are called once per transaction
per epoch (~20k calls on YCSB).  Moving the tight counter arithmetic and
dict lookup to Cython eliminates Python method dispatch overhead.
"""


cpdef int cy_get_t_id(int sequencer_id, int t_counter, int n_workers):
    """Compute t_id = sequencer_id + t_counter * n_workers."""
    return sequencer_id + t_counter * n_workers


cpdef int cy_sequence(
    object message,
    dict request_id_to_t_id_map,
    set t_ids_in_wal,
    list distributed_log,
    object sequenced_item_cls,
    int sequencer_id,
    int t_counter,
    int n_workers,
):
    """Sequence a single message, returning the updated t_counter.

    Inlines get_t_id() and the WAL-skip loop to avoid per-call Python overhead.
    """
    cdef object request_id = message.request_id
    cdef int t_id

    if request_id in request_id_to_t_id_map:
        t_id = <int>request_id_to_t_id_map[request_id]
    else:
        t_id = sequencer_id + t_counter * n_workers
        t_counter += 1
        while t_id in t_ids_in_wal:
            t_id = sequencer_id + t_counter * n_workers
            t_counter += 1

    distributed_log.append(sequenced_item_cls(t_id, message))
    return t_counter
