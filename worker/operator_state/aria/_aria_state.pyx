# cython: language_level=3, boundscheck=False, wraparound=False
"""Cython-accelerated hot-path helpers for the Aria concurrency protocol.

These functions replace the pure-Python equivalents in ``BaseAriaState`` and
``InMemoryOperatorState`` for the bookkeeping, conflict-detection, and
state-access methods that dominate epoch processing time.  All operate on
standard Python dicts/sets/lists -- no custom C structs -- so there is zero
marshalling cost.
"""

from worker.operator_state.aria._fast_copy import fast_deepcopy as _fast_deepcopy


# ---------------------------------------------------------------------------
# Inlined deal_with_reads: takes operator_name + partition, builds tuple once
# ---------------------------------------------------------------------------

cpdef void deal_with_reads(
    dict reads,
    dict read_sets,
    object key,
    int t_id,
    tuple operator_partition,
):
    """Record that *t_id* read *key* in *operator_partition*."""
    cdef dict op_reads = <dict>reads[operator_partition]
    cdef dict op_read_sets = <dict>read_sets[operator_partition]
    cdef list key_readers
    cdef set tid_keys

    if key in op_reads:
        key_readers = <list>op_reads[key]
        key_readers.append(t_id)
    else:
        op_reads[key] = [t_id]

    if t_id in op_read_sets:
        tid_keys = <set>op_read_sets[t_id]
        tid_keys.add(key)
    else:
        op_read_sets[t_id] = {key}


cpdef void put_write_sets(
    dict write_sets,
    dict writes,
    object key,
    object value,
    int t_id,
    tuple operator_partition,
):
    """Record that *t_id* wrote *key* = *value* in *operator_partition*."""
    cdef dict op_ws = <dict>write_sets[operator_partition]
    cdef dict op_writes = <dict>writes[operator_partition]
    cdef dict tid_ws
    cdef list key_writers

    if t_id in op_ws:
        tid_ws = <dict>op_ws[t_id]
        tid_ws[key] = value
    else:
        op_ws[t_id] = {key: value}

    if key in op_writes:
        key_writers = <list>op_writes[key]
        key_writers.append(t_id)
    else:
        op_writes[key] = [t_id]


# ---------------------------------------------------------------------------
# Fully-inlined state_get / state_put: eliminate Python method dispatch
# ---------------------------------------------------------------------------

cpdef object state_get(
    dict data,
    dict write_sets,
    dict reads,
    dict read_sets,
    object key,
    int t_id,
    str operator_name,
    int partition,
):
    """Inlined InMemoryOperatorState.get() -- avoids Python method + tuple overhead.

    1. Records the read (deal_with_reads).
    2. Returns write-set value if this t_id wrote the key, else data value.
    3. Returns an isolation copy via fast_deepcopy.
    """
    cdef tuple op = (operator_name, partition)
    cdef dict op_reads = <dict>reads[op]
    cdef dict op_read_sets = <dict>read_sets[op]
    cdef list key_readers
    cdef set tid_keys

    # -- inline deal_with_reads --
    if key in op_reads:
        key_readers = <list>op_reads[key]
        key_readers.append(t_id)
    else:
        op_reads[key] = [t_id]

    if t_id in op_read_sets:
        tid_keys = <set>op_read_sets[t_id]
        tid_keys.add(key)
    else:
        op_read_sets[t_id] = {key}

    # -- read from write set or data --
    cdef dict op_ws = <dict>write_sets[op]
    if t_id in op_ws:
        tid_ws = <dict>op_ws[t_id]
        if key in tid_ws:
            return _fast_deepcopy(tid_ws[key])

    return _fast_deepcopy((<dict>data[op]).get(key))


cpdef void state_put(
    dict write_sets,
    dict writes,
    object key,
    object value,
    int t_id,
    str operator_name,
    int partition,
):
    """Inlined InMemoryOperatorState.put() -- avoids Python method + tuple overhead."""
    cdef tuple op = (operator_name, partition)
    cdef dict op_ws = <dict>write_sets[op]
    cdef dict op_writes = <dict>writes[op]
    cdef dict tid_ws
    cdef list key_writers

    if t_id in op_ws:
        tid_ws = <dict>op_ws[t_id]
        tid_ws[key] = value
    else:
        op_ws[t_id] = {key: value}

    if key in op_writes:
        key_writers = <list>op_writes[key]
        key_writers.append(t_id)
    else:
        op_writes[key] = [t_id]


cpdef object state_get_immediate(
    dict data,
    object fallback_commit_buffer,
    object key,
    int t_id,
    str operator_name,
    int partition,
    dict fallback_read_sets=None,
):
    """Inlined InMemoryOperatorState.get_immediate().

    When *fallback_read_sets* is provided, records the key read for
    rw-set change detection (Aria paper §4.2).
    """
    cdef tuple op = (operator_name, partition)

    # Track fallback read
    if fallback_read_sets is not None:
        if t_id in fallback_read_sets:
            tid_rs = fallback_read_sets[t_id]
            if op in tid_rs:
                (<set>tid_rs[op]).add(key)
            else:
                tid_rs[op] = {key}
        else:
            fallback_read_sets[t_id] = {op: {key}}

    if t_id in fallback_commit_buffer:
        tid_buf = fallback_commit_buffer[t_id]
        if op in tid_buf:
            op_buf = tid_buf[op]
            if key in op_buf:
                return _fast_deepcopy(op_buf[key])

    return _fast_deepcopy((<dict>data[op]).get(key))


cpdef set commit(
    dict write_sets,
    dict data,
    dict delta_map,
    set aborted_from_remote,
):
    """Inlined InMemoryOperatorState.commit()."""
    cdef set committed_t_ids = set()
    cdef tuple op_name
    cdef dict ws
    cdef dict updates_to_commit
    cdef int t_id

    for op_name, ws in write_sets.items():
        updates_to_commit = {}
        for t_id, kv in ws.items():
            if t_id not in aborted_from_remote:
                (<dict>updates_to_commit).update(<dict>kv)
                committed_t_ids.add(t_id)
        (<dict>data[op_name]).update(updates_to_commit)
        (<dict>delta_map[op_name]).update(updates_to_commit)
    return committed_t_ids


# ---------------------------------------------------------------------------
# Conflict detection (unchanged)
# ---------------------------------------------------------------------------

cpdef bint has_conflicts(int t_id, object keys, dict reservations):
    """Return True if any key in *keys* has a reservation with a lower t_id."""
    cdef object key
    cdef object res_tid
    for key in keys:
        if key in reservations:
            res_tid = reservations[key]
            if <int>res_tid < t_id:
                return True
    return False


cpdef dict min_rw_reservations(dict reservations):
    """For each (operator_partition -> key -> [t_ids]), compute the minimum t_id."""
    cdef dict result = {}
    cdef tuple op_part
    cdef dict reservation
    cdef dict new_res
    cdef object key
    cdef list t_ids
    for op_part, reservation in reservations.items():
        new_res = {}
        for key, t_ids in (<dict>reservation).items():
            if t_ids:
                new_res[key] = min(t_ids)
        result[op_part] = new_res
    return result


cpdef set check_conflicts(dict write_sets, dict read_sets, dict writes):
    """Default Aria conflict detection (serializability)."""
    cdef set aborted = set()
    cdef dict minimized_writes = min_rw_reservations(writes)
    cdef tuple op_part
    cdef dict write_set
    cdef dict read_set
    cdef set t_ids
    cdef int t_id
    cdef set rs
    cdef dict ws
    cdef dict min_ws

    for op_part, write_set in write_sets.items():
        read_set = <dict>read_sets[op_part]
        t_ids = set(read_set.keys())
        t_ids.update(write_set.keys())
        min_ws = <dict>minimized_writes[op_part]
        for t_id in t_ids:
            rs = read_set.get(t_id, set())
            ws = write_set.get(t_id, {})
            if has_conflicts(t_id, rs, min_ws) or has_conflicts(t_id, ws, min_ws):
                aborted.add(t_id)
    return aborted


cpdef set check_conflicts_snapshot_isolation(dict write_sets, dict writes):
    """Snapshot-isolation conflict detection (WAW only)."""
    cdef set aborted = set()
    cdef dict minimized_writes = min_rw_reservations(writes)
    cdef tuple op_part
    cdef dict ws_dict
    cdef set t_ids
    cdef int t_id
    cdef dict ws
    cdef dict min_ws

    for op_part in write_sets:
        ws_dict = <dict>write_sets[op_part]
        t_ids = set(ws_dict.keys())
        min_ws = <dict>minimized_writes[op_part]
        for t_id in t_ids:
            ws = ws_dict.get(t_id, set())
            if has_conflicts(t_id, ws, min_ws):
                aborted.add(t_id)
    return aborted


cpdef set check_conflicts_deterministic_reordering(
    dict write_sets,
    dict read_sets,
    dict writes,
    dict global_reads,
    dict global_write_sets,
    dict global_read_sets,
):
    """Deterministic-reordering conflict detection."""
    cdef set aborted = set()
    cdef dict merged_reads = min_rw_reservations(global_reads)
    cdef dict minimized_writes = min_rw_reservations(writes)
    cdef tuple op_name
    cdef dict write_set
    cdef dict read_set
    cdef set t_ids
    cdef int t_id
    cdef object ws
    cdef object rs
    cdef dict min_ws
    cdef dict m_reads

    for op_name in write_sets:
        write_set = <dict>global_write_sets[op_name]
        read_set = <dict>global_read_sets[op_name]
        t_ids = set((<dict>write_sets[op_name]).keys())
        t_ids.update((<dict>read_sets[op_name]).keys())
        min_ws = <dict>minimized_writes[op_name]
        m_reads = <dict>merged_reads[op_name]
        for t_id in t_ids:
            ws = write_set.get(t_id, set())
            if has_conflicts(t_id, ws, min_ws):
                aborted.add(t_id)
                continue
            war = has_conflicts(t_id, ws, m_reads)
            rs = read_set.get(t_id, set())
            raw = has_conflicts(t_id, rs, min_ws)
            if not war or not raw:
                continue
            aborted.add(t_id)
    return aborted


cpdef void remove_aborted_from_rw_sets(
    set operator_partitions,
    dict read_sets,
    dict write_sets,
    dict reads,
    dict writes_dict,
    set global_logic_aborts,
):
    """Remove aborted transaction IDs from all read/write sets and reservations."""
    if not global_logic_aborts:
        return

    cdef tuple op_part
    cdef dict op_dict
    cdef int tid
    cdef object key
    cdef list t_ids
    cdef list new_tids

    for op_part in operator_partitions:
        op_dict = <dict>read_sets[op_part]
        for tid in list(op_dict.keys()):
            if tid in global_logic_aborts:
                del op_dict[tid]

        op_dict = <dict>write_sets[op_part]
        for tid in list(op_dict.keys()):
            if tid in global_logic_aborts:
                del op_dict[tid]

        op_dict = <dict>reads[op_part]
        for key in op_dict:
            t_ids = <list>op_dict[key]
            new_tids = [tid for tid in t_ids if tid not in global_logic_aborts]
            op_dict[key] = new_tids

        op_dict = <dict>writes_dict[op_part]
        for key in op_dict:
            t_ids = <list>op_dict[key]
            new_tids = [tid for tid in t_ids if tid not in global_logic_aborts]
            op_dict[key] = new_tids
