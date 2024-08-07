from worker.operator_state.aria.in_memory_state import InMemoryOperatorState


def merge_rw_sets(d1: dict[str, dict[any, set[any] | dict[any, any]]],
                  d2: dict[str, dict[any, set[any] | dict[any, any]]]
                  ) -> dict[str, dict[any, set[any] | dict[any, any]]]:
    output_dict: dict[str, dict[any, set[any] | dict[any, any]]] = {}
    namespaces: set[str] = set(d1.keys()) | set(d2.keys())
    for namespace in namespaces:
        output_dict[namespace] = {}
        if namespace in d1 and namespace in d2:
            t_ids = set(d1[namespace].keys()) | set(d2[namespace].keys())
            for t_id in t_ids:
                if t_id in d1[namespace] and t_id in d2[namespace]:
                    output_dict[namespace][t_id] = d1[namespace][t_id] | d2[namespace][t_id]
                elif t_id not in d1[namespace]:
                    output_dict[namespace][t_id] = d2[namespace][t_id]
                else:
                    output_dict[namespace][t_id] = d1[namespace][t_id]
        elif namespace in d1 and namespace not in d2:
            output_dict[namespace] = d1[namespace]
        elif namespace not in d1 and namespace in d2:
            output_dict[namespace] = d2[namespace]
    return output_dict


def merge_rw_reservations(d1: dict[str, dict[any, list[int]]],
                          d2: dict[str, dict[any, list[int]]]
                          ) -> dict[str, dict[any, list[int]]]:
    output_dict: dict[str, dict[any, list[int]]] = {}
    namespaces: set[str] = set(d1.keys()) | set(d2.keys())
    for namespace in namespaces:
        output_dict[namespace] = {}
        if namespace in d1 and namespace in d2:
            keys = set(d1[namespace].keys()) | set(d2[namespace].keys())
            for key in keys:
                output_dict[namespace][key] = d1[namespace].get(key, []) + d2[namespace].get(key, [])
        elif namespace in d1 and namespace not in d2:
            output_dict[namespace] = d1[namespace]
        elif namespace not in d1 and namespace in d2:
            output_dict[namespace] = d2[namespace]
    return output_dict


def commit_state(w1_state: InMemoryOperatorState, w2_state: InMemoryOperatorState, reordering: bool = True):
    if reordering:
        global_reads = merge_rw_reservations(w1_state.reads, w2_state.reads)
        global_write_sets = merge_rw_sets(w1_state.write_sets, w2_state.write_sets)
        global_read_sets = merge_rw_sets(w1_state.read_sets, w2_state.read_sets)
        w1_state.set_global_read_write_sets(global_reads, global_write_sets, global_read_sets)
        w2_state.set_global_read_write_sets(global_reads, global_write_sets, global_read_sets)
        w1_conflicts = w1_state.check_conflicts_deterministic_reordering()
        w2_conflicts = w2_state.check_conflicts_deterministic_reordering()
    else:
        w1_conflicts = w1_state.check_conflicts()
        w2_conflicts = w2_state.check_conflicts()
    global_conflicts = w1_conflicts | w2_conflicts
    w1_committed_t_ids = w1_state.commit(global_conflicts)
    w2_committed_t_ids = w2_state.commit(global_conflicts)
    w1_state.cleanup()
    w2_state.cleanup()
    return global_conflicts


def commit_state3(w1_state: InMemoryOperatorState, w2_state: InMemoryOperatorState,
                        w3_state: InMemoryOperatorState, reordering: bool = True):
    if reordering:
        global_reads = merge_rw_reservations(merge_rw_reservations(w1_state.reads, w2_state.reads), w3_state.reads)
        global_write_sets = merge_rw_sets(merge_rw_sets(w1_state.write_sets, w2_state.write_sets), w3_state.write_sets)
        global_read_sets = merge_rw_sets(merge_rw_sets(w1_state.read_sets, w2_state.read_sets), w3_state.read_sets)
        w1_state.set_global_read_write_sets(global_reads, global_write_sets, global_read_sets)
        w2_state.set_global_read_write_sets(global_reads, global_write_sets, global_read_sets)
        w3_state.set_global_read_write_sets(global_reads, global_write_sets, global_read_sets)
        w1_conflicts = w1_state.check_conflicts_deterministic_reordering()
        w2_conflicts = w2_state.check_conflicts_deterministic_reordering()
        w3_conflicts = w3_state.check_conflicts_deterministic_reordering()
    else:
        w1_conflicts = w1_state.check_conflicts()
        w2_conflicts = w2_state.check_conflicts()
        w3_conflicts = w3_state.check_conflicts()
    global_conflicts = w1_conflicts | w2_conflicts | w3_conflicts
    w1_committed_t_ids = w1_state.commit(global_conflicts)
    w2_committed_t_ids = w2_state.commit(global_conflicts)
    w3_committed_t_ids = w3_state.commit(global_conflicts)
    w1_state.cleanup()
    w2_state.cleanup()
    w3_state.cleanup()
    return global_conflicts


def transaction(k1, k1_state, k2, k2_state, t_id):
    k1_value = k1_state.get(key=k1, t_id=t_id, operator_name="test")
    k1_state.put(key=k1, value=k1_value - 1, t_id=t_id, operator_name="test")
    k2_value = k2_state.get(key=k2, t_id=t_id, operator_name="test")
    k2_state.put(key=k2, value=k2_value + 1, t_id=t_id, operator_name="test")


def rerun_conflicts(global_conflicts, transactions):
    for conflict in global_conflicts:
        transaction(**transactions[conflict])
