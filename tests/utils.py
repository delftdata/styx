from worker.operator_state.aria.in_memory_state import InMemoryOperatorState


async def commit_state(w1_state: InMemoryOperatorState, w2_state: InMemoryOperatorState, reordering: bool = True):
    if reordering:
        w1_state.merge_rr(w2_state.reads)
        w1_state.merge_ws(w2_state.write_sets)
        w1_state.merge_rs(w2_state.read_sets)
        w2_state.merge_rr(w1_state.reads)
        w2_state.merge_ws(w1_state.write_sets)
        w2_state.merge_rs(w1_state.read_sets)
        w1_conflicts = w1_state.check_conflicts_deterministic_reordering()
        w2_conflicts = w2_state.check_conflicts_deterministic_reordering()
    else:
        w1_conflicts = w1_state.check_conflicts()
        w2_conflicts = w2_state.check_conflicts()
    global_conflicts = w1_conflicts | w2_conflicts
    w1_committed_t_ids = await w1_state.commit(global_conflicts)
    w2_committed_t_ids = await w2_state.commit(global_conflicts)
    w1_state.cleanup()
    w2_state.cleanup()
    return global_conflicts


async def commit_state3(w1_state: InMemoryOperatorState, w2_state: InMemoryOperatorState,
                        w3_state: InMemoryOperatorState, reordering: bool = True):
    if reordering:
        w1_state.merge_rr(w2_state.reads)
        w1_state.merge_ws(w2_state.write_sets)
        w1_state.merge_rs(w2_state.read_sets)

        w1_state.merge_rr(w3_state.reads)
        w1_state.merge_ws(w3_state.write_sets)
        w1_state.merge_rs(w3_state.read_sets)

        w2_state.merge_rr(w1_state.reads)
        w2_state.merge_ws(w1_state.write_sets)
        w2_state.merge_rs(w1_state.read_sets)

        w2_state.merge_rr(w3_state.reads)
        w2_state.merge_ws(w3_state.write_sets)
        w2_state.merge_rs(w3_state.read_sets)

        w3_state.merge_rr(w1_state.reads)
        w3_state.merge_ws(w1_state.write_sets)
        w3_state.merge_rs(w1_state.read_sets)

        w3_state.merge_rr(w2_state.reads)
        w3_state.merge_ws(w2_state.write_sets)
        w3_state.merge_rs(w2_state.read_sets)

        w1_conflicts = w1_state.check_conflicts_deterministic_reordering()
        w2_conflicts = w2_state.check_conflicts_deterministic_reordering()
        w3_conflicts = w3_state.check_conflicts_deterministic_reordering()
    else:
        w1_conflicts = w1_state.check_conflicts()
        w2_conflicts = w2_state.check_conflicts()
        w3_conflicts = w3_state.check_conflicts()
    global_conflicts = w1_conflicts | w2_conflicts | w3_conflicts
    w1_committed_t_ids = await w1_state.commit(global_conflicts)
    w2_committed_t_ids = await w2_state.commit(global_conflicts)
    w3_committed_t_ids = await w3_state.commit(global_conflicts)
    w1_state.cleanup()
    w2_state.cleanup()
    w3_state.cleanup()
    return global_conflicts


async def transaction(k1, k1_state, k2, k2_state, t_id):
    k1_value = await k1_state.get(key=k1, t_id=t_id, operator_name="test")
    await k1_state.put(key=k1, value=k1_value - 1, t_id=t_id, operator_name="test")
    k2_value = await k2_state.get(key=k2, t_id=t_id, operator_name="test")
    await k2_state.put(key=k2, value=k2_value + 1, t_id=t_id, operator_name="test")


async def rerun_conflicts(global_conflicts, transactions):
    for conflict in global_conflicts:
        await transaction(**transactions[conflict])
