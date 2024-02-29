import unittest

from tests.utils import commit_state, transaction, rerun_conflicts, commit_state3
from worker.operator_state.aria.in_memory_state import InMemoryOperatorState

operator_names = {"test"}


class TestState(unittest.IsolatedAsyncioTestCase):

    async def test_state(self):
        state = InMemoryOperatorState(operator_names)
        value_to_put = "value1"
        await state.put(key=1, value=value_to_put, t_id=1, operator_name="test")
        reply = await state.get(key=1, t_id=1, operator_name="test")
        # same t_id reads its own changes
        assert reply == value_to_put
        reply = await state.get(key=1, t_id=2, operator_name="test")
        # different t_id reads snapshot
        # None because t-id = 1 wrote first and not yet committed
        assert reply is None
        await state.commit(set())
        reply = await state.get(key=1, t_id=2, operator_name="test")
        # after commit t_id = 2 should read ti_d = 1 changes
        assert reply == value_to_put
        # test fallback commit
        # tid: {operator_name: {key: value}}
        state.fallback_commit_buffer[2] = {"test": {2: value_to_put}}
        await state.commit_fallback_transaction(2)
        reply = await state.get(key=2, t_id=2, operator_name="test")
        assert reply == value_to_put
        await state.delete(key=2, operator_name="test")
        exists = await state.exists(key=1, operator_name="test")
        assert exists
        exists = await state.exists(key=3, operator_name="test")
        assert not exists
        non_exists_is_none = await state.get(key=3,  t_id=3, operator_name="test")
        assert non_exists_is_none is None

    async def test_reordering(self):
        # example from Aria paper Figure 6
        state = InMemoryOperatorState(operator_names)
        x_key = 1
        y_key = 2
        z_key = 3
        value_to_put = "irrelevant"
        # Top example
        # T1
        await state.get(key=x_key, t_id=1, operator_name="test")
        await state.put(key=y_key, value=value_to_put, t_id=1, operator_name="test")
        # T2
        await state.get(key=y_key, t_id=2, operator_name="test")
        await state.put(key=z_key, value=value_to_put, t_id=2, operator_name="test")
        # T3
        await state.get(key=y_key, t_id=3, operator_name="test")
        await state.get(key=z_key, t_id=3, operator_name="test")
        conflicts = state.check_conflicts()
        assert conflicts == {2, 3}
        conflicts = state.check_conflicts_deterministic_reordering()
        assert conflicts == set()
        # Bottom example
        state = InMemoryOperatorState(operator_names)
        # T1
        await state.get(key=x_key, t_id=1, operator_name="test")
        await state.put(key=y_key, value=value_to_put, t_id=1, operator_name="test")
        # T2
        await state.put(key=x_key, value=value_to_put, t_id=2, operator_name="test")
        await state.get(key=z_key, t_id=2, operator_name="test")
        # T3
        await state.get(key=y_key, t_id=3, operator_name="test")
        await state.put(key=z_key, value=value_to_put, t_id=3, operator_name="test")
        conflicts = state.check_conflicts()
        assert conflicts == {1} or conflicts == {2} or conflicts == {3}
        conflicts = state.check_conflicts_deterministic_reordering()
        assert conflicts == {1} or conflicts == {2} or conflicts == {3}

    async def test_two_workers_ycsb(self):
        # w1 gets the even keys w2 gets the odd
        starting_money: int = 10_000
        w1_state = InMemoryOperatorState(operator_names)
        w2_state = InMemoryOperatorState(operator_names)

        # inserts
        await w1_state.put(key=0, value=starting_money, t_id=1, operator_name="test")
        await w1_state.put(key=2, value=starting_money, t_id=3, operator_name="test")
        await w2_state.put(key=1, value=starting_money, t_id=2, operator_name="test")
        await w2_state.put(key=3, value=starting_money, t_id=4, operator_name="test")
        await commit_state(w1_state, w2_state)
        # transaction mix
        transactions = {6: {'k1': 1, 'k1_state': w2_state, 'k2': 2, 'k2_state': w1_state, 't_id': 6},
                        7: {'k1': 0, 'k1_state': w1_state, 'k2': 1, 'k2_state': w2_state, 't_id': 7},
                        8: {'k1': 1, 'k1_state': w2_state, 'k2': 3, 'k2_state': w2_state, 't_id': 8},
                        9: {'k1': 0, 'k1_state': w1_state, 'k2': 1, 'k2_state': w2_state, 't_id': 9},
                        10: {'k1': 0, 'k1_state': w1_state, 'k2': 2, 'k2_state': w1_state, 't_id': 10},
                        11: {'k1': 3, 'k1_state': w2_state, 'k2': 0, 'k2_state': w1_state, 't_id': 11}}
        # transfer from key 1: 9999 to 2: 10001
        await transaction(**transactions[6])
        # transfer from key 0: 9999 to 1 10000
        await transaction(**transactions[7])
        global_conflicts = await commit_state(w1_state, w2_state)
        await rerun_conflicts(global_conflicts, transactions)
        await commit_state(w1_state, w2_state)
        assert w1_state.data == {'test': {0: 9999, 2: 10001}}
        assert w2_state.data == {'test': {1: 10000, 3: 10000}}
        # transfer from key 1: 9999 to 3: 10001
        await transaction(**transactions[8])
        # transfer from key 0: 9998 to 1: 10000
        await transaction(**transactions[9])
        # transfer from key 0: 9997 to 2: 10002
        await transaction(**transactions[10])
        # transfer from key 3: 10000  to 0: 9998
        await transaction(**transactions[11])
        global_conflicts = await commit_state(w1_state, w2_state)
        while global_conflicts:
            await rerun_conflicts(global_conflicts, transactions)
            global_conflicts = await commit_state(w1_state, w2_state)
        assert global_conflicts == set()
        assert w1_state.data == {'test': {0: 9998, 2: 10002}}
        assert w2_state.data == {'test': {1: 10000, 3: 10000}}

    async def test_two_workers_ycsb_2(self):
        # w1 gets the even keys w2 gets the odd
        starting_money: int = 100
        w1_state = InMemoryOperatorState(operator_names)
        w2_state = InMemoryOperatorState(operator_names)

        # inserts
        await w1_state.put(key=0, value=starting_money, t_id=1, operator_name="test")
        await w1_state.put(key=2, value=starting_money, t_id=3, operator_name="test")
        await w2_state.put(key=1, value=starting_money, t_id=2, operator_name="test")
        await w2_state.put(key=3, value=starting_money, t_id=4, operator_name="test")
        await commit_state(w1_state, w2_state)
        # transaction mix
        transactions = {5: {'k1': 2, 'k1_state': w1_state, 'k2': 0, 'k2_state': w1_state, 't_id': 5},
                        6: {'k1': 3, 'k1_state': w2_state, 'k2': 2, 'k2_state': w1_state, 't_id': 6},
                        7: {'k1': 0, 'k1_state': w1_state, 'k2': 2, 'k2_state': w1_state, 't_id': 7},
                        8: {'k1': 3, 'k1_state': w2_state, 'k2': 1, 'k2_state': w2_state, 't_id': 8}}

        await transaction(**transactions[5])
        await transaction(**transactions[6])
        await transaction(**transactions[7])
        await transaction(**transactions[8])
        await commit_state(w1_state, w2_state)

    async def test_write_after_write_conflict_in_deterministic_reordering(self):
        # T1 and T2 write to the same key, only one should be allowed.
        state = InMemoryOperatorState(operator_names)
        x_key = 1
        value_to_put = "irrelevant"

        # Top example
        # T1
        await state.put(key=x_key, value=value_to_put, t_id=1, operator_name="test")
        # T2
        await state.put(key=x_key, value=value_to_put, t_id=2, operator_name="test")

        conflicts = state.check_conflicts_deterministic_reordering()
        assert conflicts == {2}

    async def test_write_skew_one_worker(self):
        # w1 gets the even keys w2 gets the odd
        starting_money: int = 1
        w1_state = InMemoryOperatorState(operator_names)

        # inserts
        await w1_state.put(key=0, value=starting_money, t_id=1, operator_name="test")
        await w1_state.put(key=1, value=starting_money, t_id=2, operator_name="test")
        conflicts = w1_state.check_conflicts()
        assert conflicts == set()
        await w1_state.commit(set())
        w1_state.cleanup()

        # T_3 only updating if both accounts contain more than 1. Write to key 0.
        x = await w1_state.get(key=0, t_id=3, operator_name="test")
        y = await w1_state.get(key=1, t_id=3, operator_name="test")
        if x + y > 1:
            await w1_state.put(key=0, value=x - 1, t_id=3, operator_name="test")

        # T_4 only updating if both accounts contain more than 1. Write to key 1.
        x = await w1_state.get(key=0, t_id=4, operator_name="test")
        y = await w1_state.get(key=1, t_id=4, operator_name="test")
        if x + y > 1:
            await w1_state.put(key=1, value=y - 1, t_id=4, operator_name="test")

        conflicts = w1_state.check_conflicts_deterministic_reordering()
        committed_t_ids = await w1_state.commit(conflicts)
        assert conflicts == {4}
        assert committed_t_ids == {3}
        w1_state.cleanup()

        # check results
        x = await w1_state.get(key=0, t_id=5, operator_name="test")
        y = await w1_state.get(key=1, t_id=5, operator_name="test")

        assert x == 0
        assert y == 1

    async def test_write_skew_two_workers(self):
        # This found a bug with the war check in two workers
        # w1 gets the even keys w2 gets the odd
        starting_money: int = 1
        w1_state = InMemoryOperatorState(operator_names)
        w2_state = InMemoryOperatorState(operator_names)

        # inserts
        await w1_state.put(key=0, value=starting_money, t_id=1, operator_name="test")
        await w2_state.put(key=1, value=starting_money, t_id=2, operator_name="test")
        await commit_state(w1_state, w2_state)

        # T_3 only updating if both accounts contain more than 1. Write to key 0.
        x = await w1_state.get(key=0, t_id=3, operator_name="test")
        y = await w2_state.get(key=1, t_id=3, operator_name="test")
        if x + y > 1:
            await w1_state.put(key=0, value=x-1, t_id=3, operator_name="test")

        # T_4 only updating if both accounts contain more than 1. Write to key 1.
        x = await w1_state.get(key=0, t_id=4, operator_name="test")
        y = await w2_state.get(key=1, t_id=4, operator_name="test")
        if x + y > 1:
            await w2_state.put(key=1, value=y-1, t_id=4, operator_name="test")

        global_conflicts = await commit_state(w1_state, w2_state, reordering=True)

        assert global_conflicts != set()

        # check results
        x = await w1_state.get(key=0, t_id=5, operator_name="test")
        y = await w2_state.get(key=1, t_id=5, operator_name="test")
        assert x == 0
        assert y == 1

    async def test_write_raw_two_workers(self):
        # w1 gets the even keys w2 gets the odd
        starting_money: int = 1
        w1_state = InMemoryOperatorState(operator_names)
        w2_state = InMemoryOperatorState(operator_names)

        # inserts
        await w1_state.put(key=0, value=starting_money, t_id=1, operator_name="test")
        await w2_state.put(key=1, value=starting_money, t_id=2, operator_name="test")
        await commit_state(w1_state, w2_state)

        # T_3 only updating if both accounts contain more than 1. Write to key 0.
        # x = await w1_state.get(key=0, t_id=3, operator_name="test")
        # y = await w2_state.get(key=1, t_id=3, operator_name="test")
        # if x + y > 1:
        await w1_state.put(key=0, value=10, t_id=3, operator_name="test")
        await w2_state.put(key=1, value=10, t_id=3, operator_name="test")

        # T_4 only updating if both accounts contain more than 1. Write to key 1.
        x = await w1_state.get(key=0, t_id=4, operator_name="test")
        y = await w2_state.get(key=1, t_id=4, operator_name="test")
        assert x == starting_money
        assert y == starting_money

        global_conflicts = await commit_state(w1_state, w2_state, reordering=True)

        assert global_conflicts == set()

    async def test_aria_reordering_example_with_three_workers(self):
        # example from Aria paper Figure 6
        x_state = InMemoryOperatorState(operator_names)
        y_state = InMemoryOperatorState(operator_names)
        z_state = InMemoryOperatorState(operator_names)
        x_key = 1
        y_key = 2
        z_key = 3
        value_to_put = "irrelevant"
        # Top example
        # T1
        await x_state.get(key=x_key, t_id=1, operator_name="test")
        await y_state.put(key=y_key, value=value_to_put, t_id=1, operator_name="test")
        # T2
        await y_state.get(key=y_key, t_id=2, operator_name="test")
        await z_state.put(key=z_key, value=value_to_put, t_id=2, operator_name="test")
        # T3
        await y_state.get(key=y_key, t_id=3, operator_name="test")
        await z_state.get(key=z_key, t_id=3, operator_name="test")
        x_conflicts = x_state.check_conflicts()
        y_conflicts = y_state.check_conflicts()
        z_conflicts = z_state.check_conflicts()
        conflicts = x_conflicts | y_conflicts | z_conflicts
        assert conflicts == {2, 3}
        x_state.merge_rr(y_state.reads)
        x_state.merge_ws(y_state.write_sets)
        x_state.merge_rr(z_state.reads)
        x_state.merge_ws(z_state.write_sets)
        y_state.merge_rr(x_state.reads)
        y_state.merge_ws(x_state.write_sets)
        y_state.merge_rr(z_state.reads)
        y_state.merge_ws(z_state.write_sets)
        z_state.merge_rr(x_state.reads)
        z_state.merge_ws(x_state.write_sets)
        z_state.merge_rr(y_state.reads)
        z_state.merge_ws(y_state.write_sets)
        x_conflicts = x_state.check_conflicts_deterministic_reordering()
        y_conflicts = y_state.check_conflicts_deterministic_reordering()
        z_conflicts = z_state.check_conflicts_deterministic_reordering()
        conflicts = x_conflicts | y_conflicts | z_conflicts
        assert conflicts == set()
        # Bottom example
        x_state = InMemoryOperatorState(operator_names)
        y_state = InMemoryOperatorState(operator_names)
        z_state = InMemoryOperatorState(operator_names)
        # T1
        await x_state.get(key=x_key, t_id=1, operator_name="test")
        await y_state.put(key=y_key, value=value_to_put, t_id=1, operator_name="test")
        # T2
        await x_state.put(key=x_key, value=value_to_put, t_id=2, operator_name="test")
        await z_state.get(key=z_key, t_id=2, operator_name="test")
        # T3
        await y_state.get(key=y_key, t_id=3, operator_name="test")
        await z_state.put(key=z_key, value=value_to_put, t_id=3, operator_name="test")
        x_conflicts = x_state.check_conflicts()
        y_conflicts = y_state.check_conflicts()
        z_conflicts = z_state.check_conflicts()
        conflicts = x_conflicts | y_conflicts | z_conflicts
        assert conflicts == {1} or conflicts == {2} or conflicts == {3}
        x_state.merge_rr(y_state.reads)
        x_state.merge_ws(y_state.write_sets)
        x_state.merge_rr(z_state.reads)
        x_state.merge_ws(z_state.write_sets)
        y_state.merge_rr(x_state.reads)
        y_state.merge_ws(x_state.write_sets)
        y_state.merge_rr(z_state.reads)
        y_state.merge_ws(z_state.write_sets)
        z_state.merge_rr(x_state.reads)
        z_state.merge_ws(x_state.write_sets)
        z_state.merge_rr(y_state.reads)
        z_state.merge_ws(y_state.write_sets)
        x_conflicts = x_state.check_conflicts_deterministic_reordering()
        y_conflicts = y_state.check_conflicts_deterministic_reordering()
        z_conflicts = z_state.check_conflicts_deterministic_reordering()
        conflicts = x_conflicts | y_conflicts | z_conflicts
        assert conflicts == {1} or conflicts == {2} or conflicts == {3}

    async def test_write_skew_three_workers(self):
        # w1 gets the even keys w2 gets the odd
        starting_money: int = 1
        w1_state = InMemoryOperatorState(operator_names)
        w2_state = InMemoryOperatorState(operator_names)
        w3_state = InMemoryOperatorState(operator_names)

        # inserts
        await w1_state.put(key=0, value=starting_money, t_id=1, operator_name="test")
        await w2_state.put(key=1, value=starting_money, t_id=2, operator_name="test")
        await w3_state.put(key=2, value=starting_money, t_id=3, operator_name="test")
        await commit_state3(w1_state, w2_state, w3_state)

        # T_3 only updating if both accounts contain more than 1. Write to key 0.
        x = await w1_state.get(key=0, t_id=4, operator_name="test")
        y = await w2_state.get(key=1, t_id=4, operator_name="test")
        z = await w3_state.get(key=2, t_id=4, operator_name="test")
        if x + y + z > 2:
            await w1_state.put(key=0, value=x-1, t_id=4, operator_name="test")

        # T_4 only updating if both accounts contain more than 1. Write to key 1.
        x = await w1_state.get(key=0, t_id=5, operator_name="test")
        y = await w2_state.get(key=1, t_id=5, operator_name="test")
        z = await w3_state.get(key=2, t_id=5, operator_name="test")
        if x + y + z > 2:
            await w2_state.put(key=1, value=y-1, t_id=5, operator_name="test")

        x = await w1_state.get(key=0, t_id=6, operator_name="test")
        y = await w2_state.get(key=1, t_id=6, operator_name="test")
        z = await w3_state.get(key=2, t_id=6, operator_name="test")
        if x + y + z > 2:
            await w3_state.put(key=2, value=z-1, t_id=6, operator_name="test")

        global_conflicts = await commit_state3(w1_state, w2_state, w3_state, reordering=True)

        assert global_conflicts != set()

        # check results
        x = await w1_state.get(key=0, t_id=5, operator_name="test")
        y = await w2_state.get(key=1, t_id=5, operator_name="test")
        z = await w3_state.get(key=2, t_id=5, operator_name="test")
        assert x == 0
        assert y == 1
        assert z == 1

    async def test_logic_aborts(self):
        state = InMemoryOperatorState(operator_names)
        value_to_put = "irrelevant"
        await state.put(key=0, value=value_to_put, t_id=1, operator_name="test")
        await state.put(key=1, value=value_to_put, t_id=1, operator_name="test")
        await state.commit(set())
        state.cleanup()

        await state.get(key=0, t_id=2, operator_name="test")
        await state.get(key=1, t_id=2, operator_name="test")
        await state.put(key=0, value="T2", t_id=2, operator_name="test")
        await state.put(key=1, value="T2", t_id=2, operator_name="test")

        await state.get(key=0, t_id=3, operator_name="test")
        await state.get(key=1, t_id=3, operator_name="test")
        await state.put(key=0, value="T3", t_id=3, operator_name="test")
        await state.put(key=1, value="T3", t_id=3, operator_name="test")

        await state.get(key=0, t_id=4, operator_name="test")
        await state.get(key=1, t_id=4, operator_name="test")
        await state.put(key=0, value="T4", t_id=4, operator_name="test")
        await state.put(key=1, value="T4", t_id=4, operator_name="test")

        logic_aborts = {2, 4}

        state.remove_aborted_from_rw_sets(logic_aborts)

        conflicts = state.check_conflicts()
        assert conflicts == set()
        conflicts = state.check_conflicts_deterministic_reordering()
        assert conflicts == set()

        state.cleanup()

        await state.get(key=0, t_id=2, operator_name="test")
        await state.get(key=1, t_id=2, operator_name="test")
        await state.put(key=0, value="T2", t_id=2, operator_name="test")
        await state.put(key=1, value="T2", t_id=2, operator_name="test")

        await state.get(key=0, t_id=3, operator_name="test")
        await state.get(key=1, t_id=3, operator_name="test")
        await state.put(key=0, value="T3", t_id=3, operator_name="test")
        await state.put(key=1, value="T3", t_id=3, operator_name="test")

        await state.get(key=0, t_id=4, operator_name="test")
        await state.get(key=1, t_id=4, operator_name="test")
        await state.put(key=0, value="T4", t_id=4, operator_name="test")
        await state.put(key=1, value="T4", t_id=4, operator_name="test")

        logic_aborts = {2}
        state.remove_aborted_from_rw_sets(logic_aborts)

        conflicts = state.check_conflicts()
        assert conflicts == {4}
        conflicts = state.check_conflicts_deterministic_reordering()
        assert conflicts == {4}

        state.cleanup()

        await state.get(key=0, t_id=2, operator_name="test")
        await state.get(key=1, t_id=2, operator_name="test")
        await state.put(key=0, value="T2", t_id=2, operator_name="test")
        await state.put(key=1, value="T2", t_id=2, operator_name="test")

        await state.get(key=0, t_id=3, operator_name="test")
        await state.get(key=1, t_id=3, operator_name="test")
        await state.put(key=0, value="T3", t_id=3, operator_name="test")
        await state.put(key=1, value="T3", t_id=3, operator_name="test")

        await state.get(key=0, t_id=4, operator_name="test")
        await state.get(key=1, t_id=4, operator_name="test")
        await state.put(key=0, value="T4", t_id=4, operator_name="test")
        await state.put(key=1, value="T4", t_id=4, operator_name="test")

        logic_aborts = {4}
        state.remove_aborted_from_rw_sets(logic_aborts)

        conflicts = state.check_conflicts()
        assert conflicts == {3}
        conflicts = state.check_conflicts_deterministic_reordering()
        assert conflicts == {3}

    async def test_two_workers_ycsb_fallback(self):
        # w1 gets the even keys w2 gets the odd
        starting_money: int = 10_000
        w1_state = InMemoryOperatorState(operator_names)
        w2_state = InMemoryOperatorState(operator_names)

        # inserts
        await w1_state.put(key=0, value=starting_money, t_id=1, operator_name="test")
        await w1_state.put(key=2, value=starting_money, t_id=3, operator_name="test")
        await w2_state.put(key=1, value=starting_money, t_id=2, operator_name="test")
        await w2_state.put(key=3, value=starting_money, t_id=4, operator_name="test")
        await commit_state(w1_state, w2_state)
        # transaction mix
        transactions = {6: {'k1': 1, 'k1_state': w2_state, 'k2': 2, 'k2_state': w1_state, 't_id': 6},
                        7: {'k1': 0, 'k1_state': w1_state, 'k2': 1, 'k2_state': w2_state, 't_id': 7},
                        8: {'k1': 1, 'k1_state': w2_state, 'k2': 3, 'k2_state': w2_state, 't_id': 8},
                        9: {'k1': 0, 'k1_state': w1_state, 'k2': 1, 'k2_state': w2_state, 't_id': 9},
                        10: {'k1': 0, 'k1_state': w1_state, 'k2': 2, 'k2_state': w1_state, 't_id': 10},
                        11: {'k1': 3, 'k1_state': w2_state, 'k2': 0, 'k2_state': w1_state, 't_id': 11}}
        # transfer from key 1: 9999 to 2: 10001
        await transaction(**transactions[6])
        # transfer from key 0: 9999 to 1 10000
        await transaction(**transactions[7])

        await commit_state(w1_state, w2_state)
        # 7 aborted rerun with fallback
        v7 = await w1_state.get_immediate(0, 7, "test")
        await w1_state.put_immediate(0, v7 - 1, 7, "test")
        v7 = await w2_state.get_immediate(1, 7, "test")
        await w2_state.put_immediate(1, v7 + 1, 7, "test")

        await w1_state.commit_fallback_transaction(7)
        await w2_state.commit_fallback_transaction(7)

        assert w1_state.data == {'test': {0: 9999, 2: 10001}}
        assert w2_state.data == {'test': {1: 10000, 3: 10000}}
        # transfer from key 1: 9999 to 3: 10001
        await transaction(**transactions[8])
        # transfer from key 0: 9998 to 1: 10000
        await transaction(**transactions[9])
        # transfer from key 0: 9997 to 2: 10002
        await transaction(**transactions[10])
        # transfer from key 3: 10000  to 0: 9998
        await transaction(**transactions[11])

        await commit_state(w1_state, w2_state)

        v9 = await w1_state.get_immediate(0, 9, "test")
        await w1_state.put_immediate(0, v9 - 1, 9, "test")
        v9 = await w2_state.get_immediate(1, 9, "test")
        await w2_state.put_immediate(1, v9 + 1, 9, "test")

        await w1_state.commit_fallback_transaction(9)
        await w2_state.commit_fallback_transaction(9)

        v10 = await w1_state.get_immediate(0, 10, "test")
        await w1_state.put_immediate(0, v10 - 1, 10, "test")
        v10 = await w1_state.get_immediate(2, 10, "test")
        await w1_state.put_immediate(2, v10 + 1, 10, "test")

        await w1_state.commit_fallback_transaction(10)
        await w2_state.commit_fallback_transaction(10)

        v11 = await w2_state.get_immediate(3, 11, "test")
        await w2_state.put_immediate(3, v11 - 1, 11, "test")
        v11 = await w1_state.get_immediate(0, 11, "test")
        await w1_state.put_immediate(0, v11 + 1, 11, "test")

        await w1_state.commit_fallback_transaction(11)
        await w2_state.commit_fallback_transaction(11)

        assert w1_state.data == {'test': {0: 9998, 2: 10002}}
        assert w2_state.data == {'test': {1: 10000, 3: 10000}}

    async def test_write_after_write_conflict_in_deterministic_reordering(self):
        # T1 and T2 write to the same key, only one should be allowed.
        state = InMemoryOperatorState(operator_names)
        x_key = 1
        value_to_put = "irrelevant"

        # Top example
        # T1
        await state.put(key=x_key, value=value_to_put, t_id=1, operator_name="test")
        # T2
        await state.put(key=x_key, value=value_to_put, t_id=2, operator_name="test")

        conflicts = state.check_conflicts_deterministic_reordering()
        assert conflicts == {2}
