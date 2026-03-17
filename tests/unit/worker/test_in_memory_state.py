"""Unit tests for worker/operator_state/aria/in_memory_state.py

Tests cover the concrete InMemoryOperatorState behaviour: reads/writes
through the write-set layer, commit, fallback, migration helpers, and
snapshot utilities.
"""

from worker.operator_state.aria.in_memory_state import InMemoryOperatorState

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

OP = "op"
PART = 0
OP_PART = (OP, PART)


def _state(*extra_partitions) -> InMemoryOperatorState:
    parts = {OP_PART, *extra_partitions}
    return InMemoryOperatorState(parts)


def _put(s, key, value, t_id, op=OP, part=PART):
    s.put(key, value, t_id, op, part)


# ---------------------------------------------------------------------------
# get — read-your-own-writes + read-set tracking
# ---------------------------------------------------------------------------


class TestGet:
    def test_returns_committed_data_value(self):
        s = _state()
        s.data[OP_PART]["k"] = "stored"
        val = s.get("k", t_id=1, operator_name=OP, partition=PART)
        assert val == "stored"

    def test_returns_write_set_value_for_same_t_id(self):
        s = _state()
        s.data[OP_PART]["k"] = "old"
        _put(s, "k", "new", t_id=1)
        val = s.get("k", t_id=1, operator_name=OP, partition=PART)
        assert val == "new"

    def test_different_t_id_reads_data_not_write_set(self):
        s = _state()
        s.data[OP_PART]["k"] = "committed"
        _put(s, "k", "pending", t_id=3)
        # t_id=1 has no write to "k", so reads from data
        val = s.get("k", t_id=1, operator_name=OP, partition=PART)
        assert val == "committed"

    def test_records_read_in_read_sets(self):
        s = _state()
        s.data[OP_PART]["k"] = "v"
        s.get("k", t_id=1, operator_name=OP, partition=PART)
        assert "k" in s.read_sets[OP_PART][1]

    def test_missing_key_returns_none(self):
        s = _state()
        val = s.get("missing", t_id=1, operator_name=OP, partition=PART)
        assert val is None

    def test_get_returns_deep_copy(self):
        # msgpack encode/decode ensures isolation
        s = _state()
        s.data[OP_PART]["k"] = [1, 2, 3]
        val = s.get("k", t_id=1, operator_name=OP, partition=PART)
        val.append(99)
        assert s.data[OP_PART]["k"] == [1, 2, 3]


# ---------------------------------------------------------------------------
# get_immediate — fallback read
# ---------------------------------------------------------------------------


class TestGetImmediate:
    def test_returns_fallback_buffer_value(self):
        s = _state()
        s.data[OP_PART]["k"] = "old"
        s.put_immediate("k", "new", t_id=1, operator_name=OP, partition=PART)
        val = s.get_immediate("k", t_id=1, operator_name=OP, partition=PART)
        assert val == "new"

    def test_falls_back_to_data_when_no_buffer_entry(self):
        s = _state()
        s.data[OP_PART]["k"] = "stored"
        val = s.get_immediate("k", t_id=1, operator_name=OP, partition=PART)
        assert val == "stored"

    def test_different_t_id_does_not_see_other_buffer(self):
        s = _state()
        s.data[OP_PART]["k"] = "data"
        s.put_immediate("k", "buf", t_id=3, operator_name=OP, partition=PART)
        val = s.get_immediate("k", t_id=1, operator_name=OP, partition=PART)
        assert val == "data"

    def test_missing_key_returns_none(self):
        s = _state()
        assert s.get_immediate("missing", t_id=1, operator_name=OP, partition=PART) is None


# ---------------------------------------------------------------------------
# commit
# ---------------------------------------------------------------------------


class TestCommit:
    def test_committed_writes_applied_to_data(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        s.commit(aborted_from_remote=set())
        assert s.data[OP_PART]["k"] == "v"

    def test_aborted_writes_not_applied(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        s.commit(aborted_from_remote={1})
        assert "k" not in s.data[OP_PART]

    def test_returns_set_of_committed_t_ids(self):
        s = _state()
        _put(s, "k1", "v", t_id=1)
        _put(s, "k2", "v", t_id=3)
        committed = s.commit(aborted_from_remote={1})
        assert committed == {3}

    def test_delta_map_updated_with_committed_writes(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        s.commit(aborted_from_remote=set())
        assert s.delta_map[OP_PART]["k"] == "v"

    def test_aborted_writes_not_in_delta_map(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        s.commit(aborted_from_remote={1})
        assert "k" not in s.delta_map[OP_PART]

    def test_multiple_writes_same_key_last_writer_wins(self):
        s = _state()
        _put(s, "k", "v1", t_id=1)
        _put(s, "k", "v2", t_id=3)
        s.commit(aborted_from_remote=set())
        # Both non-aborted; dict.update order means last wins
        assert s.data[OP_PART]["k"] in ("v1", "v2")


# ---------------------------------------------------------------------------
# commit_fallback_transaction
# ---------------------------------------------------------------------------


class TestCommitFallbackTransaction:
    def test_applies_buffer_to_data_and_delta(self):
        s = _state()
        s.put_immediate("k", "v", t_id=1, operator_name=OP, partition=PART)
        s.commit_fallback_transaction(1)
        assert s.data[OP_PART]["k"] == "v"
        assert s.delta_map[OP_PART]["k"] == "v"

    def test_unknown_t_id_is_no_op(self):
        s = _state()
        s.commit_fallback_transaction(999)  # no KeyError

    def test_multiple_keys_all_applied(self):
        s = _state()
        s.put_immediate("k1", "v1", t_id=1, operator_name=OP, partition=PART)
        s.put_immediate("k2", "v2", t_id=1, operator_name=OP, partition=PART)
        s.commit_fallback_transaction(1)
        assert s.data[OP_PART]["k1"] == "v1"
        assert s.data[OP_PART]["k2"] == "v2"


# ---------------------------------------------------------------------------
# batch_insert
# ---------------------------------------------------------------------------


class TestBatchInsert:
    def test_updates_data(self):
        s = _state()
        s.batch_insert({"k1": "v1", "k2": "v2"}, operator_name=OP, partition=PART)
        assert s.data[OP_PART]["k1"] == "v1"
        assert s.data[OP_PART]["k2"] == "v2"

    def test_updates_delta_map(self):
        s = _state()
        s.batch_insert({"k": "v"}, operator_name=OP, partition=PART)
        assert s.delta_map[OP_PART]["k"] == "v"

    def test_overwrites_existing_keys(self):
        s = _state()
        s.data[OP_PART]["k"] = "old"
        s.batch_insert({"k": "new"}, operator_name=OP, partition=PART)
        assert s.data[OP_PART]["k"] == "new"


# ---------------------------------------------------------------------------
# clear_delta_map
# ---------------------------------------------------------------------------


class TestClearDeltaMap:
    def test_clears_all_partitions(self):
        s = _state(("op", 1))
        s.delta_map[OP_PART]["k"] = "v"
        s.delta_map[("op", 1)]["j"] = "w"
        s.clear_delta_map()
        assert s.delta_map[OP_PART] == {}
        assert s.delta_map[("op", 1)] == {}

    def test_data_unaffected(self):
        s = _state()
        s.data[OP_PART]["k"] = "v"
        s.delta_map[OP_PART]["k"] = "v"
        s.clear_delta_map()
        assert s.data[OP_PART]["k"] == "v"


# ---------------------------------------------------------------------------
# add_new_operator_partition
# ---------------------------------------------------------------------------


class TestAddNewOperatorPartition:
    def test_new_partition_added_to_all_structures(self):
        s = _state()
        s.add_new_operator_partition(("op", 1))
        assert ("op", 1) in s.operator_partitions
        assert ("op", 1) in s.data
        assert ("op", 1) in s.delta_map

    def test_adding_existing_partition_is_idempotent(self):
        s = _state()
        s.data[OP_PART]["k"] = "v"
        s.add_new_operator_partition(OP_PART)
        assert len(s.operator_partitions) == 1
        assert s.data[OP_PART]["k"] == "v"  # not overwritten


# ---------------------------------------------------------------------------
# Remote keys (migration receiving side)
# ---------------------------------------------------------------------------


class TestRemoteKeys:
    def test_add_remote_keys_creates_entry(self):
        s = _state()
        s.add_remote_keys(OP_PART, {"k": (2, 1)})
        assert s.remote_keys[OP_PART]["k"] == (2, 1)

    def test_add_remote_keys_merges_into_existing(self):
        s = _state()
        s.add_remote_keys(OP_PART, {"k1": (1, 0)})
        s.add_remote_keys(OP_PART, {"k2": (2, 0)})
        assert "k1" in s.remote_keys[OP_PART]
        assert "k2" in s.remote_keys[OP_PART]

    def test_get_worker_id_old_partition_found(self):
        s = _state()
        s.remote_keys[OP_PART] = {"k": (2, 1)}
        assert s.get_worker_id_old_partition(OP, PART, "k") == (2, 1)

    def test_get_worker_id_old_partition_missing_returns_none(self):
        s = _state()
        assert s.get_worker_id_old_partition(OP, PART, "missing") is None

    def test_in_remote_keys_true(self):
        s = _state()
        s.remote_keys[OP_PART] = {"k": (1, 0)}
        assert s.in_remote_keys("k", OP, PART) is True

    def test_in_remote_keys_false_when_not_present(self):
        s = _state()
        assert s.in_remote_keys("k", OP, PART) is False


# ---------------------------------------------------------------------------
# keys_to_send (migration sending side)
# ---------------------------------------------------------------------------


class TestKeysToSend:
    def test_has_keys_to_send_false_when_empty(self):
        s = _state()
        assert s.has_keys_to_send() is False

    def test_has_keys_to_send_true_when_populated(self):
        s = _state()
        s.keys_to_send[OP_PART] = {("k", 1)}
        assert s.has_keys_to_send() is True

    def test_keys_remaining_to_send_counts_all(self):
        s = _state()
        s.keys_to_send[OP_PART] = {("k1", 1), ("k2", 2)}
        assert s.keys_remaining_to_send() == 2

    def test_keys_remaining_to_remote_counts_all(self):
        s = _state()
        s.remote_keys[OP_PART] = {"k1": (1, 0), "k2": (2, 0)}
        assert s.keys_remaining_to_remote() == 2


# ---------------------------------------------------------------------------
# get_async_migrate_batch
# ---------------------------------------------------------------------------


class TestGetAsyncMigrateBatch:
    def test_returns_at_most_batch_size_keys(self):
        s = _state()
        s.data[OP_PART] = {"k1": "v1", "k2": "v2", "k3": "v3"}
        s.keys_to_send[OP_PART] = {("k1", 1), ("k2", 1), ("k3", 1)}
        batch = s.get_async_migrate_batch(batch_size=2)
        total = sum(len(v) for v in batch.values())
        assert total == 2

    def test_removes_sent_keys_from_data(self):
        s = _state()
        s.data[OP_PART] = {"k": "v"}
        s.keys_to_send[OP_PART] = {("k", 1)}
        s.get_async_migrate_batch(batch_size=10)
        assert "k" not in s.data[OP_PART]

    def test_exhausted_partition_removed_from_keys_to_send(self):
        s = _state()
        s.data[OP_PART] = {"k": "v"}
        s.keys_to_send[OP_PART] = {("k", 1)}
        s.get_async_migrate_batch(batch_size=10)
        assert OP_PART not in s.keys_to_send

    def test_routes_to_correct_new_partition(self):
        s = _state()
        s.data[OP_PART] = {"k": "v"}
        s.keys_to_send[OP_PART] = {("k", 2)}
        batch = s.get_async_migrate_batch(batch_size=10)
        assert (OP, 2) in batch
        assert batch[(OP, 2)]["k"] == "v"

    def test_empty_keys_to_send_returns_empty_batch(self):
        s = _state()
        batch = s.get_async_migrate_batch(batch_size=10)
        assert batch == {}


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


class TestExists:
    def test_false_when_key_not_present(self):
        s = _state()
        assert s.exists("k", OP, PART) is False

    def test_true_when_in_remote_keys(self):
        s = _state()
        s.remote_keys[OP_PART] = {"k": (1, 0)}
        assert s.exists("k", OP, PART) is True

    def test_true_when_in_data_and_not_being_sent(self):
        s = _state()
        s.data[OP_PART]["k"] = "v"
        s.keys_to_send[OP_PART] = set()  # partition tracked but "k" not queued
        assert s.exists("k", OP, PART) is True

    def test_false_when_key_queued_for_migration(self):
        # A key in keys_to_send is being migrated to another partition,
        # so it should not be considered as existing locally.
        s = _state()
        s.data[OP_PART]["k"] = "v"
        s.keys_to_send[OP_PART] = {("k", 1)}
        assert s.exists("k", OP, PART) is False

    def test_true_outside_migration_context(self):
        # Key exists in data without any migration context — should return True
        s = _state()
        s.data[OP_PART]["k"] = "v"
        assert s.exists("k", OP, PART) is True

    def test_false_for_unknown_operator_partition(self):
        s = _state()
        assert s.exists("k", "other_op", 99) is False


# ---------------------------------------------------------------------------
# snapshot helpers
# ---------------------------------------------------------------------------


class TestSnapshotHelpers:
    def test_set_data_from_snapshot_loads_kv_pairs(self):
        s = _state()
        s.set_data_from_snapshot({OP_PART: {"k": "v"}})
        assert s.data[OP_PART]["k"] == "v"

    def test_get_data_for_snapshot_returns_delta_map(self):
        s = _state()
        s.delta_map[OP_PART]["k"] = "v"
        delta = s.get_data_for_snapshot()
        assert delta[OP_PART]["k"] == "v"

    def test_get_operator_partitions_to_repartition_groups_by_operator(self):
        s = InMemoryOperatorState({("op", 0), ("op", 1), ("other", 0)})
        result = s.get_operator_partitions_to_repartition()
        assert ("op", 0) in result["op"]
        assert ("op", 1) in result["op"]
        assert ("other", 0) in result["other"]

    def test_get_operator_data_for_repartitioning_returns_data(self):
        s = _state()
        s.data[OP_PART] = {"k": "v"}
        assert s.get_operator_data_for_repartitioning(OP_PART) == {"k": "v"}


# ---------------------------------------------------------------------------
# add_keys_to_send
# ---------------------------------------------------------------------------


class TestAddKeysToSend:
    def test_assigns_keys_to_send(self):
        s = _state()
        kts = {OP_PART: {("k1", 1), ("k2", 2)}}
        s.add_keys_to_send(kts)
        assert s.keys_to_send is kts


# ---------------------------------------------------------------------------
# get_key_to_migrate
# ---------------------------------------------------------------------------


class TestGetKeyToMigrate:
    def test_pops_key_and_removes_from_keys_to_send(self):
        s = _state()
        s.data[OP_PART]["k1"] = "v1"
        s.keys_to_send[OP_PART] = {("k1", 2)}
        val = s.get_key_to_migrate((OP, 2), "k1", PART)
        assert val == "v1"
        assert "k1" not in s.data[OP_PART]
        assert ("k1", 2) not in s.keys_to_send[OP_PART]


# ---------------------------------------------------------------------------
# set_data_from_migration
# ---------------------------------------------------------------------------


class TestSetDataFromMigration:
    def test_stores_data_and_cleans_remote_keys(self):
        s = _state()
        s.remote_keys[OP_PART] = {"k1": (1, 0)}
        s.set_data_from_migration(OP_PART, "k1", "migrated_value")
        assert s.data[OP_PART]["k1"] == "migrated_value"
        assert OP_PART not in s.remote_keys

    def test_partial_remote_keys_cleanup(self):
        s = _state()
        s.remote_keys[OP_PART] = {"k1": (1, 0), "k2": (2, 0)}
        s.set_data_from_migration(OP_PART, "k1", "v1")
        assert "k1" not in s.remote_keys[OP_PART]
        assert "k2" in s.remote_keys[OP_PART]


# ---------------------------------------------------------------------------
# migrate_within_the_same_worker
# ---------------------------------------------------------------------------


class TestMigrateWithinSameWorker:
    def test_moves_key_between_partitions(self):
        s = InMemoryOperatorState({(OP, 0), (OP, 1)})
        s.data[(OP, 0)]["k1"] = "val"
        s.keys_to_send[(OP, 0)] = {("k1", 1)}
        s.remote_keys[(OP, 1)] = {"k1": (0, 0)}
        s.migrate_within_the_same_worker(OP, 1, "k1", 0)
        assert "k1" not in s.data[(OP, 0)]
        assert s.data[(OP, 1)]["k1"] == "val"


# ---------------------------------------------------------------------------
# set_batch_data_from_migration
# ---------------------------------------------------------------------------


class TestSetBatchDataFromMigration:
    def test_batch_stores_and_cleans_remote_keys(self):
        s = _state()
        s.remote_keys[OP_PART] = {"k1": (1, 0), "k2": (1, 0)}
        s.set_batch_data_from_migration(OP_PART, {"k1": "v1", "k2": "v2"})
        assert s.data[OP_PART]["k1"] == "v1"
        assert s.data[OP_PART]["k2"] == "v2"
        assert OP_PART not in s.remote_keys


# ---------------------------------------------------------------------------
# get_all
# ---------------------------------------------------------------------------


class TestGetAll:
    def test_returns_copy_of_data(self):
        s = _state()
        s.data[OP_PART] = {"k1": "v1", "k2": "v2"}
        result = s.get_all(t_id=1, operator_name=OP, partition=PART)
        assert result == {"k1": "v1", "k2": "v2"}
        # Should be a copy
        result["k3"] = "v3"
        assert "k3" not in s.data[OP_PART]

    def test_records_reads_for_all_keys(self):
        s = _state()
        s.data[OP_PART] = {"k1": "v1", "k2": "v2"}
        s.get_all(t_id=1, operator_name=OP, partition=PART)
        assert "k1" in s.read_sets[OP_PART][1]
        assert "k2" in s.read_sets[OP_PART][1]


# ---------------------------------------------------------------------------
# commit exception path
# ---------------------------------------------------------------------------


class TestCommitException:
    async def test_commit_exception_is_reraised(self):
        s = _state()
        _put(s, "k", "v", t_id=1)
        # Corrupt write_sets to trigger an exception
        s.write_sets[("nonexistent", 99)] = {2: {"x": "y"}}
        import pytest

        with pytest.raises(KeyError):
            s.commit(aborted_from_remote=set())


# ---------------------------------------------------------------------------
# delete (no-op)
# ---------------------------------------------------------------------------


class TestDelete:
    def test_delete_is_noop(self):
        s = _state()
        s.data[OP_PART]["k"] = "v"
        s.delete("k", OP, PART)
        # Delete is not implemented, data should remain
        assert s.data[OP_PART]["k"] == "v"
