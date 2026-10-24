"""Unit tests for styx/common/message_types.py"""

from enum import IntEnum

from styx.common.message_types import MessageType


class TestMessageType:
    def test_is_int_enum(self):
        assert issubclass(MessageType, IntEnum)

    def test_total_count(self):
        assert len(MessageType) == 47

    def test_all_values_unique(self):
        values = [m.value for m in MessageType]
        assert len(values) == len(set(values))

    def test_values_in_expected_range(self):
        # 25 (AckCache) was removed when the fallback cache was dropped, and 26
        # (WrongPartitionRequest) when mis-routed requests became proxied calls;
        # the remaining IDs are preserved to keep wire-format stability.
        values = sorted(m.value for m in MessageType)
        assert values == [v for v in range(49) if v not in {25, 26}]

    def test_known_values(self):
        assert MessageType.RunFunRemote == 0
        assert MessageType.SendExecutionGraph == 2
        assert MessageType.ClientMsg == 16
        assert MessageType.InitDataComplete == 40
        assert MessageType.UpdateExecutionGraph == 41
        assert MessageType.RunFunRemoteBatch == 42
        assert MessageType.AckBatch == 43
        assert MessageType.SnapMigrationReassign == 44
        assert MessageType.AsyncMigrationAck == 45
        assert MessageType.RecoveryMigrationHashes == 46
        assert MessageType.RunFunProxy == 47
        assert MessageType.ProxyResult == 48

    def test_int_comparison(self):
        assert MessageType.ClientMsg == 16
        assert MessageType.ClientMsg > MessageType.RunFunRemote

    def test_lookup_by_value(self):
        assert MessageType(7) is MessageType.AriaCommit
        assert MessageType(19) is MessageType.Heartbeat
        assert MessageType(30) is MessageType.SnapMarker
