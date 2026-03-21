"""Unit tests for styx/common/message_types.py"""

from enum import IntEnum

from styx.common.message_types import MessageType


class TestMessageType:
    def test_is_int_enum(self):
        assert issubclass(MessageType, IntEnum)

    def test_total_count(self):
        assert len(MessageType) == 43

    def test_all_values_unique(self):
        values = [m.value for m in MessageType]
        assert len(values) == len(set(values))

    def test_values_are_contiguous_from_zero(self):
        values = sorted(m.value for m in MessageType)
        assert values == list(range(43))

    def test_known_values(self):
        assert MessageType.RunFunRemote == 0
        assert MessageType.SendExecutionGraph == 2
        assert MessageType.ClientMsg == 16
        assert MessageType.InitDataComplete == 40
        assert MessageType.UpdateExecutionGraph == 41
        assert MessageType.SnapMigrationReassign == 42

    def test_int_comparison(self):
        assert MessageType.ClientMsg == 16
        assert MessageType.ClientMsg > MessageType.RunFunRemote

    def test_lookup_by_value(self):
        assert MessageType(7) is MessageType.AriaCommit
        assert MessageType(19) is MessageType.Heartbeat
        assert MessageType(30) is MessageType.SnapMarker
