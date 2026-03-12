"""Unit tests for styx/common/run_func_payload.py"""

from styx.common.run_func_payload import RunFuncPayload, SequencedItem


# ---------------------------------------------------------------------------
# RunFuncPayload
# ---------------------------------------------------------------------------


class TestRunFuncPayload:
    def test_basic_fields(self):
        p = RunFuncPayload(
            request_id=b"req1",
            key="k",
            operator_name="op",
            partition=2,
            function_name="fn",
            params=(1, 2),
        )
        assert p.request_id == b"req1"
        assert p.key == "k"
        assert p.operator_name == "op"
        assert p.partition == 2
        assert p.function_name == "fn"
        assert p.params == (1, 2)

    def test_defaults(self):
        p = RunFuncPayload(
            request_id=b"r",
            key="k",
            operator_name="op",
            partition=0,
            function_name="fn",
            params=(),
        )
        assert p.kafka_offset == -1
        assert p.kafka_ingress_partition == -1
        assert p.ack_payload is None

    def test_with_ack_payload(self):
        ack = ("host", 8080, 1, "1/2", [0, 1], 0)
        p = RunFuncPayload(
            request_id=b"r",
            key="k",
            operator_name="op",
            partition=0,
            function_name="fn",
            params=(),
            ack_payload=ack,
        )
        assert p.ack_payload == ack

    def test_equality(self):
        p1 = RunFuncPayload(request_id=b"r", key="k", operator_name="op", partition=0, function_name="fn", params=())
        p2 = RunFuncPayload(request_id=b"r", key="k", operator_name="op", partition=0, function_name="fn", params=())
        assert p1 == p2


# ---------------------------------------------------------------------------
# SequencedItem
# ---------------------------------------------------------------------------


class TestSequencedItem:
    def test_basic_fields(self):
        p = RunFuncPayload(request_id=b"r", key="k", operator_name="op", partition=0, function_name="fn", params=())
        s = SequencedItem(t_id=10, payload=p)
        assert s.t_id == 10
        assert s.payload is p

    def test_hash_based_on_tid(self):
        p = RunFuncPayload(request_id=b"r", key="k", operator_name="op", partition=0, function_name="fn", params=())
        s1 = SequencedItem(t_id=10, payload=p)
        s2 = SequencedItem(t_id=10, payload=p)
        assert hash(s1) == hash(s2)

    def test_different_tid_different_hash(self):
        p = RunFuncPayload(request_id=b"r", key="k", operator_name="op", partition=0, function_name="fn", params=())
        s1 = SequencedItem(t_id=10, payload=p)
        s2 = SequencedItem(t_id=20, payload=p)
        assert hash(s1) != hash(s2)

    def test_lt_comparison(self):
        p = RunFuncPayload(request_id=b"r", key="k", operator_name="op", partition=0, function_name="fn", params=())
        s1 = SequencedItem(t_id=5, payload=p)
        s2 = SequencedItem(t_id=10, payload=p)
        assert s1 < s2
        assert not s2 < s1

    def test_sorting(self):
        p = RunFuncPayload(request_id=b"r", key="k", operator_name="op", partition=0, function_name="fn", params=())
        items = [SequencedItem(t_id=30, payload=p), SequencedItem(t_id=10, payload=p), SequencedItem(t_id=20, payload=p)]
        sorted_items = sorted(items)
        assert [i.t_id for i in sorted_items] == [10, 20, 30]
