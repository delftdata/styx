"""Unit tests for worker/operator_state/stateless.py"""

from worker.operator_state.stateless import Stateless


class TestStateless:
    def test_can_instantiate(self):
        s = Stateless()
        assert s is not None

    def test_is_empty_class(self):
        s = Stateless()
        # Stateless has no custom attributes beyond default object attrs
        assert not hasattr(s, "__dict__") or len(s.__dict__) == 0
