import time
from datetime import timedelta
from uuid import uuid4

import pytest

from prefect._internal.budget import (
    SELF,
    BudgetLimit,
    EffectiveTimeout,
    budget_context,
    build_installed_limit,
    get_inherited_budget,
    resolve_effective_timeout,
    validate_timeout_propagation,
    validate_timeout_seconds,
)
from prefect.types._datetime import now


def _limit(
    remaining: float = 10.0,
    layer_depth: int = 0,
    source_run_id=None,
    source_run_name: str = "source-flow",
) -> BudgetLimit:
    return BudgetLimit(
        layer_depth=layer_depth,
        source_run_id=source_run_id or uuid4(),
        source_run_name=source_run_name,
        source_depth=layer_depth,
        monotonic_deadline=time.monotonic() + remaining,
        expires_at=now("UTC") + timedelta(seconds=remaining),
    )


class TestResolveEffectiveTimeout:
    def test_no_timeout_no_inherited(self):
        result = resolve_effective_timeout(None)
        assert result == EffectiveTimeout(seconds=None, source=None, inherited=None)

    def test_declared_without_inherited_binds_self(self):
        result = resolve_effective_timeout(5.0)
        assert result.seconds == 5.0
        assert result.source is SELF
        assert result.inherited is None

    def test_declared_smaller_than_inherited_binds_self(self):
        inherited = _limit(remaining=10.0)
        with budget_context(inherited):
            result = resolve_effective_timeout(5.0)
        assert result.seconds == 5.0
        assert result.source is SELF
        assert result.inherited is inherited

    def test_declared_larger_than_inherited_binds_inherited(self):
        inherited = _limit(remaining=3.0)
        with budget_context(inherited):
            result = resolve_effective_timeout(10.0)
        assert result.seconds == pytest.approx(3.0, abs=0.1)
        assert result.source is inherited
        assert result.is_inherited

    def test_expired_inherited_binds_with_zero(self):
        inherited = _limit(remaining=-1.0)
        assert inherited.is_expired()
        with budget_context(inherited):
            result = resolve_effective_timeout(10.0)
        assert result.seconds == 0.0
        assert result.source is inherited

    def test_no_declared_with_inherited_uses_remaining(self):
        inherited = _limit(remaining=4.0)
        with budget_context(inherited):
            result = resolve_effective_timeout(None)
        assert result.seconds == pytest.approx(4.0, abs=0.1)
        assert result.source is inherited


class TestInstalledLimit:
    def test_self_binding_root_layer(self):
        resolved = EffectiveTimeout(seconds=5.0, source=SELF, inherited=None)
        run_id = uuid4()
        limit = build_installed_limit(resolved, run_id=run_id, run_name="root")
        assert limit.layer_depth == 0
        assert limit.source_run_id == run_id
        assert limit.source_depth == 0
        assert limit.remaining() == pytest.approx(5.0, abs=0.1)

    def test_self_binding_child_layer_advances_depth(self):
        parent = _limit(remaining=10.0, layer_depth=0)
        resolved = EffectiveTimeout(seconds=5.0, source=SELF, inherited=parent)
        limit = build_installed_limit(resolved, run_id=uuid4(), run_name="child")
        assert limit.layer_depth == 1
        assert limit.source_depth == 1

    def test_inherited_binding_preserves_source(self):
        ancestor_id = uuid4()
        parent = _limit(
            remaining=10.0, layer_depth=0, source_run_id=ancestor_id
        )
        # child at depth 1 inherits; grandchild at depth 2 inherits the same source
        child = BudgetLimit(
            layer_depth=1,
            source_run_id=ancestor_id,
            source_run_name="source-flow",
            source_depth=0,
            monotonic_deadline=parent.monotonic_deadline,
            expires_at=parent.expires_at,
        )
        resolved = EffectiveTimeout(
            seconds=parent.remaining(), source=child, inherited=child
        )
        limit = build_installed_limit(resolved, run_id=uuid4(), run_name="grandchild")
        assert limit.layer_depth == 2
        assert limit.source_run_id == ancestor_id
        assert limit.source_depth == 0

    def test_raises_without_effective_seconds(self):
        with pytest.raises(ValueError):
            build_installed_limit(
                EffectiveTimeout(seconds=None, source=None, inherited=None),
                run_id=None,
                run_name=None,
            )


class TestSerialization:
    def test_roundtrip_preserves_remaining(self):
        original = _limit(remaining=10.0)
        serialized = original.to_serialized()
        assert isinstance(serialized["source_run_id"], str)
        assert isinstance(serialized["expires_at"], str)

        restored = BudgetLimit.from_serialized(serialized)
        assert restored.source_run_id == original.source_run_id
        assert restored.layer_depth == original.layer_depth
        assert restored.source_depth == original.source_depth
        assert restored.remaining() == pytest.approx(10.0, abs=0.5)

    def test_roundtrip_expired_deadline(self):
        original = _limit(remaining=-5.0)
        restored = BudgetLimit.from_serialized(original.to_serialized())
        assert restored.remaining() == 0.0
        assert restored.is_expired()


class TestBudgetContext:
    def test_none_writes_nothing(self):
        assert get_inherited_budget() is None
        with budget_context(None):
            assert get_inherited_budget() is None
        assert get_inherited_budget() is None

    def test_install_reset_and_nesting(self):
        outer = _limit(remaining=10.0)
        inner = _limit(remaining=2.0, layer_depth=1)

        with budget_context(outer):
            assert get_inherited_budget() is outer
            with budget_context(inner):
                assert get_inherited_budget() is inner
            assert get_inherited_budget() is outer
        assert get_inherited_budget() is None


class TestValidation:
    def test_timeout_propagation_valid(self):
        assert validate_timeout_propagation("fail") == "fail"
        assert validate_timeout_propagation("raise") == "raise"

    @pytest.mark.parametrize("value", ["FAIL", "RAISE", "none", "", None, 0, True])
    def test_timeout_propagation_invalid(self, value):
        with pytest.raises(ValueError):
            validate_timeout_propagation(value)

    def test_timeout_seconds_none(self):
        assert validate_timeout_seconds(None) is None

    def test_timeout_seconds_numbers_and_strings(self):
        assert validate_timeout_seconds(1) == 1.0
        assert validate_timeout_seconds("2.5") == 2.5
        assert validate_timeout_seconds(True) == 1.0

    @pytest.mark.parametrize("value", [0, -1, "0", "-0.1"])
    def test_timeout_seconds_non_positive(self, value):
        with pytest.raises(ValueError):
            validate_timeout_seconds(value)

    def test_timeout_seconds_non_numeric(self):
        with pytest.raises(TypeError):
            validate_timeout_seconds("soon")
