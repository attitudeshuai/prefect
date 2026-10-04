"""
Time budget propagation along the flow/task run tree.

A run's attempt receives an *effective timeout* that is the smaller of its own
declared `timeout_seconds` and the remaining budget handed down by its parent
run's current attempt. The budget is expressed as a deadline:

- same-process propagation uses a `time.monotonic()` deadline;
- cross-process serialization (ProcessPoolTaskRunner) uses a UTC wall-clock
  deadline that is converted back to a monotonic deadline on hydration.

The currently inherited budget is held in a ContextVar. When no run in the tree
declares a timeout, the variable is never written and no timing is added.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Optional, Union
from uuid import UUID

import prefect.types._datetime
from prefect.types._datetime import DateTime

TimeoutPropagation = Literal["fail", "raise"]

TIMEOUT_PROPAGATION_VALUES: tuple[str, str] = ("fail", "raise")
#: Sentinel source value meaning the run's own declared timeout is binding.
SELF = "self"

# Holds the budget installed by the enclosing run, if any.
_BUDGET_VAR: ContextVar[BudgetLimit] = ContextVar("run-time-budget")


@dataclass(frozen=True)
class BudgetLimit:
    """
    A budget deadline installed by a run for its children.

    Attributes:
        layer_depth: depth in the run tree of the run that installed this limit
        source_run_id: id of the ancestor run whose budget is the binding
            constraint; equals the installing run's id when its own timeout binds
        source_run_name: name of the binding source run
        source_depth: tree depth of the binding source run
        monotonic_deadline: same-process deadline on the monotonic clock
        expires_at: UTC wall-clock deadline used for cross-process serialization
    """

    layer_depth: int
    source_run_id: Optional[UUID]
    source_run_name: Optional[str]
    source_depth: int
    monotonic_deadline: float
    expires_at: Optional[DateTime]

    def remaining(self) -> float:
        """Seconds left on the monotonic deadline; zero once expired."""
        return max(0.0, self.monotonic_deadline - time.monotonic())

    def is_expired(self) -> bool:
        return time.monotonic() >= self.monotonic_deadline

    def to_serialized(self) -> dict[str, object]:
        """Serialize to a plain, picklable dict for cross-process hydration."""
        return {
            "layer_depth": self.layer_depth,
            "source_run_id": str(self.source_run_id) if self.source_run_id else None,
            "source_run_name": self.source_run_name,
            "source_depth": self.source_depth,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }

    @classmethod
    def from_serialized(cls, data: dict[str, object]) -> BudgetLimit:
        """
        Rebuild a limit after a process boundary.

        The monotonic deadline is recomputed from the UTC deadline so it remains
        on the new process's monotonic clock.
        """
        expires_at_str = data.get("expires_at")
        expires_at = (
            prefect.types._datetime.parse_datetime(str(expires_at_str))
            if expires_at_str
            else None
        )
        if expires_at is not None:
            wall_remaining = (expires_at - prefect.types._datetime.now("UTC")).total_seconds()
            monotonic_deadline = time.monotonic() + max(0.0, wall_remaining)
        else:  # defensive: no deadline means already expired
            monotonic_deadline = time.monotonic()

        source_run_id = data.get("source_run_id")
        return cls(
            layer_depth=int(data["layer_depth"]),  # type: ignore[arg-type]
            source_run_id=UUID(str(source_run_id)) if source_run_id else None,
            source_run_name=(
                str(data["source_run_name"]) if data.get("source_run_name") else None
            ),
            source_depth=int(data["source_depth"]),  # type: ignore[arg-type]
            monotonic_deadline=monotonic_deadline,
            expires_at=expires_at,
        )


@dataclass(frozen=True)
class EffectiveTimeout:
    """
    Resolution of a run attempt's effective timeout.

    Attributes:
        seconds: effective timeout in seconds; None when no timeout applies
        source: SELF when the run's own declared timeout binds; otherwise the
            inherited BudgetLimit that is binding
        inherited: the inherited budget read at resolution, if any
    """

    seconds: Optional[float]
    source: Optional[Union[str, BudgetLimit]]
    inherited: Optional[BudgetLimit]

    @property
    def is_inherited(self) -> bool:
        return self.source is not SELF and self.source is not None


def get_inherited_budget() -> Optional[BudgetLimit]:
    """Return the budget installed by the enclosing run, if any."""
    return _BUDGET_VAR.get(None)


def resolve_effective_timeout(
    declared_seconds: Optional[float],
) -> EffectiveTimeout:
    """
    Resolve the effective timeout for a run attempt at its Running boundary.

    - no declared timeout and no inherited budget -> None (unchanged behavior);
    - declared timeout smaller than/equal to inherited remaining -> own timeout
      binds, source is SELF;
    - inherited remaining smaller (including already expired, remaining == 0) ->
      inherited budget binds.
    """
    inherited = get_inherited_budget()

    if declared_seconds is None:
        if inherited is None:
            return EffectiveTimeout(seconds=None, source=None, inherited=None)
        return EffectiveTimeout(
            seconds=inherited.remaining(), source=inherited, inherited=inherited
        )

    if inherited is None:
        return EffectiveTimeout(seconds=declared_seconds, source=SELF, inherited=None)

    remaining = inherited.remaining()
    if declared_seconds <= remaining:
        return EffectiveTimeout(
            seconds=declared_seconds, source=SELF, inherited=inherited
        )
    return EffectiveTimeout(seconds=remaining, source=inherited, inherited=inherited)


def build_installed_limit(
    effective: EffectiveTimeout,
    run_id: Optional[UUID],
    run_name: Optional[str],
) -> BudgetLimit:
    """
    Build the BudgetLimit a run installs for its children after resolution.

    When the run's own timeout binds the limit is sourced at this layer; when an
    inherited budget binds the ancestor's source descriptor is carried through
    unchanged while layer depth advances.
    """
    if effective.seconds is None:
        raise ValueError("Cannot install a budget limit without an effective timeout")

    parent_depth = effective.inherited.layer_depth if effective.inherited else -1
    layer_depth = parent_depth + 1

    if effective.source is SELF:
        seconds = effective.seconds
        return BudgetLimit(
            layer_depth=layer_depth,
            source_run_id=run_id,
            source_run_name=run_name,
            source_depth=layer_depth,
            monotonic_deadline=time.monotonic() + seconds,
            expires_at=prefect.types._datetime.now("UTC") + timedelta(seconds=seconds),
        )

    # inherited budget binds: preserve the ancestor's source descriptor
    assert isinstance(effective.source, BudgetLimit)
    return BudgetLimit(
        layer_depth=layer_depth,
        source_run_id=effective.source.source_run_id,
        source_run_name=effective.source.source_run_name,
        source_depth=effective.source.source_depth,
        monotonic_deadline=effective.source.monotonic_deadline,
        expires_at=effective.source.expires_at,
    )


@contextmanager
def budget_context(limit: Optional[BudgetLimit]) -> Generator[None, Any, None]:
    """
    Install a budget for children started within the scope.

    A None limit (no timeout configured anywhere on the path) writes nothing to
    the ContextVar so the no-timeout path is unchanged.
    """
    if limit is None:
        yield
        return

    token = _BUDGET_VAR.set(limit)
    try:
        yield
    finally:
        _BUDGET_VAR.reset(token)


def sync_cancellation_enforced() -> bool:
    """
    Whether a synchronous timeout can be delivered to the running body.

    On Windows `cancel_sync_after` yields a NullCancelScope so the body cannot be
    interrupted; the engine must converge at boundaries and leave a marker.
    """
    return not sys.platform.startswith("win")


def validate_timeout_propagation(value: object) -> str:
    """Validate the timeout_propagation decorator parameter."""
    if not isinstance(value, str) or value not in TIMEOUT_PROPAGATION_VALUES:
        raise ValueError(
            "Invalid value for 'timeout_propagation': "
            f"{value!r}. Expected one of {TIMEOUT_PROPAGATION_VALUES!r}."
        )
    return value


def validate_timeout_seconds(value: object) -> Optional[float]:
    """
    Validate and normalize a timeout_seconds decorator argument.

    None (unset) is allowed and returns None. Non-numeric values raise
    TypeError; non-positive values raise ValueError because they cannot describe
    a valid timeout.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        seconds = float(value)
    else:
        try:
            seconds = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise TypeError(
                "Invalid value for 'timeout_seconds': "
                f"{value!r}. Expected a number of seconds or None."
            ) from None

    if seconds <= 0:
        raise ValueError(
            "Invalid value for 'timeout_seconds': "
            f"{value!r}. Timeout must be greater than zero or None."
        )

    return seconds
