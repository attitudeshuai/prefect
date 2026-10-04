"""
Read-only precheck support for state transition orchestration.

A precheck replays the currently effective orchestration rules against a
proposed state transition without committing anything: no state record is
written, run counts and timestamps are untouched, and concurrency capacity is
neither acquired nor released. Its verdict (accept / reject / wait / abort and
any rewritten state) matches a real submission made at the same time.
"""

from __future__ import annotations

import contextlib
from typing import Any, Optional, Union

from prefect.server.database import orm_models
from prefect.server.orchestration.policies import BaseOrchestrationPolicy
from prefect.server.orchestration.rules import OrchestrationContext
from prefect.server.schemas import states


class StaleStateSnapshotError(Exception):
    """
    Raised when a caller-supplied current-state snapshot does not match the
    run's current server state.

    A precheck with a stale snapshot is rejected outright rather than evaluated
    against the newer state, so a caller can never act on a verdict produced for
    an outdated transition.
    """

    def __init__(
        self,
        message: str,
        *,
        snapshot: Optional[states.State],
        server_state: Optional[states.State],
        differences: list[str],
    ):
        super().__init__(message)
        self.snapshot = snapshot
        self.server_state = server_state
        self.differences = differences


async def verify_state_snapshot(
    run: orm_models.Run,
    snapshot: Optional[states.State],
) -> None:
    """
    Verify that a caller-held snapshot of the run's current state matches the
    server's current state.

    The snapshot is compared on state id, type, name, and timestamp. When the
    snapshot omits the name or timestamp those fields are not compared. A
    mismatch of any present field raises `StaleStateSnapshotError`.
    """
    if snapshot is None:
        return

    server_state = run.state.as_state() if run.state else None

    if server_state is None:
        raise StaleStateSnapshotError(
            "The provided current-state snapshot does not match the run: the run"
            " currently has no state on the server.",
            snapshot=snapshot,
            server_state=None,
            differences=["state_id"],
        )

    differences: list[str] = []

    if snapshot.id != server_state.id:
        differences.append("state_id")
    if snapshot.type != server_state.type:
        differences.append("type")
    if snapshot.name is not None and snapshot.name != server_state.name:
        differences.append("name")
    if (
        snapshot.timestamp is not None
        and snapshot.timestamp != server_state.timestamp
    ):
        differences.append("timestamp")

    if differences:
        raise StaleStateSnapshotError(
            "The provided current-state snapshot does not match the run's current"
            f" server state (differing fields: {', '.join(differences)}). Re-fetch"
            " the run's state and retry the precheck.",
            snapshot=snapshot,
            server_state=server_state,
            differences=differences,
        )


async def run_transition_preview(
    context: OrchestrationContext[
        orm_models.Run,
        Union[
            Any,
            Any,
        ],
    ],
    policy: type[BaseOrchestrationPolicy[Any, Any]],
    global_policy: type[BaseOrchestrationPolicy[Any, Any]],
    intended_transition: tuple[
        Optional[states.StateType], Optional[states.StateType]
    ],
) -> OrchestrationContext[Any, Any]:
    """
    Enter every rule governing `intended_transition` in dry-run mode and
    validate the proposed state without committing. Returns the governed
    context, whose response fields carry the precheck verdict.
    """
    orchestration_rules = policy.compile_transition_rules(*intended_transition)
    global_rules = global_policy.compile_transition_rules(*intended_transition)

    async with contextlib.AsyncExitStack() as stack:
        for rule in orchestration_rules:
            context = await stack.enter_async_context(
                rule(context, *intended_transition)
            )

        for rule in global_rules:
            context = await stack.enter_async_context(
                rule(context, *intended_transition)
            )

        await context.validate_proposed_state()

    return context
