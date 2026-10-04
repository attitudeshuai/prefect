"""Retry time budget: shared semantics for flow runs and task runs.

This module is the single source of truth for the cross-attempt cumulative
run-time budget used by both retry paths:

- flow runs, judged server-side by the orchestration layer;
- task runs, judged client-side by the task engine.

It is safe to import from client-side code: it has no server imports and no
third-party dependencies. Objects passed in (runs, states, policies) are used
structurally via attribute access so this module does not depend on the schema
classes.

The cumulative value (``retry_budget_elapsed``) is always read from the run's
own persisted state and folded in incrementally; nothing here sums state
history on read. The helpers in this module that build attempt records are for
display only and must never be used to derive the budget value.
"""

from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Configuration vocabulary
# ---------------------------------------------------------------------------

FAIL: str = "fail"
CANCEL: str = "cancel"
MARK: str = "mark"

#: Configurable dispositions for a run whose retry budget is exceeded:
#: fail the run, cancel the run, or mark it without changing scheduling.
ENFORCEMENT_VALUES: tuple[str, ...] = (FAIL, CANCEL, MARK)

LOCAL: str = "local"
REMOTE: str = "remote"

#: Origins of an attempt: a local retry (in-process) or a remote reschedule
#: taken over by a new process/worker.
ATTEMPT_ORIGINS: tuple[str, ...] = (LOCAL, REMOTE)

AWAITING_RETRY: str = "AwaitingRetry"


# ---------------------------------------------------------------------------
# Policy access
# ---------------------------------------------------------------------------


def _empirical_policy(run: Any) -> Any:
    return getattr(run, "empirical_policy", None)


def budget_limit_seconds(policy: Any) -> Optional[float]:
    """The configured budget limit in seconds, or None when no budget exists."""
    if policy is None:
        return None
    return getattr(policy, "retry_budget_seconds", None)


def is_budget_configured(policy: Any) -> bool:
    """Whether a retry budget limit has been configured."""
    return budget_limit_seconds(policy) is not None


def resolve_count_wait(policy: Any) -> bool:
    """Whether retry-wait time should count; defaults to False."""
    if policy is None:
        return False
    return bool(getattr(policy, "retry_budget_include_queue_time", False))


def resolve_enforcement(policy: Any) -> str:
    """Resolve the disposition used when the budget is exceeded; defaults to fail."""
    if policy is None:
        return FAIL
    enforcement = getattr(policy, "retry_budget_enforcement", None)
    if enforcement in ENFORCEMENT_VALUES:
        return str(enforcement)
    return FAIL


def exceeds_budget(elapsed: datetime.timedelta, policy: Any) -> bool:
    """Whether `elapsed` is strictly greater than the configured limit."""
    limit = budget_limit_seconds(policy)
    if limit is None:
        return False
    return elapsed.total_seconds() > float(limit)


# ---------------------------------------------------------------------------
# Inline folding
# ---------------------------------------------------------------------------


def projected_elapsed(
    run: Any,
    initial_state: Any,
    proposed_state: Any,
) -> datetime.timedelta:
    """Persisted elapsed plus the running segment currently being closed.

    This projects exactly one segment (the segment that this transition
    closes) onto the persisted cumulative value; it never reads state
    history. Used to judge a transition before the fold is committed.
    """
    elapsed: datetime.timedelta = run.retry_budget_elapsed
    if (
        initial_state is not None
        and proposed_state is not None
        and initial_state.is_running()
    ):
        elapsed += proposed_state.timestamp - initial_state.timestamp
    return elapsed


def fold_running_segment(run: Any, initial_state: Any, proposed_state: Any) -> None:
    """Fold the time spent in a closing RUNNING segment into the run's elapsed.

    Only folds when a budget is configured. No-op unless a running segment is
    being exited.
    """
    if initial_state is None or proposed_state is None:
        return
    if not initial_state.is_running():
        return
    if not is_budget_configured(_empirical_policy(run)):
        return
    run.retry_budget_elapsed += (
        proposed_state.timestamp - initial_state.timestamp
    )


def freeze_wait_policy(run: Any, proposed_state: Any) -> None:
    """Freeze the wait-inclusion basis at AwaitingRetry entry.

    The basis is taken from the configuration in effect when the wait begins
    and is not changed by later configuration edits while waiting. The wait
    state id is recorded so the later fold can be deduplicated.
    """
    policy = _empirical_policy(run)
    run.retry_budget_count_wait = resolve_count_wait(policy)
    run.retry_budget_wait_state_id = proposed_state.id


def fold_wait_segment(run: Any, wait_state: Any, next_state: Any) -> None:
    """Fold a closing retry-wait segment according to the frozen basis.

    Idempotent: only folds when the run's guard marker matches the wait state
    id. After folding, the marker is rewritten to `next_state.id` so the same
    segment cannot be folded again and the column never needs to be cleared
    via NULL.
    """
    if wait_state is None or next_state is None:
        return
    if run.retry_budget_wait_state_id != wait_state.id:
        return
    if run.retry_budget_count_wait:
        run.retry_budget_elapsed += next_state.timestamp - wait_state.timestamp
    run.retry_budget_wait_state_id = next_state.id
    # overwrite with a concrete value; avoids clearing a non-null column
    run.retry_budget_count_wait = False


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


def budget_exceeded_message(
    elapsed: datetime.timedelta,
    policy: Any,
    original_message: Optional[str] = None,
) -> str:
    """Build the state message for an exceeded budget.

    Includes the reason, the cumulative elapsed value and the configured
    limit. An optional original message is preserved as a prefix.
    """
    limit = budget_limit_seconds(policy)
    message = (
        "Retry budget exceeded: cumulative run time "
        f"{elapsed.total_seconds():.3f} second(s) exceeds the configured "
        f"limit of {float(limit):.3f} second(s)."
    )
    if original_message:
        return f"{original_message}\n{message}"
    return message


def annotate_message(message: Optional[str], annotation: str) -> str:
    """Append an annotation to a state message."""
    return f"{message}\n{annotation}" if message else annotation


# ---------------------------------------------------------------------------
# Read-only attempt sequence (display only)
# ---------------------------------------------------------------------------


@dataclass
class RunAttempt:
    """A single attempt in a run's read-only attempt sequence."""

    attempt_number: int
    state_id: uuid.UUID
    start_time: datetime.datetime
    end_time: Optional[datetime.datetime]
    run_time_seconds: float
    wait_time_seconds: float
    failure_message: Optional[str]
    next_scheduled_start_time: Optional[datetime.datetime]
    origin: Optional[str]

    def model_dump(self) -> dict[str, Any]:
        return {
            "attempt_number": self.attempt_number,
            "state_id": self.state_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "run_time_seconds": self.run_time_seconds,
            "wait_time_seconds": self.wait_time_seconds,
            "failure_message": self.failure_message,
            "next_scheduled_start_time": self.next_scheduled_start_time,
            "origin": self.origin,
        }


def _state_timestamp(state: Any) -> datetime.datetime:
    return state.timestamp


def _is_running_state(state: Any) -> bool:
    return state.type.value == "RUNNING"


def _is_failed_state(state: Any) -> bool:
    return state.type.value in ("FAILED", "CRASHED")


def _scheduled_time(state: Any) -> Optional[datetime.datetime]:
    return getattr(state.state_details, "scheduled_time", None)


def attempts_from_states(states: list[Any]) -> list[RunAttempt]:
    """Build the ordered attempt sequence from a run's states (read-only).

    Every RUNNING state begins an attempt and is numbered in timestamp
    order. A preceding ``AwaitingRetry`` state supplies that attempt's wait
    duration; the state following the running segment supplies its end time,
    failure message and next scheduled start.

    The sequence covers both local retries and remote reschedules: the
    attempt origin is read from ``state_details.attempt_origin`` when set,
    with "Retrying" states defaulting to local.
    """
    ordered = sorted(states, key=_state_timestamp)

    attempts: list[RunAttempt] = []
    attempt_number = 0

    for index, state in enumerate(ordered):
        if not _is_running_state(state):
            continue
        attempt_number += 1

        wait_time_seconds = 0.0
        if index > 0:
            previous = ordered[index - 1]
            if previous.name == AWAITING_RETRY:
                wait_time_seconds = (
                    state.timestamp - previous.timestamp
                ).total_seconds()

        following = ordered[index + 1] if index + 1 < len(ordered) else None

        if following is not None:
            end_time: Optional[datetime.datetime] = following.timestamp
            run_time_seconds = (
                following.timestamp - state.timestamp
            ).total_seconds()
            failure_message = (
                following.message if _is_failed_state(following) else None
            )
            next_scheduled_start_time = (
                _scheduled_time(following)
                if following.name == AWAITING_RETRY
                else None
            )
        else:
            end_time = None
            run_time_seconds = 0.0
            failure_message = None
            next_scheduled_start_time = None

        origin = getattr(state.state_details, "attempt_origin", None)
        if origin is None and state.name == "Retrying":
            origin = LOCAL

        attempts.append(
            RunAttempt(
                attempt_number=attempt_number,
                state_id=state.id,
                start_time=state.timestamp,
                end_time=end_time,
                run_time_seconds=run_time_seconds,
                wait_time_seconds=wait_time_seconds,
                failure_message=failure_message,
                next_scheduled_start_time=next_scheduled_start_time,
                origin=origin,
            )
        )

    return attempts
