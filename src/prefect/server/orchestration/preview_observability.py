"""
In-process observation surface for read-only state transition prechecks.

Records counts and orchestration-provided reasons for transitions that were
rejected, aborted, asked to wait, or rewritten during a precheck. Rule
identifiers are never recorded — only the human-readable reason attached to
the verdict and the outcome state type/name.

The surface is process-local (like the in-memory worker cleanup queue) and can
be disabled or sampled with the `server.orchestration.preview_observations_*`
settings.
"""

from __future__ import annotations

import random
import threading
from collections import Counter, deque
from typing import Deque, Optional

from prefect.server.schemas.responses import (
    OrchestrationResult,
    SetStateStatus,
)
from prefect.server.schemas.state_transitions import (
    StateTransitionPreviewObservationEvent,
    StateTransitionPreviewObservations,
    StateTransitionRunType,
)
from prefect.settings import get_current_settings
from prefect.types._datetime import now

_NON_ACCEPTED_STATUSES = {
    SetStateStatus.REJECT,
    SetStateStatus.ABORT,
    SetStateStatus.WAIT,
}


class _PreviewObservationRecorder:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._evaluated = 0
        self._recorded = 0
        self._status_counts: Counter[str] = Counter()
        self._rewritten_count = 0
        self._reason_counts: Counter[str] = Counter()
        self._recent: Deque[StateTransitionPreviewObservationEvent] = deque()

    def record(
        self,
        run_type: StateTransitionRunType,
        result: OrchestrationResult,
        rewritten: bool,
    ) -> None:
        settings = get_current_settings().server.orchestration
        if not settings.preview_observations_enabled:
            return

        if result.status not in _NON_ACCEPTED_STATUSES and not rewritten:
            return

        with self._lock:
            self._evaluated += 1

            sample_rate = settings.preview_observations_sample_rate
            if sample_rate <= 0 or random.random() >= sample_rate:
                return

            self._recorded += 1
            status_value = result.status.value
            self._status_counts[status_value] += 1
            if rewritten:
                self._rewritten_count += 1

            reason: Optional[str] = None
            details = result.details
            if hasattr(details, "reason"):
                reason = details.reason
            if reason:
                self._reason_counts[reason] += 1

            governed = result.state
            event = StateTransitionPreviewObservationEvent(
                occurred=now("UTC"),
                run_type=run_type,
                status=result.status,
                rewritten=rewritten,
                reason=reason,
                final_state_type=governed.type if governed is not None else None,
                final_state_name=governed.name if governed is not None else None,
            )

            max_events = settings.preview_observations_max_events
            self._recent.append(event)
            while len(self._recent) > max_events:
                self._recent.popleft()

    def snapshot(self) -> StateTransitionPreviewObservations:
        settings = get_current_settings().server.orchestration
        with self._lock:
            return StateTransitionPreviewObservations(
                enabled=settings.preview_observations_enabled,
                sample_rate=settings.preview_observations_sample_rate,
                evaluated=self._evaluated,
                recorded=self._recorded,
                status_counts=dict(self._status_counts),
                rewritten_count=self._rewritten_count,
                reason_counts=dict(self._reason_counts),
                recent=list(self._recent),
            )

    def reset(self) -> None:
        with self._lock:
            self._evaluated = 0
            self._recorded = 0
            self._status_counts.clear()
            self._rewritten_count = 0
            self._reason_counts.clear()
            self._recent.clear()


recorder = _PreviewObservationRecorder()


def record_preview_observation(
    run_type: StateTransitionRunType,
    result: OrchestrationResult,
    rewritten: bool,
) -> None:
    """Record a precheck outcome according to the observation settings."""
    recorder.record(run_type, result, rewritten)


def read_preview_observations() -> StateTransitionPreviewObservations:
    """Return a point-in-time snapshot of recorded precheck observations."""
    return recorder.snapshot()


def reset_preview_observations() -> None:
    """Clear all recorded observations (primarily for tests)."""
    recorder.reset()
