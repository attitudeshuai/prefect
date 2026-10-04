"""
Client schemas for the read-only state transition precheck endpoint.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional
from uuid import UUID

from pydantic import Field, model_validator

from prefect._internal.schemas.bases import PrefectBaseModel
from prefect.client.schemas.actions import StateCreate
from prefect.client.schemas.objects import State, StateType
from prefect.client.schemas.responses import (
    SetStateStatus,
    StateResponseDetails,
)
from prefect.utilities.collections import AutoEnum

#: Maximum number of transitions evaluated in a single precheck request.
PREVIEW_TRANSITION_LIMIT = 50


class StateTransitionRunType(AutoEnum):
    """The kind of run a proposed state transition targets."""

    FLOW_RUN = AutoEnum.auto()
    TASK_RUN = AutoEnum.auto()


class StateTransitionPreviewErrorCode(AutoEnum):
    """
    Errors that prevent a precheck verdict from being produced. These are
    distinct from a REJECT/ABORT/WAIT verdict.
    """

    RUN_NOT_FOUND = AutoEnum.auto()
    STALE_STATE_SNAPSHOT = AutoEnum.auto()
    INVALID_TARGET_STATE = AutoEnum.auto()
    INVALID_STATE_DETAILS = AutoEnum.auto()


class StateTransitionPreviewItem(PrefectBaseModel):
    """A single proposed transition to precheck."""

    run_type: StateTransitionRunType
    run_id: UUID
    state: StateCreate
    force: bool = False
    current_state: Optional[State] = None


class StateTransitionPreviewRequest(PrefectBaseModel):
    """A batch of proposed transitions to precheck."""

    transitions: List[StateTransitionPreviewItem] = Field(min_length=1)

    @model_validator(mode="after")
    def enforce_batch_limit(self) -> StateTransitionPreviewRequest:
        if len(self.transitions) > PREVIEW_TRANSITION_LIMIT:
            raise ValueError(
                "A precheck batch can contain at most"
                f" {PREVIEW_TRANSITION_LIMIT} transitions"
            )
        return self


class StateTransitionPreviewResult(PrefectBaseModel):
    """The precheck outcome for one transition."""

    run_type: StateTransitionRunType
    run_id: UUID
    ok: bool
    error_code: Optional[StateTransitionPreviewErrorCode] = None
    error_message: Optional[str] = None
    stale_snapshot_fields: Optional[List[str]] = None
    server_current_state: Optional[State] = None
    status: Optional[SetStateStatus] = None
    details: Optional[StateResponseDetails] = None
    proposed_state: Optional[State] = None
    state: Optional[State] = None
    rewritten: bool = False


class StateTransitionPreviewResponse(PrefectBaseModel):
    """Per-item results for a batch of transition prechecks."""

    results: List[StateTransitionPreviewResult] = Field(default_factory=list)


class StateTransitionPreviewObservationEvent(PrefectBaseModel):
    """One recorded rejected, waiting, aborted, or rewritten precheck."""

    occurred: datetime
    run_type: StateTransitionRunType
    status: SetStateStatus
    rewritten: bool = False
    reason: Optional[str] = None
    final_state_type: Optional[StateType] = None
    final_state_name: Optional[str] = None


class StateTransitionPreviewObservations(PrefectBaseModel):
    """Aggregate, reason-only view of precheck outcomes for a server."""

    enabled: bool
    sample_rate: float
    evaluated: int = 0
    recorded: int = 0
    status_counts: dict[str, int] = Field(default_factory=dict)
    rewritten_count: int = 0
    reason_counts: dict[str, int] = Field(default_factory=dict)
    recent: List[StateTransitionPreviewObservationEvent] = Field(default_factory=list)
