"""
Request and response schemas for the read-only state transition precheck.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional
from uuid import UUID

from pydantic import Field, model_validator

from prefect.server.schemas import actions, states
from prefect.server.schemas.responses import (
    SetStateStatus,
    StateResponseDetails,
)
from prefect.server.utilities.schemas import PrefectBaseModel
from prefect.utilities.collections import AutoEnum

#: Maximum number of transitions evaluated in a single precheck request.
PREVIEW_TRANSITION_LIMIT = 50


class StateTransitionRunType(AutoEnum):
    """The kind of run a proposed state transition targets."""

    FLOW_RUN = AutoEnum.auto()
    TASK_RUN = AutoEnum.auto()


class StateTransitionPreviewErrorCode(AutoEnum):
    """
    Errors that prevent a precheck verdict from being produced.

    These are distinct from a REJECT/ABORT/WAIT verdict: the transition itself
    was never evaluated because the request could not be honored.
    """

    RUN_NOT_FOUND = AutoEnum.auto()
    STALE_STATE_SNAPSHOT = AutoEnum.auto()
    INVALID_TARGET_STATE = AutoEnum.auto()
    INVALID_STATE_DETAILS = AutoEnum.auto()


class StateTransitionPreviewItem(PrefectBaseModel):
    """A single proposed transition to precheck."""

    run_type: StateTransitionRunType = Field(
        default=..., description="Whether the target is a flow run or a task run."
    )
    run_id: UUID = Field(
        default=..., description="The id of the flow run or task run."
    )
    state: actions.StateCreate = Field(
        default=..., description="The intended state to evaluate."
    )
    force: bool = Field(
        default=False,
        description=(
            "If true, evaluate the transition with the minimal policy, matching a"
            " forced real submission."
        ),
    )
    current_state: Optional[states.State] = Field(
        default=None,
        description=(
            "An optional snapshot of the state the caller currently believes the"
            " run to be in. When provided, it must match the server's current"
            " state; otherwise the item fails with STALE_STATE_SNAPSHOT."
        ),
    )


class StateTransitionPreviewRequest(PrefectBaseModel):
    """A batch of proposed transitions to precheck."""

    transitions: List[StateTransitionPreviewItem] = Field(
        default=...,
        min_length=1,
    )

    @model_validator(mode="after")
    def enforce_batch_limit(self) -> StateTransitionPreviewRequest:
        if len(self.transitions) > PREVIEW_TRANSITION_LIMIT:
            raise ValueError(
                "A precheck batch can contain at most"
                f" {PREVIEW_TRANSITION_LIMIT} transitions"
            )
        return self


class StateTransitionPreviewResult(PrefectBaseModel):
    """
    The precheck outcome for one transition.

    A successful evaluation sets `ok=True` and reports the orchestration
    verdict: `status` and `details` mirror a real submission, `state` is the
    governed state that would be committed, and `rewritten` indicates that the
    governed state differs from the proposed state. A failed evaluation sets
    `ok=False` with an explicit `error_code`; no verdict is produced.
    """

    run_type: StateTransitionRunType
    run_id: UUID
    ok: bool
    error_code: Optional[StateTransitionPreviewErrorCode] = None
    error_message: Optional[str] = None
    stale_snapshot_fields: Optional[List[str]] = None
    server_current_state: Optional[states.State] = None
    status: Optional[SetStateStatus] = None
    details: Optional[StateResponseDetails] = None
    proposed_state: Optional[states.State] = None
    state: Optional[states.State] = None
    rewritten: bool = False


class StateTransitionPreviewResponse(PrefectBaseModel):
    """Per-item results for a batch of transition prechecks."""

    results: List[StateTransitionPreviewResult] = Field(default_factory=list)


class StateTransitionPreviewObservationEvent(PrefectBaseModel):
    """One recorded rejected, waiting, aborted, or rewritten precheck."""

    occurred: datetime = Field(
        default=...,
        description="When the precheck was evaluated (UTC).",
    )
    run_type: StateTransitionRunType
    status: SetStateStatus
    rewritten: bool = False
    reason: Optional[str] = Field(
        default=None,
        description=(
            "The orchestration reason for the outcome. Rule identifiers are"
            " never recorded."
        ),
    )
    final_state_type: Optional[states.StateType] = None
    final_state_name: Optional[str] = None


class StateTransitionPreviewObservations(PrefectBaseModel):
    """Aggregate, reason-only view of precheck outcomes for this server."""

    enabled: bool
    sample_rate: float
    evaluated: int = Field(
        default=0,
        description="Total precheck outcomes considered for recording.",
    )
    recorded: int = Field(
        default=0,
        description="Total observations kept after sampling.",
    )
    status_counts: dict[str, int] = Field(default_factory=dict)
    rewritten_count: int = 0
    reason_counts: dict[str, int] = Field(
        default_factory=dict,
        description="How often each orchestration reason was observed.",
    )
    recent: List[StateTransitionPreviewObservationEvent] = Field(default_factory=list)
