"""
Routes for read-only state transition prechecks.

A precheck replays the currently effective orchestration rules for one or
more proposed flow- or task-run state transitions and returns the verdict
(accept / reject / wait / abort, any rewritten state, and the reason) without
committing anything.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import Body, Depends
from pydantic import ValidationError

import prefect.server.api.dependencies as dependencies
import prefect.server.models as models
import prefect.server.schemas as schemas
from prefect.server.database import PrefectDBInterface, provide_database_interface
from prefect.server.exceptions import ObjectNotFoundError
from prefect.server.orchestration import dependencies as orchestration_dependencies
from prefect.server.orchestration.policies import (
    FlowRunOrchestrationPolicy,
    TaskRunOrchestrationPolicy,
)
from prefect.server.orchestration.preview import StaleStateSnapshotError
from prefect.server.orchestration.preview_observability import (
    read_preview_observations,
    record_preview_observation,
)
from prefect.server.schemas import states
from prefect.server.schemas.responses import OrchestrationResult
from prefect.server.schemas.state_transitions import (
    StateTransitionPreviewErrorCode,
    StateTransitionPreviewItem,
    StateTransitionPreviewObservations,
    StateTransitionPreviewRequest,
    StateTransitionPreviewResponse,
    StateTransitionPreviewResult,
    StateTransitionRunType,
)
from prefect.server.utilities.server import PrefectRouter

router: PrefectRouter = PrefectRouter(
    prefix="/state_transitions", tags=["State Transitions"]
)


#: state_details fields stamped by bookkeeping transforms on every write; they
#: differ between a submitted state and the committed state by design and do
#: not constitute an orchestration rewrite.
_BOOKKEEPING_DETAIL_FIELDS = {
    "flow_run_id",
    "task_run_id",
    "child_flow_run_id",
    "deployment_concurrency_lease_id",
    "transition_id",
}


def _state_semantic_signature(state: Optional[states.State]) -> Optional[tuple]:
    if state is None:
        return None
    details = {
        key: value
        for key, value in state.state_details.model_dump().items()
        if key not in _BOOKKEEPING_DETAIL_FIELDS and value is not None
    }
    return (
        state.type,
        state.name,
        state.message,
        state.data,
        details,
    )


def _is_rewritten(
    submitted: Optional[states.State],
    governed: Optional[states.State],
) -> bool:
    """True if orchestration changed the state beyond run bookkeeping."""
    if governed is None:
        return False
    return _state_semantic_signature(submitted) != _state_semantic_signature(governed)


def _error_result(
    item: StateTransitionPreviewItem,
    error_code: StateTransitionPreviewErrorCode,
    error_message: str,
    *,
    stale_snapshot_fields: Optional[list[str]] = None,
    server_current_state: Optional[states.State] = None,
) -> StateTransitionPreviewResult:
    return StateTransitionPreviewResult(
        run_type=item.run_type,
        run_id=item.run_id,
        ok=False,
        error_code=error_code,
        error_message=error_message,
        stale_snapshot_fields=stale_snapshot_fields,
        server_current_state=server_current_state,
        proposed_state=schemas.states.State.model_validate(item.state),
    )


async def _preview_one(
    db: PrefectDBInterface,
    item: StateTransitionPreviewItem,
    flow_policy: type[FlowRunOrchestrationPolicy],
    task_policy: type[TaskRunOrchestrationPolicy],
    orchestration_parameters: Dict[str, Any],
    client_version: Optional[str],
) -> StateTransitionPreviewResult:
    # Validate the target state independently of the transition verdict.
    try:
        submitted = schemas.states.State.model_validate(item.state)
    except ValidationError as exc:
        return _error_result(
            item,
            StateTransitionPreviewErrorCode.INVALID_TARGET_STATE,
            f"The proposed state is not a valid state: {exc.errors()}",
        )

    if not isinstance(submitted.type, states.StateType):
        return _error_result(
            item,
            StateTransitionPreviewErrorCode.INVALID_TARGET_STATE,
            f"{submitted.type!r} is not a valid state type.",
        )

    try:
        # rules mutate the proposed state in place; keep the submitted copy
        # pristine so the response can show what was changed.
        governed_input = submitted.model_copy(deep=True)

        async with db.session_context(begin_transaction=True) as session:
            # Evaluate inside a SAVEPOINT and roll the savepoint back: every
            # rule still runs against real data, but none of the in-session
            # bookkeeping (run counts, timestamps, state rows) is committed.
            # External side effects (leases, slot counters) are bypassed by
            # the dry-run rules themselves.
            savepoint = await session.begin_nested()
            preview_error: Optional[Exception] = None
            try:
                if item.run_type == StateTransitionRunType.FLOW_RUN:
                    result: OrchestrationResult = (
                        await models.flow_runs.preview_flow_run_state(
                            session=session,
                            flow_run_id=item.run_id,
                            state=governed_input,
                            force=item.force,
                            flow_policy=flow_policy,
                            orchestration_parameters=dict(orchestration_parameters),
                            client_version=client_version,
                            current_state=item.current_state,
                        )
                    )
                else:
                    result = await models.task_runs.preview_task_run_state(
                        session=session,
                        task_run_id=item.run_id,
                        state=governed_input,
                        force=item.force,
                        task_policy=task_policy,
                        orchestration_parameters=dict(orchestration_parameters),
                        current_state=item.current_state,
                    )
            except (ObjectNotFoundError, StaleStateSnapshotError) as exc:
                preview_error = exc
            await savepoint.rollback()

    except ObjectNotFoundError as exc:
        return _error_result(
            item,
            StateTransitionPreviewErrorCode.RUN_NOT_FOUND,
            str(exc),
        )
    except StaleStateSnapshotError as exc:
        return _error_result(
            item,
            StateTransitionPreviewErrorCode.STALE_STATE_SNAPSHOT,
            str(exc),
            stale_snapshot_fields=exc.differences,
            server_current_state=exc.server_state,
        )
    except ValidationError as exc:
        return _error_result(
            item,
            StateTransitionPreviewErrorCode.INVALID_STATE_DETAILS,
            f"The proposed state details do not hold: {exc.errors()}",
        )

    if preview_error is not None:
        if isinstance(preview_error, ObjectNotFoundError):
            return _error_result(
                item,
                StateTransitionPreviewErrorCode.RUN_NOT_FOUND,
                str(preview_error),
            )
        return _error_result(
            item,
            StateTransitionPreviewErrorCode.STALE_STATE_SNAPSHOT,
            str(preview_error),
            stale_snapshot_fields=preview_error.differences,
            server_current_state=preview_error.server_state,
        )

    rewritten = _is_rewritten(submitted, result.state)

    preview_result = StateTransitionPreviewResult(
        run_type=item.run_type,
        run_id=item.run_id,
        ok=True,
        status=result.status,
        details=result.details,
        proposed_state=submitted,
        state=result.state,
        rewritten=rewritten,
    )

    record_preview_observation(item.run_type, result, rewritten)

    return preview_result


@router.post("/preview")
async def preview_state_transitions(
    request: StateTransitionPreviewRequest = Body(
        ..., description="The transitions to precheck."
    ),
    db: PrefectDBInterface = Depends(provide_database_interface),
    flow_policy: type[FlowRunOrchestrationPolicy] = Depends(
        orchestration_dependencies.provide_flow_policy
    ),
    task_policy: type[TaskRunOrchestrationPolicy] = Depends(
        orchestration_dependencies.provide_task_policy
    ),
    flow_orchestration_parameters: Dict[str, Any] = Depends(
        orchestration_dependencies.provide_flow_orchestration_parameters
    ),
    task_orchestration_parameters: Dict[str, Any] = Depends(
        orchestration_dependencies.provide_task_orchestration_parameters
    ),
    client_version: Optional[str] = Depends(
        dependencies.get_prefect_client_version
    ),
    api_version: str = Depends(dependencies.provide_request_api_version),
) -> StateTransitionPreviewResponse:
    """
    Precheck one or more state transitions without committing them.

    Each item is evaluated independently against the orchestration rules
    currently in effect. A verdict mirrors a real submission made at the same
    time: accepted transitions return the state that would be committed (with
    `rewritten=true` when orchestration alters it), while rejected, waiting,
    and aborted transitions carry the orchestration reason. Missing runs,
    stale current-state snapshots, and invalid target states are reported as
    per-item errors rather than verdicts.
    """
    flow_parameters = {**flow_orchestration_parameters, "api-version": api_version}
    task_parameters = {**task_orchestration_parameters, "api-version": api_version}

    results = []
    for item in request.transitions:
        parameters = (
            flow_parameters
            if item.run_type == StateTransitionRunType.FLOW_RUN
            else task_parameters
        )
        results.append(
            await _preview_one(
                db=db,
                item=item,
                flow_policy=flow_policy,
                task_policy=task_policy,
                orchestration_parameters=parameters,
                client_version=client_version,
            )
        )

    return StateTransitionPreviewResponse(results=results)


@router.get("/preview/observations")
async def read_state_transition_preview_observations() -> (
    StateTransitionPreviewObservations
):
    """
    View counts and reasons of rejected, waiting, aborted, and rewritten
    prechecks recorded by this server process. Rule identifiers are never
    included; recording can be disabled or sampled via the
    `PREFECT_SERVER_ORCHESTRATION_PREVIEW_OBSERVATIONS_*` settings.
    """
    return read_preview_observations()
