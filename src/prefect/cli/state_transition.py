"""
State transition command — native cyclopts implementation.

Precheck flow/task run state transitions without committing them and inspect
precheck observations.
"""

from __future__ import annotations

from typing import Annotated, Optional
from uuid import UUID

import cyclopts
import orjson

import prefect.cli._app as _cli
from prefect.cli._utilities import (
    exit_with_error,
    with_cli_exception_handling,
)

state_transition_app: cyclopts.App = cyclopts.App(
    name="state-transition",
    alias="state-transitions",
    help="Precheck state transitions without committing them.",
    version_flags=[],
    help_flags=["--help"],
)


@state_transition_app.command(name="preview")
@with_cli_exception_handling
async def preview(
    *,
    run_type: Annotated[
        str,
        cyclopts.Parameter(
            "--type",
            help="The type of run: 'flow' or 'task'.",
        ),
    ],
    id: Annotated[
        list[UUID],
        cyclopts.Parameter(
            "--id",
            help=(
                "A flow run or task run id to precheck. Repeat to precheck a"
                " batch."
            ),
        ),
    ],
    state: Annotated[
        str,
        cyclopts.Parameter(
            "--state",
            help="The proposed state type, e.g. RUNNING, COMPLETED, FAILED.",
        ),
    ],
    name: Annotated[
        Optional[str],
        cyclopts.Parameter("--name", help="An optional proposed state name."),
    ] = None,
    message: Annotated[
        Optional[str],
        cyclopts.Parameter(
            "--message", help="An optional proposed state message."
        ),
    ] = None,
    force: Annotated[
        bool,
        cyclopts.Parameter(
            "--force",
            help="Precheck with the minimal policy used by forced submissions.",
        ),
    ] = False,
    snapshot: Annotated[
        bool,
        cyclopts.Parameter(
            "--snapshot-current-state",
            help=(
                "Attach the run's current server state as a snapshot; the"
                " precheck fails if it is stale."
            ),
        ),
    ] = False,
    output: Annotated[
        Optional[str],
        cyclopts.Parameter(
            "--output",
            alias="-o",
            help="Specify an output format. Currently supports: json",
        ),
    ] = None,
):
    """
    Show whether one or more state transitions would be accepted, rejected,
    asked to wait, or aborted, and what state would be committed. Nothing is
    written to the server.
    """
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.objects import StateType
    from prefect.client.schemas.state_transitions import (
        StateTransitionPreviewItem,
        StateTransitionRunType,
    )
    from prefect.exceptions import ObjectNotFound
    from prefect.states import State, to_state_create

    if output and output.lower() != "json":
        exit_with_error("Only 'json' output format is supported.")

    run_type_value = run_type.strip().lower()
    if run_type_value in ("flow", "flow_run", "flow-run"):
        transition_run_type = StateTransitionRunType.FLOW_RUN
    elif run_type_value in ("task", "task_run", "task-run"):
        transition_run_type = StateTransitionRunType.TASK_RUN
    else:
        exit_with_error("--type must be either 'flow' or 'task'.")

    if not id:
        exit_with_error("Provide at least one --id.")

    try:
        proposed_state_type = StateType(state.strip().upper())
    except ValueError:
        valid = ", ".join(t.value for t in StateType)
        exit_with_error(f"Invalid --state {state!r}. Valid types: {valid}.")

    async with get_client() as client:
        items = []
        for run_id in id:
            current_state = None
            if snapshot:
                try:
                    if transition_run_type == StateTransitionRunType.FLOW_RUN:
                        run = await client.read_flow_run(run_id)
                    else:
                        run = await client.read_task_run(run_id)
                except ObjectNotFound:
                    exit_with_error(f"Run '{run_id}' not found!")
                current_state = run.state

            items.append(
                StateTransitionPreviewItem(
                    run_type=transition_run_type,
                    run_id=run_id,
                    state=to_state_create(
                        State(
                            type=proposed_state_type,
                            name=name,
                            message=message,
                        )
                    ),
                    force=force,
                    current_state=current_state,
                )
            )

        response = await client.preview_state_transitions(items)

    if output and output.lower() == "json":
        _cli.console.print(
            orjson.dumps(
                response.model_dump(mode="json"), option=orjson.OPT_INDENT_2
            ).decode(),
            soft_wrap=True,
        )
        return

    from rich.table import Table

    table = Table(title="State transition precheck")
    table.add_column("Run ID", style="cyan", no_wrap=True)
    table.add_column("Verdict")
    table.add_column("Rewritten")
    table.add_column("Would-be state")
    table.add_column("Reason / error")

    any_error = False
    for result in response.results:
        if not result.ok:
            any_error = True
            table.add_row(
                str(result.run_id),
                "ERROR",
                "-",
                "-",
                f"{result.error_code.value}: {result.error_message}",
                style="red",
            )
            continue

        governed = result.state
        governed_label = (
            f"{governed.type.value}:{governed.name}" if governed else "-"
        )
        reason = getattr(result.details, "reason", None) or ""
        style = {
            "ACCEPT": "green",
            "REJECT": "yellow",
            "ABORT": "red",
            "WAIT": "blue",
        }.get(result.status.value, "")
        table.add_row(
            str(result.run_id),
            result.status.value,
            "yes" if result.rewritten else "no",
            governed_label,
            reason,
            style=style,
        )

    _cli.console.print(table)

    if any_error:
        exit_with_error("One or more prechecks could not be evaluated.")


@state_transition_app.command(name="observations")
@with_cli_exception_handling
async def observations(
    *,
    output: Annotated[
        Optional[str],
        cyclopts.Parameter(
            "--output",
            alias="-o",
            help="Specify an output format. Currently supports: json",
        ),
    ] = None,
):
    """View counts and reasons recorded for rejected or rewritten prechecks."""
    from prefect.client.orchestration import get_client

    if output and output.lower() != "json":
        exit_with_error("Only 'json' output format is supported.")

    async with get_client() as client:
        snapshot = await client.read_state_transition_preview_observations()

    if output and output.lower() == "json":
        _cli.console.print(
            orjson.dumps(
                snapshot.model_dump(mode="json"), option=orjson.OPT_INDENT_2
            ).decode(),
            soft_wrap=True,
        )
        return

    from rich.table import Table

    _cli.console.print(
        f"Observations enabled: {snapshot.enabled}, sample rate:"
        f" {snapshot.sample_rate}, evaluated: {snapshot.evaluated}, recorded:"
        f" {snapshot.recorded}, rewritten: {snapshot.rewritten_count}"
    )

    counts_table = Table(title="Outcome counts")
    counts_table.add_column("Status")
    counts_table.add_column("Count")
    for status_value, count in sorted(snapshot.status_counts.items()):
        counts_table.add_row(status_value, str(count))
    _cli.console.print(counts_table)

    reasons_table = Table(title="Reasons")
    reasons_table.add_column("Reason")
    reasons_table.add_column("Count")
    for reason, count in sorted(
        snapshot.reason_counts.items(), key=lambda item: -item[1]
    ):
        reasons_table.add_row(reason, str(count))
    _cli.console.print(reasons_table)
