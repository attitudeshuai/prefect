"""
Tests for the read-only state transition precheck API
(`POST /state_transitions/preview`).
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa

from prefect.server import models, schemas
from prefect.server.database import orm_models
from prefect.server.orchestration.preview_observability import (
    reset_preview_observations,
)
from prefect.server.schemas import states
from prefect.settings.context import temporary_settings
from prefect.types._datetime import now

pytestmark = pytest.mark.clear_db


@pytest.fixture(autouse=True)
def reset_observations():
    reset_preview_observations()
    yield
    reset_preview_observations()


def _item(
    run_type: str,
    run_id,
    state_type: str,
    *,
    name: str | None = None,
    message: str | None = None,
    force: bool = False,
    current_state: dict | None = None,
    state_details: dict | None = None,
) -> dict:
    state: dict = {"type": state_type}
    if name is not None:
        state["name"] = name
    if message is not None:
        state["message"] = message
    if state_details is not None:
        state["state_details"] = state_details
    item = {
        "run_type": run_type,
        "run_id": str(run_id),
        "state": state,
        "force": force,
    }
    if current_state is not None:
        item["current_state"] = current_state
    return item


async def _preview(client, *items) -> dict:
    response = await client.post(
        "/state_transitions/preview",
        json={"transitions": list(items)},
    )
    assert response.status_code == 200, response.text
    return response.json()


async def _flow_run_states_count(session, flow_run_id) -> int:
    result = await session.execute(
        sa.select(sa.func.count())
        .select_from(orm_models.FlowRunState)
        .where(orm_models.FlowRunState.flow_run_id == flow_run_id)
    )
    return result.scalar_one()


class TestPreviewVerdicts:
    async def test_accepted_preview_writes_nothing(self, flow_run, client, session):
        states_before = await _flow_run_states_count(session, flow_run.id)

        body = await _preview(
            client, _item("FLOW_RUN", flow_run.id, "RUNNING", name="Test State")
        )
        result = body["results"][0]

        assert result["ok"] is True
        assert result["status"] == "ACCEPT"
        assert result["rewritten"] is False
        assert result["state"]["type"] == "RUNNING"
        assert result["state"]["name"] == "Test State"

        # nothing was committed
        session.expire_all()
        run = await models.flow_runs.read_flow_run(
            session=session, flow_run_id=flow_run.id
        )
        assert run.state is None
        assert await _flow_run_states_count(session, flow_run.id) == states_before

    async def test_preview_then_real_submission_agree_on_accept(
        self, flow_run, client, session
    ):
        body = await _preview(
            client, _item("FLOW_RUN", flow_run.id, "RUNNING", name="Test State")
        )
        assert body["results"][0]["status"] == "ACCEPT"

        response = await client.post(
            f"/flow_runs/{flow_run.id}/set_state",
            json=dict(state=dict(type="RUNNING", name="Test State")),
        )
        assert response.status_code == 201, response.text
        assert response.json()["status"] == "ACCEPT"
        assert response.json()["state"]["name"] == "Test State"

    async def test_wait_for_scheduled_time(self, flow, client, session):
        scheduled = states.Scheduled(
            scheduled_time=now("UTC") + timedelta(hours=1)
        )
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.core.FlowRun(
                flow_id=flow.id, flow_version="0.1", state=scheduled
            ),
        )
        await session.commit()
        states_before = await _flow_run_states_count(session, flow_run.id)

        body = await _preview(
            client, _item("FLOW_RUN", flow_run.id, "RUNNING")
        )
        result = body["results"][0]
        assert result["status"] == "WAIT"
        assert result["details"]["delay_seconds"] >= 3500
        assert result["details"]["reason"] == "Scheduled time is in the future"
        assert result["state"] is None

        # run is untouched
        session.expire_all()
        run = await models.flow_runs.read_flow_run(
            session=session, flow_run_id=flow_run.id
        )
        assert run.state.type == states.StateType.SCHEDULED
        assert await _flow_run_states_count(session, flow_run.id) == states_before

    async def test_rejected_completed_with_persisted_result(self, flow, client, session):
        completed = states.Completed(data={"result": 42})
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.core.FlowRun(
                flow_id=flow.id, flow_version="0.1", state=completed
            ),
        )
        await session.commit()
        states_before = await _flow_run_states_count(session, flow_run.id)

        body = await _preview(
            client,
            _item(
                "FLOW_RUN",
                flow_run.id,
                "COMPLETED",
                state_details={},
                message="overwrite",
            ),
        )
        result = body["results"][0]
        assert result["ok"] is True
        assert result["status"] == "REJECT"
        assert "COMPLETED" in result["details"]["reason"]
        assert result["state"] is None
        assert result["rewritten"] is False

        # no state written, run stays completed
        session.expire_all()
        run = await models.flow_runs.read_flow_run(
            session=session, flow_run_id=flow_run.id
        )
        assert run.state.type == states.StateType.COMPLETED
        assert await _flow_run_states_count(session, flow_run.id) == states_before

        # a real submission made immediately after agrees
        response = await client.post(
            f"/flow_runs/{flow_run.id}/set_state",
            json=dict(state=dict(type="COMPLETED", message="overwrite")),
        )
        assert response.status_code in (200, 201)
        assert response.json()["status"] == "REJECT"
        assert "COMPLETED" in response.json()["details"]["reason"]

    async def test_abort_terminal_to_paused(self, flow, client, session):
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.core.FlowRun(
                flow_id=flow.id,
                flow_version="0.1",
                state=states.Crashed(),
            ),
        )
        await session.commit()

        body = await _preview(
            client, _item("FLOW_RUN", flow_run.id, "PAUSED")
        )
        result = body["results"][0]
        assert result["status"] == "ABORT"
        assert "terminal" in result["details"]["reason"]

        session.expire_all()
        run = await models.flow_runs.read_flow_run(
            session=session, flow_run_id=flow_run.id
        )
        assert run.state.type == states.StateType.CRASHED

    async def test_task_retry_is_reported_as_rewritten(
        self, flow_run, client, session
    ):
        task_run = await models.task_runs.create_task_run(
            session=session,
            task_run=schemas.actions.TaskRunCreate(
                flow_run_id=flow_run.id,
                task_key="retry-task",
                dynamic_key="0",
                state=states.Running(),
            ),
        )
        await session.commit()
        task_run.run_count = 1
        task_run.empirical_policy = schemas.core.TaskRunPolicy(
            retries=1, retry_delay=0
        )
        await session.commit()

        body = await _preview(
            client, _item("TASK_RUN", task_run.id, "FAILED", message="boom")
        )
        result = body["results"][0]
        assert result["status"] == "REJECT"
        assert result["details"]["reason"] == "Retrying"
        assert result["rewritten"] is True
        assert result["state"]["type"] == "SCHEDULED"
        assert result["state"]["name"] == "AwaitingRetry"

        # the run did not leave RUNNING and its run count was not changed
        session.expire_all()
        stored = await models.task_runs.read_task_run(
            session=session, task_run_id=task_run.id
        )
        assert stored.state.type == states.StateType.RUNNING
        assert stored.run_count == 1

    async def test_terminal_run_preview_matches_real_submission(
        self, flow, client, session
    ):
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.core.FlowRun(
                flow_id=flow.id,
                flow_version="0.1",
                state=states.Failed(),
                run_count=1,
            ),
        )
        await session.commit()

        body = await _preview(
            client, _item("FLOW_RUN", flow_run.id, "RUNNING")
        )
        preview_result = body["results"][0]

        response = await client.post(
            f"/flow_runs/{flow_run.id}/set_state",
            json=dict(state=dict(type="RUNNING")),
        )
        real_result = response.json()

        assert preview_result["status"] == real_result["status"]
        assert preview_result["state"]["type"] == real_result["state"]["type"]

    async def test_batch_mixes_flow_and_task_runs(self, flow_run, client, session):
        task_run = await models.task_runs.create_task_run(
            session=session,
            task_run=schemas.actions.TaskRunCreate(
                flow_run_id=flow_run.id, task_key="k", dynamic_key="0"
            ),
        )
        await session.commit()

        body = await _preview(
            client,
            _item("FLOW_RUN", flow_run.id, "RUNNING"),
            _item("TASK_RUN", task_run.id, "RUNNING"),
        )
        assert len(body["results"]) == 2
        assert {r["run_type"] for r in body["results"]} == {"FLOW_RUN", "TASK_RUN"}
        assert all(r["ok"] for r in body["results"])
        assert all(r["status"] == "ACCEPT" for r in body["results"])

        # the task's enclosing flow is not running so a real submission would
        # abort; the precheck must surface that verdict without writing
        task_result = next(
            r for r in body["results"] if r["run_type"] == "TASK_RUN"
        )
        assert task_result["status"] == "ABORT"
        assert task_result["details"]["reason"] == (
            "The enclosing flow must be running to begin task execution."
        )


class TestPreviewErrors:
    async def test_run_not_found(self, client):
        body = await _preview(
            client, _item("FLOW_RUN", uuid4(), "RUNNING")
        )
        result = body["results"][0]
        assert result["ok"] is False
        assert result["error_code"] == "RUN_NOT_FOUND"
        assert result["status"] is None

    async def test_matching_snapshot_is_accepted(self, flow, client, session):
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.core.FlowRun(
                flow_id=flow.id, flow_version="0.1", state=states.Scheduled()
            ),
        )
        await session.commit()
        snapshot = flow_run.state.as_state().model_dump(mode="json")

        body = await _preview(
            client,
            _item(
                "FLOW_RUN",
                flow_run.id,
                "PENDING",
                current_state=snapshot,
            ),
        )
        assert body["results"][0]["ok"] is True
        assert body["results"][0]["status"] == "ACCEPT"

        # no state was written
        session.expire_all()
        stored = await models.flow_runs.read_flow_run(
            session=session, flow_run_id=flow_run.id
        )
        assert stored.state.type == states.StateType.SCHEDULED

    async def test_stale_snapshot_is_rejected_not_evaluated(
        self, flow, client, session
    ):
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.core.FlowRun(
                flow_id=flow.id, flow_version="0.1", state=states.Scheduled()
            ),
        )
        await session.commit()
        snapshot = flow_run.state.as_state().model_dump(mode="json")
        snapshot["id"] = str(uuid4())
        states_before = await _flow_run_states_count(session, flow_run.id)

        body = await _preview(
            client,
            _item(
                "FLOW_RUN",
                flow_run.id,
                "PENDING",
                current_state=snapshot,
            ),
        )
        result = body["results"][0]
        assert result["ok"] is False
        assert result["error_code"] == "STALE_STATE_SNAPSHOT"
        assert "state_id" in result["stale_snapshot_fields"]
        assert result["server_current_state"]["id"] is not None
        assert result["status"] is None
        assert await _flow_run_states_count(session, flow_run.id) == states_before

    async def test_invalid_target_state_is_422(self, flow_run, client):
        response = await client.post(
            "/state_transitions/preview",
            json={
                "transitions": [
                    {
                        "run_type": "FLOW_RUN",
                        "run_id": str(flow_run.id),
                        "state": {"type": "NOT_A_STATE"},
                    }
                ]
            },
        )
        assert response.status_code == 422

    async def test_empty_and_oversized_batches_are_422(self, flow_run, client):
        response = await client.post(
            "/state_transitions/preview", json={"transitions": []}
        )
        assert response.status_code == 422

        response = await client.post(
            "/state_transitions/preview",
            json={
                "transitions": [
                    _item("FLOW_RUN", flow_run.id, "RUNNING")
                    for _ in range(51)
                ]
            },
        )
        assert response.status_code == 422


class TestPreviewConcurrency:
    async def _deployment_with_full_limit(
        self, session, flow, strategy, limit=1
    ):
        deployment = await models.deployments.create_deployment(
            session=session,
            deployment=schemas.core.Deployment(
                name=f"dep-{uuid4()}",
                flow_id=flow.id,
                concurrency_limit=limit,
                concurrency_options={"collision_strategy": strategy.value},
            ),
        )
        await session.commit()
        acquired = await models.concurrency_limits_v2.bulk_increment_active_slots(
            session=session,
            concurrency_limit_ids=[deployment.concurrency_limit_id],
            slots=1,
        )
        assert acquired is True
        await session.commit()
        return deployment

    async def test_full_deployment_limit_enqueue_rejects_without_acquiring(
        self, flow, client, session
    ):
        deployment = await self._deployment_with_full_limit(
            session,
            flow,
            schemas.core.ConcurrencyLimitStrategy.ENQUEUE,
        )
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.actions.FlowRunCreate(
                flow_id=flow.id, deployment_id=deployment.id
            ),
        )
        await session.commit()

        body = await _preview(
            client, _item("FLOW_RUN", flow_run.id, "PENDING")
        )
        result = body["results"][0]
        assert result["status"] == "REJECT"
        assert result["state"]["type"] == "SCHEDULED"
        assert result["state"]["name"] == "AwaitingConcurrencySlot"
        assert result["details"]["reason"] == "Deployment concurrency limit reached."

        # capacity untouched, no lease minted, run still stateless
        limit = await models.concurrency_limits_v2.read_concurrency_limit(
            session, concurrency_limit_id=deployment.concurrency_limit_id
        )
        assert limit.active_slots == 1
        session.expire_all()
        run = await models.flow_runs.read_flow_run(
            session=session, flow_run_id=flow_run.id
        )
        assert run.state is None

    async def test_full_deployment_limit_cancel_new(self, flow, client, session):
        deployment = await self._deployment_with_full_limit(
            session, flow, schemas.core.ConcurrencyLimitStrategy.CANCEL_NEW
        )
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.actions.FlowRunCreate(
                flow_id=flow.id, deployment_id=deployment.id
            ),
        )
        await session.commit()

        body = await _preview(
            client, _item("FLOW_RUN", flow_run.id, "PENDING")
        )
        result = body["results"][0]
        assert result["status"] == "REJECT"
        assert result["state"]["type"] == "CANCELLED"

    async def test_available_deployment_capacity_is_not_reserved(
        self, flow, client, session
    ):
        deployment = await models.deployments.create_deployment(
            session=session,
            deployment=schemas.core.Deployment(
                name=f"dep-{uuid4()}",
                flow_id=flow.id,
                concurrency_limit=1,
                concurrency_options={
                    "collision_strategy": (
                        schemas.core.ConcurrencyLimitStrategy.ENQUEUE.value
                    )
                },
            ),
        )
        await session.commit()
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.actions.FlowRunCreate(
                flow_id=flow.id, deployment_id=deployment.id
            ),
        )
        await session.commit()

        body = await _preview(
            client, _item("FLOW_RUN", flow_run.id, "PENDING")
        )
        result = body["results"][0]
        assert result["status"] == "ACCEPT"
        assert (
            result["state"]["state_details"]["deployment_concurrency_lease_id"]
            is None
        )

        limit = await models.concurrency_limits_v2.read_concurrency_limit(
            session, concurrency_limit_id=deployment.concurrency_limit_id
        )
        assert limit.active_slots == 0

    async def test_v1_tag_limit_full_waits_without_taking_a_slot(
        self, flow, client, session
    ):
        running_flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.core.FlowRun(
                flow_id=flow.id, flow_version="0.1", state=states.Running()
            ),
        )
        await models.concurrency_limits.create_concurrency_limit(
            session,
            schemas.core.ConcurrencyLimit(
                tag="preview-tag",
                concurrency_limit=1,
                active_slots=["someone-else"],
            ),
        )
        task_run = await models.task_runs.create_task_run(
            session=session,
            task_run=schemas.actions.TaskRunCreate(
                flow_run_id=running_flow_run.id,
                task_key="tagged",
                dynamic_key="0",
                tags=["preview-tag"],
            ),
        )
        await session.commit()

        body = await _preview(
            client, _item("TASK_RUN", task_run.id, "RUNNING")
        )
        result = body["results"][0]
        assert result["status"] == "WAIT"
        assert "preview-tag" in result["details"]["reason"]

        limit = await models.concurrency_limits.read_concurrency_limit_by_tag(
            session, tag="preview-tag"
        )
        assert limit.active_slots == ["someone-else"]


class TestPreviewObservations:
    async def _future_scheduled_flow_run(self, session, flow):
        flow_run = await models.flow_runs.create_flow_run(
            session=session,
            flow_run=schemas.core.FlowRun(
                flow_id=flow.id,
                flow_version="0.1",
                state=states.Scheduled(
                    scheduled_time=now("UTC") + timedelta(hours=1)
                ),
            ),
        )
        await session.commit()
        return flow_run

    async def test_records_counts_and_reasons_only(self, flow, client, session):
        flow_run = await self._future_scheduled_flow_run(session, flow)
        await _preview(client, _item("FLOW_RUN", flow_run.id, "RUNNING"))

        response = await client.get("/state_transitions/preview/observations")
        assert response.status_code == 200
        body = response.text
        snapshot = response.json()
        assert snapshot["status_counts"]["WAIT"] == 1
        assert (
            snapshot["reason_counts"]["Scheduled time is in the future"] == 1
        )
        # rule identities never leak into the observation surface
        assert "core_policy" not in body
        assert "OrchestrationRule" not in body
        assert "WaitForScheduledTime" not in body

    async def test_can_be_disabled(self, flow, client, session):
        flow_run = await self._future_scheduled_flow_run(session, flow)
        with temporary_settings(
            {
                "server.orchestration.preview_observations_enabled": False,
            }
        ):
            await _preview(
                client, _item("FLOW_RUN", flow_run.id, "RUNNING")
            )
            response = await client.get(
                "/state_transitions/preview/observations"
            )
            snapshot = response.json()
            assert snapshot["enabled"] is False
            assert snapshot["evaluated"] == 0
            assert snapshot["recorded"] == 0

    async def test_sample_rate_zero_records_nothing(self, flow, client, session):
        flow_run = await self._future_scheduled_flow_run(session, flow)
        with temporary_settings(
            {"server.orchestration.preview_observations_sample_rate": 0}
        ):
            await _preview(
                client, _item("FLOW_RUN", flow_run.id, "RUNNING")
            )
            snapshot = (
                await client.get("/state_transitions/preview/observations")
            ).json()
            assert snapshot["evaluated"] == 0

    async def test_recent_events_are_bounded(self, flow, client, session):
        with temporary_settings(
            {"server.orchestration.preview_observations_max_events": 2}
        ):
            for _ in range(3):
                flow_run = await self._future_scheduled_flow_run(session, flow)
                await _preview(
                    client, _item("FLOW_RUN", flow_run.id, "RUNNING")
                )
            snapshot = (
                await client.get("/state_transitions/preview/observations")
            ).json()
            assert len(snapshot["recent"]) == 2
