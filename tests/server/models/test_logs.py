from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select

from prefect.server import models
from prefect.server.schemas.actions import LogCreate
from prefect.server.schemas.core import Log
from prefect.server.schemas.filters import (
    LogFilter,
    LogFilterFlowRunId,
    LogFilterLevel,
    LogFilterStructuredFields,
    LogFilterTaskRunId,
    LogFilterTimestamp,
)
from prefect.server.schemas.sorting import LogSort
from prefect.types._datetime import now

pytestmark = pytest.mark.clear_db

NOW = now("UTC")


@pytest.fixture
def flow_run_id():
    yield uuid4()


@pytest.fixture
def task_run_id():
    yield uuid4()


@pytest.fixture
def other_flow_run_id():
    yield uuid4()


@pytest.fixture
def log_data(client, flow_run_id, task_run_id, other_flow_run_id):
    yield [
        LogCreate(
            name="prefect.flow_run",
            level=10,
            message="Ahoy, captain",
            timestamp=NOW,
            flow_run_id=flow_run_id,
        ),
        LogCreate(
            name="prefect.flow_run",
            level=20,
            message="Aye-aye, captain!",
            timestamp=(NOW + timedelta(minutes=1)),
            flow_run_id=flow_run_id,
        ),
        LogCreate(
            name="prefect.task_run",
            level=50,
            message="Black flag ahead, captain!",
            timestamp=(NOW + timedelta(hours=1)),
            flow_run_id=flow_run_id,
            task_run_id=task_run_id,
        ),
    ]


@pytest.fixture
async def logs(log_data, session):
    await models.logs.create_logs(session=session, logs=log_data)


class TestCreateLogs:
    async def test_create_logs_succeeds(self, session, flow_run_id, log_data, logs, db):
        query = select(db.Log).order_by(db.Log.timestamp.asc())
        result = await session.execute(query)
        read_logs = result.scalars().unique().all()

        for i, log in enumerate(read_logs):
            assert (
                Log.model_validate(log, from_attributes=True).model_dump(
                    exclude={"created", "id", "updated"},
                )
                == log_data[i]
            )


class TestReadLogs:
    async def test_read_logs_timestamp_after_inclusive(self, session, logs, log_data):
        after = log_data[1].timestamp
        log_filter = LogFilter(timestamp={"after_": after})
        logs = await models.logs.read_logs(
            session=session, log_filter=log_filter, sort=LogSort.TIMESTAMP_ASC
        )
        assert len(logs) == 2

        # We get a log with the same timestamp as the given timestamp.
        assert logs[0].timestamp == after
        # We get a log with a timestamp after the given timestamp.
        assert logs[1].timestamp > after

    async def test_read_logs_timestamp_before_inclusive(self, session, logs, log_data):
        before = log_data[1].timestamp
        log_filter = LogFilter(timestamp={"before_": before})
        logs = await models.logs.read_logs(
            session=session, log_filter=log_filter, sort=LogSort.TIMESTAMP_ASC
        )
        assert len(logs) == 2

        # We get a log with a timestamp before the given timestamp.
        assert logs[0].timestamp < before
        # We get a log with the same timestamp as the given timestamp.
        assert logs[1].timestamp == before

    async def test_read_logs_flow_run_id(self, session, logs, flow_run_id):
        log_filter = LogFilter(flow_run_id={"any_": [flow_run_id]})
        logs = await models.logs.read_logs(session=session, log_filter=log_filter)

        assert len(logs) == 3
        assert all([log.flow_run_id == flow_run_id for log in logs])

    async def test_read_logs_task_run_id(self, session, logs, task_run_id):
        log_filter = LogFilter(task_run_id={"any_": [task_run_id]})
        logs = await models.logs.read_logs(session=session, log_filter=log_filter)

        assert len(logs) == 1
        assert all([log.task_run_id == task_run_id for log in logs])

    async def test_read_logs_task_run_id_is_null(self, session, logs, flow_run_id):
        log_filter = LogFilter(
            flow_run_id={"any_": [flow_run_id]},
            task_run_id=LogFilterTaskRunId(is_null_=True),
        )
        logs_filtered = await models.logs.read_logs(
            session=session, log_filter=log_filter
        )

        assert len(logs_filtered) == 2
        assert all([log.task_run_id is None for log in logs_filtered])

    async def test_read_logs_task_run_id_is_not_null(self, session, logs, flow_run_id):
        log_filter = LogFilter(
            flow_run_id={"any_": [flow_run_id]}, task_run_id={"is_null_": False}
        )
        logs = await models.logs.read_logs(session=session, log_filter=log_filter)

        assert len(logs) == 1
        assert all([log.task_run_id is not None for log in logs])


class TestCreateLogsWithNullBytes:
    async def test_create_logs_strips_null_bytes_from_message(
        self, session, flow_run_id, db
    ):
        """Null bytes in log messages cause PostgreSQL to reject the INSERT
        with CharacterNotInRepertoireError.  They should be stripped."""
        log_data = [
            LogCreate(
                name="prefect.flow_run",
                level=20,
                message="binary data: \x00\x00 end",
                timestamp=NOW,
                flow_run_id=flow_run_id,
            ),
        ]
        await models.logs.create_logs(session=session, logs=log_data)

        query = select(db.Log)
        result = await session.execute(query)
        saved = result.scalars().unique().all()
        assert len(saved) == 1
        assert saved[0].message == "binary data:  end"
        assert "\x00" not in saved[0].message

    async def test_create_logs_strips_null_bytes_from_name(
        self, session, flow_run_id, db
    ):
        log_data = [
            LogCreate(
                name="prefect\x00.flow_run",
                level=20,
                message="test",
                timestamp=NOW,
                flow_run_id=flow_run_id,
            ),
        ]
        await models.logs.create_logs(session=session, logs=log_data)

        query = select(db.Log)
        result = await session.execute(query)
        saved = result.scalars().unique().all()
        assert len(saved) == 1
        assert saved[0].name == "prefect.flow_run"

    async def test_create_logs_without_null_bytes_unchanged(
        self, session, flow_run_id, db
    ):
        """Normal logs without null bytes should pass through unmodified."""
        log_data = [
            LogCreate(
                name="prefect.flow_run",
                level=20,
                message="normal message",
                timestamp=NOW,
                flow_run_id=flow_run_id,
            ),
        ]
        await models.logs.create_logs(session=session, logs=log_data)

        query = select(db.Log)
        result = await session.execute(query)
        saved = result.scalars().unique().all()
        assert len(saved) == 1
        assert saved[0].message == "normal message"


class TestLogSchemaConversion:
    """Tests for LogCreate to Log schema conversion - the core issue that was fixed"""

    async def test_create_logs_with_logcreate_publishes_logs_with_ids(self, session):
        """Test calling create_logs with LogCreate objects results in Log objects with IDs being published"""
        log_create = LogCreate(
            name="test.logger",
            level=20,
            message="Test message",
            timestamp=NOW,
            flow_run_id=uuid4(),
        )

        # Mock publish_logs to capture what gets passed to messaging
        with patch("prefect.server.logs.messaging.publish_logs") as mock_publish:
            await models.logs.create_logs(session=session, logs=[log_create])

            # Verify publish_logs was called with Log objects that have IDs
            mock_publish.assert_called_once()
            published_logs = mock_publish.call_args[0][0]

            assert len(published_logs) == 1
            published_log = published_logs[0]

            # This was the key issue: published logs must have IDs (LogCreate doesn't have IDs)
            assert isinstance(published_log, Log)
            assert published_log.id is not None

            # Core fields should match the original LogCreate
            assert published_log.name == log_create.name
            assert published_log.level == log_create.level
            assert published_log.message == log_create.message
            assert published_log.flow_run_id == log_create.flow_run_id


class TestDeleteLogs:
    async def test_delete_logs_flow_run_id(self, session, logs, flow_run_id):
        log_filter = LogFilter(flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]))
        logs = await models.logs.read_logs(session=session, log_filter=log_filter)

        assert len(logs) == 3
        assert all([log.flow_run_id == flow_run_id for log in logs])

        num_deleted = await models.logs.delete_logs(
            session=session, log_filter=log_filter
        )
        assert num_deleted == len(logs)

        logs = await models.logs.read_logs(session=session, log_filter=log_filter)
        assert len(logs) == 0, logs

    async def test_delete_logs_task_run_id(self, session, logs, task_run_id):
        log_filter = LogFilter(task_run_id=LogFilterTaskRunId(any_=[task_run_id]))
        logs = await models.logs.read_logs(session=session, log_filter=log_filter)

        assert len(logs) == 1
        assert all([log.task_run_id == task_run_id for log in logs])

        num_deleted = await models.logs.delete_logs(
            session=session, log_filter=log_filter
        )
        assert num_deleted == len(logs)

        logs = await models.logs.read_logs(session=session, log_filter=log_filter)
        assert len(logs) == 0, logs


@pytest.fixture
def structured_log_data(flow_run_id, other_flow_run_id, task_run_id):
    yield [
        LogCreate(
            name="prefect.flow_run",
            level=20,
            message="structured log one",
            timestamp=NOW,
            flow_run_id=flow_run_id,
            structured_fields={
                "user_id": 42,
                "region": "us-east-1",
                "active": True,
                "score": 9.5,
                "empty": None,
                "nested": {"role": "admin", "flags": ["read", "write"]},
            },
        ),
        LogCreate(
            name="prefect.flow_run",
            level=40,
            message="structured log two",
            timestamp=(NOW + timedelta(minutes=30)),
            flow_run_id=flow_run_id,
            structured_fields={
                "user_id": 7,
                "region": "eu-west-1",
                "active": False,
                "score": 1.0,
                "nested": {"role": "viewer"},
            },
        ),
        LogCreate(
            name="prefect.flow_run",
            level=20,
            message="other run log",
            timestamp=(NOW + timedelta(hours=1)),
            flow_run_id=other_flow_run_id,
            structured_fields={"user_id": 42, "region": "us-east-1"},
        ),
        LogCreate(
            name="prefect.flow_run",
            level=20,
            message="legacy log without structured fields",
            timestamp=(NOW + timedelta(hours=2)),
            flow_run_id=flow_run_id,
        ),
    ]


class TestStructuredFieldsPersistence:
    async def test_structured_fields_round_trip_with_shape(
        self, session, flow_run_id, structured_log_data
    ):
        await models.logs.create_logs(session=session, logs=structured_log_data)

        read = await models.logs.read_logs(session=session, log_filter=None)
        by_message = {log.message: log for log in read}

        one = by_message["structured log one"]
        assert one.structured_fields == {
            "user_id": 42,
            "region": "us-east-1",
            "active": True,
            "score": 9.5,
            "empty": None,
            "nested": {"role": "admin", "flags": ["read", "write"]},
        }
        legacy = by_message["legacy log without structured fields"]
        assert legacy.structured_fields is None


class TestReadLogsStructuredFieldsFilter:
    @pytest.fixture
    async def created_structured_logs(self, session, structured_log_data):
        await models.logs.create_logs(session=session, logs=structured_log_data)

    async def test_filter_by_string_value(
        self, session, flow_run_id, created_structured_logs
    ):
        log_filter = LogFilter(
            structured_fields=LogFilterStructuredFields(
                fields={"region": "us-east-1"}
            )
        )
        result = await models.logs.read_logs(session=session, log_filter=log_filter)
        assert {log.message for log in result} == {
            "structured log one",
            "other run log",
        }

    async def test_filter_by_integer_value(
        self, session, flow_run_id, created_structured_logs
    ):
        log_filter = LogFilter(
            structured_fields=LogFilterStructuredFields(fields={"user_id": 42})
        )
        result = await models.logs.read_logs(session=session, log_filter=log_filter)
        assert {log.message for log in result} == {
            "structured log one",
            "other run log",
        }

    async def test_filter_by_boolean_value(self, session, created_structured_logs):
        log_filter = LogFilter(
            structured_fields=LogFilterStructuredFields(fields={"active": True})
        )
        result = await models.logs.read_logs(session=session, log_filter=log_filter)
        assert [log.message for log in result] == ["structured log one"]

    async def test_filter_by_float_value(self, session, created_structured_logs):
        log_filter = LogFilter(
            structured_fields=LogFilterStructuredFields(fields={"score": 9.5})
        )
        result = await models.logs.read_logs(session=session, log_filter=log_filter)
        assert [log.message for log in result] == ["structured log one"]

    async def test_filter_by_null_value_matches_missing_and_null(
        self, session, created_structured_logs
    ):
        log_filter = LogFilter(
            structured_fields=LogFilterStructuredFields(fields={"empty": None})
        )
        result = await models.logs.read_logs(session=session, log_filter=log_filter)
        # Explicit JSON null ("one") and a missing key ("two", "other run",
        # "legacy") all extract as NULL.
        assert len(result) == 4

    async def test_multiple_fields_are_and_ed(self, session, created_structured_logs):
        log_filter = LogFilter(
            structured_fields=LogFilterStructuredFields(
                fields={"user_id": 42, "region": "us-east-1"}
            )
        )
        result = await models.logs.read_logs(session=session, log_filter=log_filter)
        assert {log.message for log in result} == {
            "structured log one",
            "other run log",
        }

    async def test_structured_filter_stacks_with_run_level_and_time(
        self, session, flow_run_id, created_structured_logs
    ):
        log_filter = LogFilter(
            flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]),
            level=LogFilterLevel(ge_=20, le_=30),
            timestamp=LogFilterTimestamp(after_=NOW - timedelta(minutes=1)),
            structured_fields=LogFilterStructuredFields(fields={"user_id": 42}),
        )
        result = await models.logs.read_logs(
            session=session, log_filter=log_filter, sort=LogSort.TIMESTAMP_ASC
        )
        # The other-flow-run match and the level-40/missing-key rows are
        # excluded by the run/level filters even though their business key
        # value matches.
        assert [log.message for log in result] == ["structured log one"]

    async def test_non_matching_value_returns_nothing(
        self, session, created_structured_logs
    ):
        log_filter = LogFilter(
            structured_fields=LogFilterStructuredFields(fields={"user_id": 999})
        )
        result = await models.logs.read_logs(session=session, log_filter=log_filter)
        assert result == []

    async def test_non_scalar_filter_value_rejected(self):
        with pytest.raises(ValueError, match="scalar JSON value"):
            LogFilterStructuredFields(fields={"nested": {"role": "admin"}})
