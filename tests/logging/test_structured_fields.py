"""
Tests for caller-provided structured log fields: extraction from
`LogRecord` attributes, allowlisting, per-item truncation and serialization
fallbacks, payload identity when the feature is disabled, and isolation
between concurrently produced records.
"""

import json
import logging
import math
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from prefect.client.schemas.actions import LogCreate
from prefect.logging.handlers import APILogHandler, WorkerAPILogHandler
from prefect.logging.structured import (
    TRUNCATION_MARKER,
    extract_structured_fields,
)
from prefect.settings import temporary_settings
from prefect.types._datetime import now

FLOW_RUN_ID = str(uuid4())


class _Unserializable:
    pass


def _make_record(**extra: object) -> logging.LogRecord:
    return logging.getLogger("tests.structured_fields").makeRecord(
        "prefect.flow_runs",
        logging.INFO,
        __file__,
        1,
        "hello",
        (),
        None,
        extra={"flow_run_id": FLOW_RUN_ID, **extra},
    )


class TestExtractStructuredFields:
    def test_collects_caller_fields_and_preserves_nested_shape(self):
        nested = {"user": {"id": 7, "roles": ["admin", {"scope": "read"}]}, "ok": True}
        record = _make_record(user_id=42, region="us-east-1", nested=nested)

        fields = extract_structured_fields(record)

        assert fields == {
            "user_id": 42,
            "region": "us-east-1",
            "nested": nested,
        }
        # The returned mapping does not share container identity with the record
        assert fields["nested"] is not record.nested
        # Output is always JSON-native
        json.dumps(fields, allow_nan=False)

    def test_excludes_stdlib_and_prefect_reserved_attributes(self):
        record = _make_record(
            business_key="value",
            flow_run_name="my-flow",
            flow_name="f",
            task_run_id=str(uuid4()),
            task_run_name="t",
            task_name="tn",
            deployment_name="d",
            worker_id=str(uuid4()),
            send_to_api=False,
            event_message=object(),
            automation="a",
            action={},
            triggering_event=None,
            triggering_labels=[],
        )
        # `message` is attached lazily by Formatter.format()
        record.message = "hello"
        record.asctime = "2026-10-04 00:00:00"

        fields = extract_structured_fields(record)

        assert fields == {"business_key": "value"}

    def test_allowlist_restricts_collected_keys(self):
        record = _make_record(user_id=1, region="eu", other="nope")

        fields = extract_structured_fields(record, allowed_keys=["user_id", "region"])
        assert set(fields) == {"user_id", "region"}

        assert extract_structured_fields(record, allowed_keys=["missing"]) == {}

    def test_empty_allowlist_allows_every_caller_key(self):
        record = _make_record(user_id=1, region="eu")
        assert set(extract_structured_fields(record, allowed_keys=[])) == {
            "user_id",
            "region",
        }

    def test_truncates_overlong_string_values_per_item(self):
        record = _make_record(big="x" * 100, small="y")

        fields = extract_structured_fields(record, max_value_length=20)

        assert len(fields["big"]) == 20
        assert fields["big"].endswith(TRUNCATION_MARKER)
        assert fields["small"] == "y"

    def test_truncates_nested_string_leaves_and_keeps_shape(self):
        record = _make_record(payload={"a": "x" * 100, "b": ["y" * 100, "z"]})

        fields = extract_structured_fields(record, max_value_length=20)

        assert set(fields["payload"].keys()) == {"a", "b"}
        assert fields["payload"]["a"].endswith(TRUNCATION_MARKER)
        assert fields["payload"]["b"][0].endswith(TRUNCATION_MARKER)
        assert fields["payload"]["b"][1] == "z"

    def test_unserializable_value_becomes_typed_placeholder(self):
        record = _make_record(bad=_Unserializable(), good=1)

        fields = extract_structured_fields(record)

        assert fields["bad"] == "<unserializable: _Unserializable>"
        assert fields["good"] == 1

    def test_one_bad_value_does_not_lose_siblings(self):
        record = _make_record(
            container={"good": "yes", "bad": _Unserializable()},
            bad=_Unserializable(),
            good="kept",
        )

        fields = extract_structured_fields(record)

        assert fields["good"] == "kept"
        assert fields["container"]["good"] == "yes"
        assert fields["container"]["bad"] == "<unserializable: _Unserializable>"
        assert fields["bad"] == "<unserializable: _Unserializable>"

    def test_circular_references_become_placeholders(self):
        cycle: dict = {}
        cycle["self"] = cycle
        record = _make_record(cycle=cycle, ok=1)

        fields = extract_structured_fields(record)

        assert fields["cycle"] == {"self": "<circular reference: dict>"}
        assert fields["ok"] == 1
        json.dumps(fields, allow_nan=False)

    @pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
    def test_non_finite_floats_become_placeholders(self, value: float):
        record = _make_record(number=value, ok=1.5)

        fields = extract_structured_fields(record)

        assert fields["number"] == "<unserializable: float>"
        assert fields["ok"] == 1.5

    def test_non_string_mapping_keys_are_coerced(self):
        record = _make_record(mapping={1: "a"})
        fields = extract_structured_fields(record)
        assert fields["mapping"] == {"1": "a"}

    def test_output_is_always_json_serializable(self):
        record = _make_record(
            a=_Unserializable(),
            b=[{"c": _Unserializable()}],
            c=math.nan,
        )
        json.dumps(extract_structured_fields(record), allow_nan=False)


class TestAPILogHandlerStructuredFields:
    def test_disabled_payload_omits_structured_fields(self):
        payload = APILogHandler().prepare(_make_record(user_id=1))
        assert "structured_fields" not in payload
        # The payload remains byte-identical to the historical LogCreate shape
        assert set(payload) == {
            "name",
            "level",
            "message",
            "timestamp",
            "flow_run_id",
            "task_run_id",
            "__payload_size__",
        }

    def test_enabled_payload_carries_structured_fields(self):
        with temporary_settings(
            {"logging.to_api.structured_fields_enabled": True}
        ):
            payload = APILogHandler().prepare(
                _make_record(
                    user_id=7,
                    nested={"region": "us-east-1", "flags": [True, False]},
                )
            )

        assert payload["structured_fields"] == {
            "user_id": 7,
            "nested": {"region": "us-east-1", "flags": [True, False]},
        }
        json.dumps(payload)

    def test_unserializable_value_never_loses_the_log(self):
        with temporary_settings(
            {"logging.to_api.structured_fields_enabled": True}
        ):
            payload = APILogHandler().prepare(_make_record(bad=_Unserializable()))

        assert payload["message"] == "hello"
        assert payload["structured_fields"] == {
            "bad": "<unserializable: _Unserializable>"
        }

    def test_structured_fields_survive_message_truncation(self):
        record = logging.getLogger("tests.structured_fields.trunc").makeRecord(
            "prefect.flow_runs",
            logging.INFO,
            __file__,
            1,
            "z" * 5_000,
            (),
            None,
            extra={"flow_run_id": FLOW_RUN_ID, "user_id": 9},
        )
        with temporary_settings(
            {
                "logging.to_api.structured_fields_enabled": True,
                "logging.to_api.max_log_size": 200,
            }
        ):
            payload = APILogHandler().prepare(record)

        assert payload["message"].endswith("... [truncated]")
        assert payload["structured_fields"] == {"user_id": 9}

    def test_worker_handler_carries_structured_fields_when_enabled(self):
        record = _make_record(user_id=3, worker_id=str(uuid4()))
        with temporary_settings(
            {"logging.to_api.structured_fields_enabled": True}
        ):
            payload = WorkerAPILogHandler().prepare(record)
        assert payload["structured_fields"] == {"user_id": 3}
        assert "worker_id" in payload

    def test_worker_handler_payload_unchanged_when_disabled(self):
        record = _make_record(user_id=3, worker_id=str(uuid4()))
        payload = WorkerAPILogHandler().prepare(record)
        assert "structured_fields" not in payload


class TestLogCreatePayloadIdentity:
    def test_structured_fields_omitted_when_unset(self):
        log = LogCreate(name="n", level=20, message="m", timestamp=now("UTC"))
        assert "structured_fields" not in log.model_dump(mode="json")

    def test_structured_fields_included_when_set(self):
        log = LogCreate(
            name="n",
            level=20,
            message="m",
            timestamp=now("UTC"),
            structured_fields={"user_id": 1},
        )
        assert log.model_dump(mode="json")["structured_fields"] == {"user_id": 1}


class TestConcurrentIsolation:
    def test_concurrent_records_never_share_fields(self):
        run_count = 16

        def prepare_for_run(index: int) -> tuple[str, dict]:
            flow_run_id = str(uuid4())
            record = logging.getLogger(f"tests.concurrent.{index}").makeRecord(
                "prefect.flow_runs",
                logging.INFO,
                __file__,
                1,
                f"run-{index}",
                (),
                None,
                extra={
                    "flow_run_id": flow_run_id,
                    "business": {"run_index": index, "tags": [f"r{index}"]},
                },
            )
            with temporary_settings(
                {"logging.to_api.structured_fields_enabled": True}
            ):
                payload = APILogHandler().prepare(record)
            return str(payload["flow_run_id"]), payload["structured_fields"]

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(prepare_for_run, range(run_count)))

        for index, (flow_run_id, structured) in enumerate(results):
            assert structured == {
                "business": {"run_index": index, "tags": [f"r{index}"]}
            }
            assert flow_run_id != FLOW_RUN_ID
