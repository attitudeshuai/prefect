"""
Extraction and sanitization of caller-provided structured log fields.

When a caller uses the standard library convention to attach key/value pairs to
a log statement (``logger.info("...", extra={"user_id": 42})``), those pairs
become attributes on the `logging.LogRecord`. They are visible in local console
and JSON output, but were previously dropped from the payload shipped to the
Prefect API.

The helpers in this module collect the caller-provided attributes while
excluding standard library and Prefect-reserved attributes, and convert each
individual value into a JSON-native representation. Failures are isolated to
the offending value: an overlong value is truncated, a value that cannot be
serialized is replaced by placeholder text describing its original type, and
no single value can cause the whole log record to be dropped.

All state is carried on the `LogRecord` itself; nothing is stored in process
global or thread-local state, so log statements from concurrently executing
runs can never exchange structured fields.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any, Collection

#: Appended to string values that exceed the configured maximum length.
TRUNCATION_MARKER = "...[truncated]"

#: Attributes that the standard library places on every `LogRecord` for the
#: running Python version. Computing these from a synthetic record keeps the
#: set correct across Python versions (e.g. `taskName` was added in 3.12).
_STDLIB_LOG_RECORD_ATTRIBUTES: frozenset[str] = frozenset(
    logging.makeLogRecord({}).__dict__
)

#: Standard library attributes attached lazily, after record creation:
#: `Formatter.format()` sets `message`, and `formatTime()` sets `asctime`
#: when the format string uses it.
_STDLIB_LAZY_ATTRIBUTES: frozenset[str] = frozenset({"message", "asctime"})

#: Attributes injected by Prefect itself for transport or display. These are
#: never caller-provided business fields, even when a user explicitly lists a
#: matching name in the allowed keys.
_PREFECT_RESERVED_ATTRIBUTES: frozenset[str] = frozenset(
    {
        # Run identity, also carried in dedicated log columns
        "flow_run_id",
        "task_run_id",
        "worker_id",
        # Run logger metadata used by log format strings
        "flow_run_name",
        "flow_name",
        "task_run_name",
        "task_name",
        "deployment_name",
        # APILogHandler opt-out marker
        "send_to_api",
        # Internal server messaging/automation context
        "event_message",
        "automation",
        "action",
        "triggering_event",
        "triggering_labels",
    }
)

_RESERVED_ATTRIBUTES: frozenset[str] = (
    _STDLIB_LOG_RECORD_ATTRIBUTES
    | _STDLIB_LAZY_ATTRIBUTES
    | _PREFECT_RESERVED_ATTRIBUTES
)


def _placeholder(value: Any) -> str:
    """Placeholder text that records the original value's type."""
    return f"<unserializable: {type(value).__name__}>"


def _circular_placeholder(value: Any) -> str:
    return f"<circular reference: {type(value).__name__}>"


def _truncate_string(value: str, max_length: int) -> str:
    """Truncate a single string value, marking that truncation occurred."""
    if len(value) <= max_length:
        return value
    keep = max_length - len(TRUNCATION_MARKER)
    if keep > 0:
        return value[:keep] + TRUNCATION_MARKER
    return TRUNCATION_MARKER


def _sanitize(value: Any, max_length: int, seen: frozenset[int]) -> Any:
    """
    Convert one structured field value into a JSON-native representation.

    String leaves are truncated individually and containers keep their shape.
    This function never raises: any value that cannot be represented is
    replaced by placeholder text describing its original type.
    """
    try:
        # `bool` must be checked before `int` because bool is an int subclass.
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return _truncate_string(value, max_length)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            # NaN and +/-Infinity are not valid JSON; replacing them here keeps
            # the entire log payload parseable by strict JSON decoders.
            return value if math.isfinite(value) else _placeholder(value)
        if value is None:
            return None
        if isinstance(value, dict):
            if id(value) in seen:
                return _circular_placeholder(value)
            child_seen = seen | {id(value)}
            sanitized: dict[str, Any] = {}
            for key, item in value.items():
                # JSON object keys must be strings; coerce rather than drop.
                key_str = key if isinstance(key, str) else str(key)
                try:
                    sanitized[key_str] = _sanitize(item, max_length, child_seen)
                except Exception:
                    sanitized[key_str] = _placeholder(item)
            return sanitized
        if isinstance(value, list):
            if id(value) in seen:
                return _circular_placeholder(value)
            child_seen = seen | {id(value)}
            sanitized_list: list[Any] = []
            for item in value:
                try:
                    sanitized_list.append(_sanitize(item, max_length, child_seen))
                except Exception:
                    sanitized_list.append(_placeholder(item))
            return sanitized_list
        if isinstance(value, tuple):
            if id(value) in seen:
                return _circular_placeholder(value)
            child_seen = seen | {id(value)}
            sanitized_tuple: list[Any] = []
            for item in value:
                try:
                    sanitized_tuple.append(_sanitize(item, max_length, child_seen))
                except Exception:
                    sanitized_tuple.append(_placeholder(item))
            # JSON has no tuple type; represent as an array, preserving order.
            return sanitized_tuple
        # Sets, bytes, UUIDs, datetimes, and other arbitrary objects are not
        # JSON-native and are replaced individually.
        return _placeholder(value)
    except Exception:
        return _placeholder(value)


def extract_structured_fields(
    record: logging.LogRecord,
    *,
    allowed_keys: Collection[str] | None = None,
    max_value_length: int = 10_000,
) -> dict[str, Any]:
    """
    Collect caller-provided structured fields from a `LogRecord`.

    Args:
        record: The log record to extract fields from.
        allowed_keys: When non-empty, only these attribute names are collected.
            An empty or `None` value allows every caller-provided key except
            reserved standard library and Prefect attributes.
        max_value_length: Maximum length of an individual string value; longer
            strings are truncated per item.

    Returns:
        A JSON-native mapping of structured field names to values. The mapping
        is always safe to JSON-serialize and never shares mutable state with
        the record or with other records.
    """
    allowlist = frozenset(allowed_keys or ())
    fields: dict[str, Any] = {}

    for key, value in record.__dict__.items():
        if key in _RESERVED_ATTRIBUTES:
            continue
        if allowlist and key not in allowlist:
            continue
        try:
            fields[key] = _sanitize(value, max_value_length, frozenset())
        except Exception:
            fields[key] = _placeholder(value)

    # Defense in depth: guarantee the result is JSON-native so a structured
    # field can never make the whole log payload fail to serialize.
    try:
        json.dumps(fields, allow_nan=False)
    except (TypeError, ValueError):
        return {}
    return fields
