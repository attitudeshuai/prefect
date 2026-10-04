import datetime
import uuid
from types import SimpleNamespace

import pytest

from prefect._internal import retry_budget
from prefect.states import (
    AwaitingRetry,
    Cancelled,
    Completed,
    Failed,
    Retrying,
    Running,
)

SECOND = datetime.timedelta(seconds=1)


# These are pure unit tests with no client/API surface; override the autouse
# fixtures from prefect.testing so a hosted API server subprocess is not started.
@pytest.fixture
def hosted_api_server() -> None:
    return None


@pytest.fixture
def use_hosted_api_server(hosted_api_server: None):
    yield None


def policy(
    seconds=None,
    include_queue_time=None,
    enforcement=None,
):
    return SimpleNamespace(
        retry_budget_seconds=seconds,
        retry_budget_include_queue_time=include_queue_time,
        retry_budget_enforcement=enforcement,
    )


def run(elapsed=datetime.timedelta(0), pol=None):
    return SimpleNamespace(
        retry_budget_elapsed=elapsed,
        empirical_policy=pol,
        retry_budget_wait_state_id=None,
        retry_budget_count_wait=None,
        retry_budget_exceeded=False,
    )


def state_at(state, timestamp, state_id=None):
    state.timestamp = timestamp
    state.id = state_id or uuid.uuid4()
    return state


class TestPolicyAccess:
    def test_is_budget_configured(self):
        assert retry_budget.is_budget_configured(policy()) is False
        assert retry_budget.is_budget_configured(policy(seconds=10)) is True
        assert retry_budget.is_budget_configured(None) is False

    def test_resolve_count_wait_defaults_false(self):
        assert retry_budget.resolve_count_wait(policy()) is False
        assert retry_budget.resolve_count_wait(policy(include_queue_time=True)) is True
        assert retry_budget.resolve_count_wait(None) is False

    def test_resolve_enforcement_defaults_and_validation(self):
        assert retry_budget.resolve_enforcement(policy()) == retry_budget.FAIL
        assert (
            retry_budget.resolve_enforcement(policy(enforcement="cancel"))
            == retry_budget.CANCEL
        )
        assert (
            retry_budget.resolve_enforcement(policy(enforcement="bogus"))
            == retry_budget.FAIL
        )

    def test_exceeds_budget_is_strict(self):
        assert (
            retry_budget.exceeds_budget(SECOND, policy(seconds=1.0)) is False
        )
        assert (
            retry_budget.exceeds_budget(
                datetime.timedelta(seconds=1.001), policy(seconds=1.0)
            )
            is True
        )
        assert retry_budget.exceeds_budget(SECOND, policy()) is False


class TestFoldRunningSegment:
    def test_fold_adds_segment(self):
        r = run(pol=policy(seconds=100))
        initial = state_at(Running(), datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc))
        proposed = state_at(Failed(), initial.timestamp + 5 * SECOND)
        retry_budget.fold_running_segment(r, initial, proposed)
        assert r.retry_budget_elapsed == 5 * SECOND

    def test_fold_requires_running_segment(self):
        r = run(pol=policy(seconds=100))
        initial = state_at(
            AwaitingRetry(), datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        )
        proposed = state_at(Running(), initial.timestamp + SECOND)
        retry_budget.fold_running_segment(r, initial, proposed)
        assert r.retry_budget_elapsed == datetime.timedelta(0)

    def test_fold_requires_configured_budget(self):
        r = run(pol=policy())
        initial = state_at(Running(), datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc))
        proposed = state_at(Failed(), initial.timestamp + SECOND)
        retry_budget.fold_running_segment(r, initial, proposed)
        assert r.retry_budget_elapsed == datetime.timedelta(0)

    def test_projected_elapsed_does_not_mutate(self):
        r = run(elapsed=3 * SECOND, pol=policy(seconds=100))
        initial = state_at(Running(), datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc))
        proposed = state_at(Failed(), initial.timestamp + 2 * SECOND)
        projected = retry_budget.projected_elapsed(r, initial, proposed)
        assert projected == 5 * SECOND
        assert r.retry_budget_elapsed == 3 * SECOND

        # non-running transition projects only the persisted value
        assert (
            retry_budget.projected_elapsed(r, None, proposed) == 3 * SECOND
        )


class TestWaitLifecycle:
    def test_freeze_records_basis_and_marker(self):
        r = run(pol=policy(seconds=100, include_queue_time=True))
        waiting = state_at(
            AwaitingRetry(), datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        )
        retry_budget.freeze_wait_policy(r, waiting)
        assert r.retry_budget_count_wait is True
        assert r.retry_budget_wait_state_id == waiting.id

    @pytest.mark.parametrize("count_wait", [True, False])
    def test_fold_wait_segment(self, count_wait):
        r = run(pol=policy(seconds=100, include_queue_time=count_wait))
        waiting = state_at(
            AwaitingRetry(), datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        )
        retry_budget.freeze_wait_policy(r, waiting)
        next_attempt = state_at(Running(), waiting.timestamp + 4 * SECOND)
        retry_budget.fold_wait_segment(r, waiting, next_attempt)
        if count_wait:
            assert r.retry_budget_elapsed == 4 * SECOND
        else:
            assert r.retry_budget_elapsed == datetime.timedelta(0)
        assert r.retry_budget_wait_state_id == next_attempt.id
        assert r.retry_budget_count_wait is False

    def test_fold_wait_is_idempotent(self):
        r = run(pol=policy(seconds=100, include_queue_time=True))
        waiting = state_at(
            AwaitingRetry(), datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        )
        retry_budget.freeze_wait_policy(r, waiting)
        next_attempt = state_at(Running(), waiting.timestamp + 4 * SECOND)
        retry_budget.fold_wait_segment(r, waiting, next_attempt)
        # repeat the same transition; guard marker no longer matches
        retry_budget.fold_wait_segment(r, waiting, next_attempt)
        assert r.retry_budget_elapsed == 4 * SECOND

    def test_wait_to_terminal_fold(self):
        r = run(pol=policy(seconds=100, include_queue_time=True))
        waiting = state_at(
            AwaitingRetry(), datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        )
        retry_budget.freeze_wait_policy(r, waiting)
        terminal = state_at(Cancelled(), waiting.timestamp + 2 * SECOND)
        retry_budget.fold_wait_segment(r, waiting, terminal)
        assert r.retry_budget_elapsed == 2 * SECOND
        assert r.retry_budget_wait_state_id == terminal.id


class TestMessages:
    def test_budget_exceeded_message(self):
        message = retry_budget.budget_exceeded_message(
            datetime.timedelta(seconds=12.5), policy(seconds=10)
        )
        assert "Retry budget exceeded" in message
        assert "12.500" in message
        assert "10.000" in message

    def test_original_message_preserved(self):
        message = retry_budget.budget_exceeded_message(
            SECOND, policy(seconds=0.5), original_message="it broke"
        )
        assert message.startswith("it broke")

    def test_annotate_message(self):
        assert retry_budget.annotate_message("a", "b") == "a\nb"
        assert retry_budget.annotate_message(None, "b") == "b"


class TestAttemptsFromStates:
    def test_sequence_with_local_retry(self):
        t0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        first = state_at(Running(), t0)
        waiting = state_at(
            AwaitingRetry(scheduled_time=t0 + 30 * SECOND), t0 + 5 * SECOND
        )
        second = state_at(Retrying(), t0 + 30 * SECOND)
        finished = state_at(Completed(), t0 + 40 * SECOND)

        attempts = retry_budget.attempts_from_states(
            [first, waiting, second, finished]
        )
        assert len(attempts) == 2

        one, two = attempts
        assert one.attempt_number == 1
        assert one.end_time == waiting.timestamp
        assert one.run_time_seconds == pytest.approx(5)
        assert one.next_scheduled_start_time == t0 + 30 * SECOND
        assert one.failure_message is None

        assert two.attempt_number == 2
        assert two.wait_time_seconds == pytest.approx(25)
        assert two.end_time == finished.timestamp
        assert two.run_time_seconds == pytest.approx(10)
        assert two.origin == retry_budget.LOCAL

    def test_failed_attempt_records_failure_message(self):
        t0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        first = state_at(Running(), t0)
        failed = state_at(Failed(message="boom"), t0 + SECOND)
        attempts = retry_budget.attempts_from_states([first, failed])
        assert len(attempts) == 1
        assert attempts[0].failure_message == "boom"

    def test_remote_origin_preserved(self):
        t0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        first = state_at(Running(), t0)
        second = state_at(Running(), t0 + SECOND)
        # StateDetails gains the real field in Task 3; use a structural stub here
        second.state_details = SimpleNamespace(attempt_origin=retry_budget.REMOTE)
        attempts = retry_budget.attempts_from_states([first, second])
        assert attempts[1].origin == retry_budget.REMOTE
