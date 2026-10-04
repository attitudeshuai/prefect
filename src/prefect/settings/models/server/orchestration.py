from typing import ClassVar

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from prefect.settings.base import PrefectBaseSettings, build_settings_config


class ServerOrchestrationSettings(PrefectBaseSettings):
    """Settings controlling server-side state transition orchestration."""

    model_config: ClassVar[SettingsConfigDict] = build_settings_config(
        ("server", "orchestration")
    )

    preview_observations_enabled: bool = Field(
        default=True,
        description=(
            "Whether the read-only state transition precheck endpoint records"
            " observations (counts and reasons of rejected, aborted, waiting, and"
            " rewritten transitions). When disabled, no observations are recorded."
        ),
    )

    preview_observations_sample_rate: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description=(
            "The fraction of non-accepted or rewritten state transition prechecks"
            " that are recorded in the observation surface, between 0 and 1. A value"
            " of 1 records every precheck, 0 records none."
        ),
    )

    preview_observations_max_events: int = Field(
        default=200,
        ge=1,
        description=(
            "The maximum number of recent state transition precheck events retained"
            " in the in-process observation surface. Older events are evicted first."
        ),
    )
