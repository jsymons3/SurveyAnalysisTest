"""Survey Analysis System package."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .models import SurveyResponse
from .ingestion import parse_response, response_to_record

if TYPE_CHECKING:  # pragma: no cover - import-time hinting only
    from .pipelines import SurveyPipeline as _SurveyPipeline

__all__ = [
    "SurveyResponse",
    "parse_response",
    "response_to_record",
    "SurveyPipeline",
]


def __getattr__(name: str):  # pragma: no cover - trivial
    if name == "SurveyPipeline":
        from .pipelines import SurveyPipeline

        return SurveyPipeline
    raise AttributeError(name)
