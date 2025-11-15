"""High-level survey pipeline orchestration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from .analytics import aggregate_by_channel
from .ingestion import parse_response
from .storage import StorageBackend, bulk_store


@dataclass
class SurveyPipeline:
    """Coordinates ingestion, storage, analytics, and downstream publishing."""

    storage_backend: StorageBackend

    def ingest_and_store(self, raw_payloads: Iterable[Mapping[str, object]]) -> list[Mapping[str, object]]:
        """Parse raw payloads and store them via the configured backend."""

        parsed_records = []
        for raw in raw_payloads:
            response = parse_response(dict(raw))
            record = response.to_record()
            parsed_records.append(record)

        bulk_store(parsed_records, self.storage_backend)
        return parsed_records

    def run_analytics(self, records: Iterable[Mapping[str, object]]) -> dict:
        """Execute analytics routines on the provided records."""

        return aggregate_by_channel(records)
