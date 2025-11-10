"""AWS storage helpers for survey data."""
from __future__ import annotations

import json
from typing import Iterable, Mapping, Protocol


class StorageBackend(Protocol):
    """Protocol for storing survey data records."""

    def store(self, payload: Mapping[str, object]) -> None:
        ...


class S3JSONStorage:
    """Persist survey payloads as JSON objects in Amazon S3."""

    def __init__(self, bucket: str, prefix: str = "survey_responses/"):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"

    def store(self, payload: Mapping[str, object]) -> None:
        import boto3  # type: ignore

        client = boto3.client("s3")
        key = self.prefix + _build_object_key(payload)
        client.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(payload).encode("utf-8"))


class LocalJSONStorage:
    """Local fallback storage useful for testing."""

    def __init__(self, directory: str):
        from pathlib import Path

        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def store(self, payload: Mapping[str, object]) -> None:
        from datetime import datetime
        from pathlib import Path

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
        path = self.directory / f"response_{timestamp}.json"
        path.write_text(json.dumps(payload, indent=2))


def bulk_store(responses: Iterable[Mapping[str, object]], backend: StorageBackend) -> None:
    """Store a sequence of survey responses using the provided backend."""

    for response in responses:
        backend.store(response)


def _build_object_key(payload: Mapping[str, object]) -> str:
    from hashlib import sha1

    identifier = payload.get("location_or_order") or payload.get("meta_response_id") or "unknown"
    digest = sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    return f"{identifier}_{digest}.json"
