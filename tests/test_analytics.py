import sys
import types

import pytest


def test_aggregate_by_channel_requires_spark(monkeypatch):
    from survey_analysis import analytics

    responses = [
        {"channel": "store", "csat_score": 4, "ces_score": 5, "nps_score": 7, "had_problem": False},
        {"channel": "online", "csat_score": 5, "ces_score": 6, "nps_score": 8, "had_problem": True},
    ]

    fake_pyspark = types.ModuleType("pyspark")
    fake_sql = types.ModuleType("pyspark.sql")

    class _Builder:
        def appName(self, _):
            return self

        def getOrCreate(self):  # pragma: no cover - exercised via failure path
            raise RuntimeError("Spark not available")

    class _SparkSession:
        builder = _Builder()

    fake_sql.SparkSession = _SparkSession

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)

    with pytest.raises(RuntimeError):
        analytics.aggregate_by_channel(responses)
