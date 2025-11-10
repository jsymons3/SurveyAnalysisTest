"""Analytics routines leveraging Spark and Snowflake integrations."""
from __future__ import annotations

from typing import Iterable, Mapping


def aggregate_by_channel(responses: Iterable[Mapping[str, object]]) -> dict:
    """Aggregate survey metrics by sales channel."""

    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        return _aggregate_by_channel_python(responses)

    spark = SparkSession.builder.appName("SurveyAnalysis").getOrCreate()
    try:
        frame = spark.createDataFrame(list(responses))
        frame = frame.withColumn("csat_score", frame.csat_score.cast("double"))
        frame = frame.withColumn("ces_score", frame.ces_score.cast("double"))
        frame = frame.withColumn("nps_score", frame.nps_score.cast("double"))
        frame = frame.withColumn("had_problem", frame.had_problem.cast("boolean"))

        aggregated = (
            frame.groupBy("channel")
            .agg(
                {"csat_score": "avg", "ces_score": "avg", "nps_score": "avg"}
            )
            .withColumnRenamed("avg(csat_score)", "avg_csats")
            .withColumnRenamed("avg(ces_score)", "avg_ces")
            .withColumnRenamed("avg(nps_score)", "avg_nps")
        )

        problems = frame.groupBy("channel").sum("had_problem").withColumnRenamed(
            "sum(had_problem)", "problem_count"
        )

        result = {
            row["channel"]: {
                "avg_csats": row["avg_csats"],
                "avg_ces": row["avg_ces"],
                "avg_nps": row["avg_nps"],
                "problem_count": problems.where(problems.channel == row["channel"]).first()[
                    "problem_count"
                ],
            }
            for row in aggregated.collect()
        }
        return result
    finally:  # pragma: no cover - spark session cleanup
        spark.stop()


def _aggregate_by_channel_python(
    responses: Iterable[Mapping[str, object]]
) -> dict:  # pragma: no cover - fallback
    import math
    from collections import defaultdict

    aggregates = defaultdict(lambda: {"csat": [], "ces": [], "nps": [], "problem_count": 0})
    for response in responses:
        channel = response.get("channel", "unknown")
        bucket = aggregates[channel]
        bucket["problem_count"] += 1 if response.get("had_problem") else 0
        for key, label in (
            ("csat_score", "csat"),
            ("ces_score", "ces"),
            ("nps_score", "nps"),
        ):
            value = response.get(key)
            if isinstance(value, (int, float)) and not math.isnan(value):
                bucket[label].append(float(value))

    result = {}
    for channel, bucket in aggregates.items():
        result[channel] = {
            "avg_csats": sum(bucket["csat"]) / len(bucket["csat"]) if bucket["csat"] else None,
            "avg_ces": sum(bucket["ces"]) / len(bucket["ces"]) if bucket["ces"] else None,
            "avg_nps": sum(bucket["nps"]) / len(bucket["nps"]) if bucket["nps"] else None,
            "problem_count": bucket["problem_count"],
        }
    return result
