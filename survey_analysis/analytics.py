"""Analytics routines leveraging Spark and Snowflake integrations."""
from __future__ import annotations
from pyspark.sql import functions as F
from typing import Iterable, Mapping
import json


def aggregate_by_channel(responses: Iterable[Mapping[str, object]]) -> dict:
    """Aggregate survey metrics by sales channel."""

    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        return _aggregate_by_channel_python(responses)

    # Materialize responses so we can clean them up
    responses_list = list(responses)

    # We only need these fields for the aggregation
    FIELDS = ("channel", "csat_score", "ces_score", "nps_score", "had_problem", "processing")

    normalized = []
    for r in responses_list:
        # Start from a plain dict
        if isinstance(r, Mapping):
            rec = dict(r)
        else:
            rec = dict(getattr(r, "__dict__", {}))

        out = {}

        # channel: always string-ish, default to "unknown"
        channel = rec.get("channel", "unknown")
        out["channel"] = str(channel) if channel is not None else "unknown"

        # scores & had_problem: let Spark cast later; just pass the raw values through
        out["csat_score"] = rec.get("csat_score")
        out["ces_score"] = rec.get("ces_score")
        out["nps_score"] = rec.get("nps_score")
        out["had_problem"] = rec.get("had_problem")

        # processing: normalize to string/None so Spark sees a consistent type
        val = rec.get("processing", None)
        if val is None:
            out["processing"] = None
        elif isinstance(val, (list, tuple)):
            out["processing"] = ", ".join(map(str, val))
        elif isinstance(val, dict):
            out["processing"] = json.dumps(val, sort_keys=True)
        else:
            out["processing"] = str(val)

        normalized.append(out)

    spark = SparkSession.builder.appName("SurveyAnalysis").getOrCreate()
    try:
        # Only our cleaned, primitive fields go into Spark
        frame = spark.createDataFrame(normalized)

        # Cast numeric / boolean columns
        frame = frame.withColumn("csat_score", frame.csat_score.cast("double"))
        frame = frame.withColumn("ces_score", frame.ces_score.cast("double"))
        frame = frame.withColumn("nps_score", frame.nps_score.cast("double"))
        frame = frame.withColumn("had_problem", frame.had_problem.cast("boolean"))

        # Averages per channel
        aggregated = (
            frame.groupBy("channel")
            .agg(
                F.avg("csat_score").alias("avg_csats"),
                F.avg("ces_score").alias("avg_ces"),
                F.avg("nps_score").alias("avg_nps"),
            )
        )

        # Count "had_problem" = True per channel (cast to int for sum)
        problems = (
            frame.groupBy("channel")
            .agg(
                F.sum(F.col("had_problem").cast("int")).alias("problem_count")
            )
        )

        # Join the two aggregations so we only collect once
        joined = (
            aggregated.join(problems, on="channel", how="left")
        )

        result = {
            row["channel"]: {
                "avg_csats": row["avg_csats"],
                "avg_ces": row["avg_ces"],
                "avg_nps": row["avg_nps"],
                "problem_count": row["problem_count"],
            }
            for row in joined.collect()
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
