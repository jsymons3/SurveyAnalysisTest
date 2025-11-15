"""Analytics routines leveraging Spark and Snowflake integrations."""
from __future__ import annotations

from typing import Iterable, Mapping


def aggregate_by_channel(responses: Iterable[Mapping[str, object]]) -> dict:
    """Aggregate survey metrics by sales channel using Spark.

    The caller is responsible for ensuring PySpark (and a compatible Java
    runtime) is available. Any failure to import or initialise Spark is surfaced
    as a :class:`RuntimeError` so the pipeline halts instead of silently
    continuing with degraded analytics.
    """

    materialised = list(responses)

    try:  # pragma: no cover - optional dependency import
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:  # pragma: no cover - import guard
        raise RuntimeError("PySpark must be installed to run analytics") from exc

    try:
        spark = SparkSession.builder.appName("SurveyAnalysis").getOrCreate()
    except Exception as exc:  # pragma: no cover - startup guard
        raise RuntimeError("Unable to start a Spark session") from exc

    try:
        frame = spark.createDataFrame(materialised)
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

        result = {}
        for row in aggregated.collect():
            problem_row = problems.where(problems.channel == row["channel"]).first()
            result[row["channel"]] = {
                "avg_csats": row["avg_csats"],
                "avg_ces": row["avg_ces"],
                "avg_nps": row["avg_nps"],
                "problem_count": problem_row["problem_count"] if problem_row else 0,
            }
        return result
    finally:  # pragma: no cover - ensure Spark shuts down
        try:
            spark.stop()
        except Exception:  # pragma: no cover - best effort cleanup
            pass
