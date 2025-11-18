import pytest

analytics = pytest.importorskip(
    "survey_analysis.analytics",
    reason="PySpark is required to run analytics tests",
)


def test_aggregate_by_channel_with_spark():
    responses = [
        {
            "channel": "Store",
            "csat_score": 4,
            "ces_score": 5,
            "nps_score": 7,
            "had_problem": False,
            "processing": ["ingest", "spark"],
        },
        {
            "channel": "Store",
            "csat_score": 2,
            "ces_score": 3,
            "nps_score": 5,
            "had_problem": True,
            "processing": {"stage": "load"},
        },
        {
            "channel": "Online",
            "csat_score": 5,
            "ces_score": 6,
            "nps_score": 9,
            "had_problem": True,
            "processing": None,
        },
    ]

    result = analytics.aggregate_by_channel(responses)

    assert pytest.approx(result["Store"]["avg_csats"], rel=1e-3) == 3
    assert pytest.approx(result["Store"]["avg_ces"], rel=1e-3) == 4
    assert pytest.approx(result["Store"]["avg_nps"], rel=1e-3) == 6
    assert result["Store"]["problem_count"] == 1

    assert pytest.approx(result["Online"]["avg_csats"], rel=1e-3) == 5
    assert pytest.approx(result["Online"]["avg_ces"], rel=1e-3) == 6
    assert pytest.approx(result["Online"]["avg_nps"], rel=1e-3) == 9
    assert result["Online"]["problem_count"] == 1


def test_python_fallback_matches_expectations():
    responses = [
        {
            "channel": "store",
            "csat_score": 5,
            "ces_score": 6,
            "nps_score": 8,
            "had_problem": False,
        },
        {
            "channel": "store",
            "csat_score": 4,
            "ces_score": 6,
            "nps_score": 9,
            "had_problem": True,
        },
    ]

    result = analytics._aggregate_by_channel_python(responses)

    assert result["store"]["problem_count"] == 1
    assert pytest.approx(result["store"]["avg_csats"], rel=1e-3) == 4.5
    assert pytest.approx(result["store"]["avg_ces"], rel=1e-3) == 6
    assert pytest.approx(result["store"]["avg_nps"], rel=1e-3) == 8.5
