import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from survey_analysis.ingestion import parse_response


SAMPLE = {
    "channel": "Both (researched online, purchased in store)",
    "visit_date": "2024-06-01",
    "location_or_order": "Louisville, KY (Shelbyville Rd)",
    "categories": ["Computers / Tablets", "Cables / Accessories"],
    "product_found": "Yes, exactly",
    "obstacles": [],
    "ease_of_finding": 4,
    "product_information_clarity": 5,
    "interacted_with_associate": True,
    "knowledge_score": 5,
    "pressure_free_score": 4,
    "wait_time_score": 4,
    "price_alignment_score": 4,
    "offers_used": ["Sale price", "My Best Buy benefits"],
    "value_satisfaction_score": 4,
    "checkout_context": "in_store",
    "checkout_ease_score": 5,
    "payment_options_score": 5,
    "fulfillment_method": "Store pickup",
    "fulfillment_experience_score": 5,
    "services_selected": ["Geek Squad Protection"],
    "service_satisfaction_score": 5,
    "had_problem": False,
    "csat_score": 5,
    "ces_score": 6,
    "nps_score": 9,
    "repeat_intent": "Very likely",
    "highlights": "Associate quickly confirmed RAM compatibility",
    "improvements": "Cable wall was a bit messy",
    "age_range": "35-44",
    "tech_comfort_level": "Advanced",
    "household_role": "Primary decision-maker",
}


def test_parse_response_to_record():
    response = parse_response(SAMPLE)
    record = response.to_record()
    assert record["channel"] == "hybrid"
    assert record["categories"] == "Cables / Accessories, Computers / Tablets"
    assert record["csat_score"] == 5
    assert record["fulfillment_method"] == "Store pickup"


MODERN_SAMPLE = {
    "survey_id": "bby_cex_v1",
    "response_id": "resp_8d1a3b",
    "submitted_at": "2025-11-12T15:28:04-06:00",
    "channel": "web",
    "meta": {
        "store_id": "KY-LOU-SSH",
        "geo_zip": "40207",
        "device": "mobile",
        "language": "en-US",
    },
    "answers": {
        "q1_channel": "both",
        "q2_recency": "2_3d",
        "q3_location": "Louisville, KY (Shelbyville Rd) — Order #BBY01-7421938472",
        "q4_category": ["computers", "cables"],
        "q5_found": "exact",
        "q6_not_found_reason": [],
        "q7_find_ease": 4,
        "q8_info_clear": 5,
        "q9_staff_interact": "yes",
        "q10_knowledgeable": 5,
        "q11_nopressure": 4,
        "q12_wait_help": 4,
        "q13_price_value": 4,
        "q14_promos": ["sale", "membership"],
        "q15_value_sat": 4,
        "q16a_checkout_fast": 5,
        "q17a_pay_options": 5,
        "q16b_site_fast": 5,
        "q17b_fulfillment": "bopis",
        "q18_ready": 5,
        "q19_services": ["gsp"],
        "q20_service_sat": 5,
        "q21_issue": "no",
        "q23_csat": 5,
        "q24_ces": 6,
        "q25_nps": 9,
        "q26_repeat": "very_likely",
        "q27_worked": "Associate quickly confirmed RAM compatibility; pickup was ready 20 minutes early.",
        "q28_improve": "Cable wall a bit messy—group by connector type would help.",
        "q29_other": "Would love more side-by-side laptop displays with identical lighting.",
        "q30_age": "35_44",
        "q31_tech": "advanced",
        "q32_role": "primary",
    },
    "computed": {
        "nps_bucket": "Promoter",
        "awi": None,
    },
    "processing": {
        "text_embeddings": {
            "q27_worked": "vec_27",
            "q28_improve": "vec_28",
            "q29_other": "vec_29",
        },
        "topics": {
            "q28_improve": ["cables_display", "organization"],
            "q29_other": ["lighting_comparison", "merchandising"],
        },
        "residuals": {
            "model_version": "nps_driver_v2",
            "nps_hat": 8.4,
            "nps_residual": 0.6,
            "high_neg_resid": False,
        },
    },
}


def test_parse_modern_response():
    response = parse_response(MODERN_SAMPLE)
    record = response.to_record()

    assert record["channel"] == "hybrid"
    assert record["categories"] == "cables, computers"
    assert record["csat_score"] == 5
    assert record["fulfillment_method"] == "bopis"
    assert record["highlights"].startswith("Associate quickly confirmed")

    metadata = response.raw_metadata
    assert metadata["meta_store_id"] == "KY-LOU-SSH"
    assert metadata["survey_id"] == "bby_cex_v1"
    assert "computed" in metadata
