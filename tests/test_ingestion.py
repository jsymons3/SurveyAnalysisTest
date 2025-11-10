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
