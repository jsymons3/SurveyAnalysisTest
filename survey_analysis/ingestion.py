"""Survey ingestion utilities."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Iterable

from . import models


CHANNEL_NORMALIZATION = {
    "Visited a store": "store",
    "Shopped online at BestBuy.com / app": "online",
    "Both (researched online, purchased in store)": "hybrid",
    "store": "store",
    "in_store": "store",
    "web": "online",
    "online": "online",
    "app": "online",
    "digital": "online",
    "both": "hybrid",
    "hybrid": "hybrid",
}


def _parse_date(value: Any) -> date | None:
    if not value:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    value_str = str(value)

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(value_str, fmt).date()
        except ValueError:
            continue

    try:
        sanitized = value_str.replace("Z", "+00:00")
        return datetime.fromisoformat(sanitized).date()
    except ValueError as exc:
        raise ValueError(f"Unrecognized date format: {value_str}") from exc


def _normalize_choices(choices: Iterable[str] | None) -> list[str]:
    if not choices:
        return []
    return sorted({choice.strip() for choice in choices if choice and choice.strip()})


def _normalize_channel(raw_value: Any) -> str:
    if raw_value is None:
        return "unknown"

    direct = CHANNEL_NORMALIZATION.get(raw_value)
    if direct:
        return direct

    normalized = CHANNEL_NORMALIZATION.get(str(raw_value).strip().lower())
    if normalized:
        return normalized

    return str(raw_value)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"yes", "y", "true", "1"}:
            return True
        if lowered in {"no", "n", "false", "0"}:
            return False
    return bool(value)


def _derive_checkout_context(channel: str, fallback: str | None) -> str:
    if fallback:
        return fallback
    return {
        "store": "in_store",
        "online": "online",
        "hybrid": "hybrid",
    }.get(channel, "unknown")


def parse_response(raw: Dict[str, Any]) -> models.SurveyResponse:
    """Parse a raw JSON-like payload into a :class:`SurveyResponse`."""

    answers = raw.get("answers") if isinstance(raw.get("answers"), dict) else {}

    def _from_answers(key: str, legacy_key: str | None = None, default: Any = None) -> Any:
        if key in answers:
            return answers[key]
        if legacy_key is None:
            return default
        return raw.get(legacy_key, default)

    channel_raw = _from_answers("q1_channel", "channel") or raw.get("channel")
    normalized_channel = _normalize_channel(channel_raw)

    visit_date_source = raw.get("visit_date") or raw.get("submitted_at")
    visit_date = _parse_date(visit_date_source)

    screener = models.ScreenerVisit(
        channel=normalized_channel,
        visit_date=visit_date,
        location_or_order=_from_answers("q3_location", "location_or_order"),
        categories=_normalize_choices(_from_answers("q4_category", "categories")),
    )

    discovery = models.DiscoveryAvailability(
        product_found=_from_answers("q5_found", "product_found", "unknown"),
        obstacles=_normalize_choices(_from_answers("q6_not_found_reason", "obstacles")),
        ease_of_finding=_safe_int(_from_answers("q7_find_ease", "ease_of_finding")),
        product_information_clarity=_safe_int(
            _from_answers("q8_info_clear", "product_information_clarity")
        ),
    )

    staff = models.StaffService(
        interacted_with_associate=_coerce_bool(
            _from_answers("q9_staff_interact", "interacted_with_associate", False)
        ),
        knowledge_score=_safe_int(_from_answers("q10_knowledgeable", "knowledge_score")),
        pressure_free_score=_safe_int(_from_answers("q11_nopressure", "pressure_free_score")),
        wait_time_score=_safe_int(_from_answers("q12_wait_help", "wait_time_score")),
    )

    pricing = models.PricingPromos(
        price_alignment_score=_safe_int(
            _from_answers("q13_price_value", "price_alignment_score")
        ),
        offers_used=_normalize_choices(_from_answers("q14_promos", "offers_used")),
        value_satisfaction_score=_safe_int(
            _from_answers("q15_value_sat", "value_satisfaction_score")
        ),
    )

    checkout_context = _derive_checkout_context(
        normalized_channel, _from_answers("checkout_context", "checkout_context")
    )

    checkout = models.CheckoutFulfillment(
        context=checkout_context,
        checkout_ease_score=_safe_int(
            _from_answers("q16a_checkout_fast", "checkout_ease_score")
        ),
        payment_options_score=_safe_int(
            _from_answers("q17a_pay_options", "payment_options_score")
        ),
        site_speed_score=_safe_int(_from_answers("q16b_site_fast", "site_speed_score")),
        fulfillment_method=_from_answers("q17b_fulfillment", "fulfillment_method"),
        fulfillment_experience_score=_safe_int(
            _from_answers("q18_ready", "fulfillment_experience_score")
        ),
    )

    services = models.Services(
        services_selected=_normalize_choices(_from_answers("q19_services", "services_selected")),
        service_satisfaction_score=_safe_int(
            _from_answers("q20_service_sat", "service_satisfaction_score")
        ),
    )

    problem_resolution = models.ProblemResolution(
        had_problem=_coerce_bool(_from_answers("q21_issue", "had_problem", False)),
        resolution_ease_score=_safe_int(
            _from_answers("q22_resolution_ease", "resolution_ease_score")
        ),
        problem_description=_from_answers("q22_issue_detail", "problem_description"),
    )

    outcomes = models.OutcomeLoyalty(
        csat_score=_safe_int(_from_answers("q23_csat", "csat_score")),
        ces_score=_safe_int(_from_answers("q24_ces", "ces_score")),
        nps_score=_safe_int(_from_answers("q25_nps", "nps_score")),
        repeat_intent=_from_answers("q26_repeat", "repeat_intent"),
    )

    open_ended = models.OpenEnded(
        highlights=_from_answers("q27_worked", "highlights"),
        improvements=_from_answers("q28_improve", "improvements"),
        additional_comments=_from_answers("q29_other", "additional_comments"),
    )

    demographics = models.Demographics(
        age_range=_from_answers("q30_age", "age_range"),
        tech_comfort_level=_from_answers("q31_tech", "tech_comfort_level"),
        household_role=_from_answers("q32_role", "household_role"),
    )

    metadata = {key: value for key, value in raw.items() if key.startswith("meta_")}

    meta_block = raw.get("meta")
    if isinstance(meta_block, dict):
        metadata.update({f"meta_{key}": value for key, value in meta_block.items()})

    for block_name in ("computed", "processing"):
        block = raw.get(block_name)
        if isinstance(block, dict):
            metadata[block_name] = block

    for identifier in ("survey_id", "response_id", "submitted_at"):
        if identifier in raw:
            metadata[identifier] = raw[identifier]

    return models.SurveyResponse(
        screener_visit=screener,
        discovery=discovery,
        staff_service=staff,
        pricing_promos=pricing,
        checkout_fulfillment=checkout,
        services=services,
        problem_resolution=problem_resolution,
        outcome_loyalty=outcomes,
        open_ended=open_ended,
        demographics=demographics,
        raw_metadata=metadata,
    )


def response_to_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience helper to parse a raw payload and flatten it."""

    return parse_response(raw).to_record()


def _safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
