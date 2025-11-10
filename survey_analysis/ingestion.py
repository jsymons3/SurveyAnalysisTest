"""Survey ingestion utilities."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable

from . import models


CHANNEL_NORMALIZATION = {
    "Visited a store": "store",
    "Shopped online at BestBuy.com / app": "online",
    "Both (researched online, purchased in store)": "hybrid",
}


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {value}")


def _normalize_choices(choices: Iterable[str] | None) -> list[str]:
    if not choices:
        return []
    return sorted({choice.strip() for choice in choices if choice and choice.strip()})


def parse_response(raw: Dict[str, Any]) -> models.SurveyResponse:
    """Parse a raw JSON-like payload into a :class:`SurveyResponse`."""

    screener = models.ScreenerVisit(
        channel=CHANNEL_NORMALIZATION.get(raw.get("channel"), raw.get("channel", "unknown")),
        visit_date=_parse_date(raw.get("visit_date")),
        location_or_order=raw.get("location_or_order"),
        categories=_normalize_choices(raw.get("categories")),
    )

    discovery = models.DiscoveryAvailability(
        product_found=raw.get("product_found", "unknown"),
        obstacles=_normalize_choices(raw.get("obstacles")),
        ease_of_finding=_safe_int(raw.get("ease_of_finding")),
        product_information_clarity=_safe_int(raw.get("product_information_clarity")),
    )

    staff = models.StaffService(
        interacted_with_associate=bool(raw.get("interacted_with_associate", False)),
        knowledge_score=_safe_int(raw.get("knowledge_score")),
        pressure_free_score=_safe_int(raw.get("pressure_free_score")),
        wait_time_score=_safe_int(raw.get("wait_time_score")),
    )

    pricing = models.PricingPromos(
        price_alignment_score=_safe_int(raw.get("price_alignment_score")),
        offers_used=_normalize_choices(raw.get("offers_used")),
        value_satisfaction_score=_safe_int(raw.get("value_satisfaction_score")),
    )

    checkout = models.CheckoutFulfillment(
        context=raw.get("checkout_context", "unknown"),
        checkout_ease_score=_safe_int(raw.get("checkout_ease_score")),
        payment_options_score=_safe_int(raw.get("payment_options_score")),
        site_speed_score=_safe_int(raw.get("site_speed_score")),
        fulfillment_method=raw.get("fulfillment_method"),
        fulfillment_experience_score=_safe_int(raw.get("fulfillment_experience_score")),
    )

    services = models.Services(
        services_selected=_normalize_choices(raw.get("services_selected")),
        service_satisfaction_score=_safe_int(raw.get("service_satisfaction_score")),
    )

    problem_resolution = models.ProblemResolution(
        had_problem=bool(raw.get("had_problem", False)),
        resolution_ease_score=_safe_int(raw.get("resolution_ease_score")),
        problem_description=raw.get("problem_description"),
    )

    outcomes = models.OutcomeLoyalty(
        csat_score=_safe_int(raw.get("csat_score")),
        ces_score=_safe_int(raw.get("ces_score")),
        nps_score=_safe_int(raw.get("nps_score")),
        repeat_intent=raw.get("repeat_intent"),
    )

    open_ended = models.OpenEnded(
        highlights=raw.get("highlights"),
        improvements=raw.get("improvements"),
        additional_comments=raw.get("additional_comments"),
    )

    demographics = models.Demographics(
        age_range=raw.get("age_range"),
        tech_comfort_level=raw.get("tech_comfort_level"),
        household_role=raw.get("household_role"),
    )

    metadata = {key: value for key, value in raw.items() if key.startswith("meta_")}

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
