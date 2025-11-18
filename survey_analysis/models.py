"""Data models for survey analysis."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional


@dataclass(slots=True)
class ScreenerVisit:
    """Top-level screener and visit context information."""

    channel: str
    visit_date: Optional[date]
    location_or_order: Optional[str]
    categories: List[str] = field(default_factory=list)


@dataclass(slots=True)
class DiscoveryAvailability:
    """Discovery and product availability experience."""

    product_found: str
    obstacles: List[str] = field(default_factory=list)
    ease_of_finding: Optional[int] = None
    product_information_clarity: Optional[int] = None


@dataclass(slots=True)
class StaffService:
    """Staff or service interaction feedback."""

    interacted_with_associate: bool
    knowledge_score: Optional[int] = None
    pressure_free_score: Optional[int] = None
    wait_time_score: Optional[int] = None


@dataclass(slots=True)
class PricingPromos:
    """Pricing, promotions, and membership feedback."""

    price_alignment_score: Optional[int] = None
    offers_used: List[str] = field(default_factory=list)
    value_satisfaction_score: Optional[int] = None


@dataclass(slots=True)
class CheckoutFulfillment:
    """Checkout or fulfillment experience depending on the channel."""

    context: str
    checkout_ease_score: Optional[int] = None
    payment_options_score: Optional[int] = None
    site_speed_score: Optional[int] = None
    fulfillment_method: Optional[str] = None
    fulfillment_experience_score: Optional[int] = None


@dataclass(slots=True)
class Services:
    """Optional services the customer purchased or considered."""

    services_selected: List[str] = field(default_factory=list)
    service_satisfaction_score: Optional[int] = None


@dataclass(slots=True)
class ProblemResolution:
    """Problem resolution feedback."""

    had_problem: bool
    resolution_ease_score: Optional[int] = None
    problem_description: Optional[str] = None


@dataclass(slots=True)
class OutcomeLoyalty:
    """Outcome and loyalty metrics such as CSAT, CES, and NPS."""

    csat_score: Optional[int] = None
    ces_score: Optional[int] = None
    nps_score: Optional[int] = None
    repeat_intent: Optional[str] = None


@dataclass(slots=True)
class OpenEnded:
    """Open-ended verbatim feedback."""

    highlights: Optional[str] = None
    improvements: Optional[str] = None
    additional_comments: Optional[str] = None


@dataclass(slots=True)
class Demographics:
    """Demographic information provided by the respondent."""

    age_range: Optional[str] = None
    tech_comfort_level: Optional[str] = None
    household_role: Optional[str] = None


@dataclass(slots=True)
class SurveyResponse:
    """Full survey response encapsulating all sections."""

    screener_visit: ScreenerVisit
    discovery: DiscoveryAvailability
    staff_service: StaffService
    pricing_promos: PricingPromos
    checkout_fulfillment: CheckoutFulfillment
    services: Services
    problem_resolution: ProblemResolution
    outcome_loyalty: OutcomeLoyalty
    open_ended: OpenEnded
    demographics: Demographics
    raw_metadata: dict = field(default_factory=dict)

    def to_record(self) -> dict:
        """Flatten the response into a dictionary suitable for analytics."""

        record = {
            "channel": self.screener_visit.channel,
            "visit_date": self.screener_visit.visit_date.isoformat()
            if self.screener_visit.visit_date
            else None,
            "location_or_order": self.screener_visit.location_or_order,
            "categories": ", ".join(self.screener_visit.categories),
            "product_found": self.discovery.product_found,
            "obstacles": ", ".join(self.discovery.obstacles),
            "ease_of_finding": self.discovery.ease_of_finding,
            "product_info_clarity": self.discovery.product_information_clarity,
            "interacted_with_associate": self.staff_service.interacted_with_associate,
            "knowledge_score": self.staff_service.knowledge_score,
            "pressure_free_score": self.staff_service.pressure_free_score,
            "wait_time_score": self.staff_service.wait_time_score,
            "price_alignment_score": self.pricing_promos.price_alignment_score,
            "offers_used": ", ".join(self.pricing_promos.offers_used),
            "value_satisfaction_score": self.pricing_promos.value_satisfaction_score,
            "checkout_context": self.checkout_fulfillment.context,
            "checkout_ease_score": self.checkout_fulfillment.checkout_ease_score,
            "payment_options_score": self.checkout_fulfillment.payment_options_score,
            "site_speed_score": self.checkout_fulfillment.site_speed_score,
            "fulfillment_method": self.checkout_fulfillment.fulfillment_method,
            "fulfillment_experience_score": self.checkout_fulfillment.fulfillment_experience_score,
            "services_selected": ", ".join(self.services.services_selected),
            "service_satisfaction_score": self.services.service_satisfaction_score,
            "had_problem": self.problem_resolution.had_problem,
            "resolution_ease_score": self.problem_resolution.resolution_ease_score,
            "problem_description": self.problem_resolution.problem_description,
            "csat_score": self.outcome_loyalty.csat_score,
            "ces_score": self.outcome_loyalty.ces_score,
            "nps_score": self.outcome_loyalty.nps_score,
            "repeat_intent": self.outcome_loyalty.repeat_intent,
            "highlights": self.open_ended.highlights,
            "improvements": self.open_ended.improvements,
            "additional_comments": self.open_ended.additional_comments,
            "age_range": self.demographics.age_range,
            "tech_comfort_level": self.demographics.tech_comfort_level,
            "household_role": self.demographics.household_role,
        }
        record.update(_serialise_metadata(self.raw_metadata))
        return record


def _serialise_metadata(metadata: dict) -> dict:
    """Normalise metadata values for Spark compatibility."""

    import json

    normalised = {}
    for key, value in metadata.items():
        if value is None or isinstance(value, (str, int, float, bool)):
            normalised[key] = value
        else:
            try:
                normalised[key] = json.dumps(value, sort_keys=True)
            except (TypeError, ValueError):
                normalised[key] = str(value)
    return normalised
