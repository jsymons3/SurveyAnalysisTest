"""Snowflake integration utilities."""
from __future__ import annotations

from typing import Iterable, Mapping


class SnowflakeSurveyLoader:
    """Load flattened survey records into Snowflake tables."""

    def __init__(self, connection_kwargs: dict[str, str], table_name: str = "SURVEY_RESPONSES"):
        self.connection_kwargs = connection_kwargs
        self.table_name = table_name

    def create_table_if_needed(self) -> None:
        import snowflake.connector  # type: ignore

        columns = """
            channel STRING,
            visit_date DATE,
            location_or_order STRING,
            categories STRING,
            product_found STRING,
            obstacles STRING,
            ease_of_finding NUMBER,
            product_info_clarity NUMBER,
            interacted_with_associate BOOLEAN,
            knowledge_score NUMBER,
            pressure_free_score NUMBER,
            wait_time_score NUMBER,
            price_alignment_score NUMBER,
            offers_used STRING,
            value_satisfaction_score NUMBER,
            checkout_context STRING,
            checkout_ease_score NUMBER,
            payment_options_score NUMBER,
            site_speed_score NUMBER,
            fulfillment_method STRING,
            fulfillment_experience_score NUMBER,
            services_selected STRING,
            service_satisfaction_score NUMBER,
            had_problem BOOLEAN,
            resolution_ease_score NUMBER,
            problem_description STRING,
            csat_score NUMBER,
            ces_score NUMBER,
            nps_score NUMBER,
            repeat_intent STRING,
            highlights STRING,
            improvements STRING,
            additional_comments STRING,
            age_range STRING,
            tech_comfort_level STRING,
            household_role STRING
        """

        with snowflake.connector.connect(**self.connection_kwargs) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns})"
                )

    def load_records(self, records: Iterable[Mapping[str, object]]) -> None:
        import pandas as pd  # type: ignore
        import snowflake.connector  # type: ignore
        from snowflake.connector.pandas_tools import write_pandas  # type: ignore

        frame = pd.DataFrame(list(records))
        with snowflake.connector.connect(**self.connection_kwargs) as connection:
            write_pandas(connection, frame, self.table_name, quote_identifiers=False)
