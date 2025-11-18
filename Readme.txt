# Survey Analysis System

An end-to-end analytics workflow that captures Best Buy customer experience surveys, stores them on AWS, analyzes results with Apache Spark and Snowflake, and prepares curated datasets for Tableau dashboards.

## Features

- **Structured data model** for the multi-section survey, including screener, discovery, service, pricing, fulfillment, loyalty, and demographic components.
- **Ingestion pipeline** that normalizes raw survey payloads into a consistent schema.
- **AWS-ready storage layer** with pluggable backends for S3 or local JSON persistence.
- **Analytics module** that runs channel-level KPIs in Apache Spark with a Python fallback when Spark is unavailable.
- **Snowflake loader** that creates and populates analytical tables via the Snowflake Python connector.
- **Tableau integration helpers** that export curated datasets as CSV files and optionally publish to Tableau Server.
- **Command-line script** demonstrating how to stitch ingestion, storage, analytics, and Tableau export together.
- **Interactive landing page** (`web/index.html`) for dragging-and-dropping survey JSON to preview channel KPIs before running the Spark pipeline.

## Repository Structure

```
SurveyAnalysisTest/
├── survey_analysis/
│   ├── __init__.py
│   ├── analytics.py
│   ├── ingestion.py
│   ├── models.py
│   ├── pipelines.py
│   ├── snowflake_loader.py
│   ├── storage.py
│   └── tableau.py
├── scripts/
│   └── run_pipeline.py
├── tests/
│   └── test_ingestion.py
├── pyproject.toml
└── Readme.txt
```

## Getting Started

1. **Install dependencies** (base plus optional extras as needed):

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -e .[aws,spark,snowflake,tableau]
   ```

2. **Prepare survey payloads** as JSON files. Each file should match the keys in the survey form (see `tests/test_ingestion.py` for an example payload).

3. **(Optional) Validate data visually**

   Open `web/index.html` in a browser to access the landing page. Drag your `surveys.json` file into the drop zone to preview CSAT, CES, NPS, and problem counts per channel without leaving your machine. This is a quick sanity check before firing up Spark.

4. **Run the pipeline** to ingest and analyze data locally:

   ```bash
   python scripts/run_pipeline.py data/sample_payloads --output-directory data/exported --tableau-output data/tableau/survey_responses.csv
   ```

   The script will:

   - Normalize all responses and store them via the configured backend (defaults to local JSON files).
   - Produce channel-level KPI summaries using Spark if available.
   - Generate a Tableau-ready CSV export.

5. **Load data into Snowflake** (optional):

   ```python
   from survey_analysis.snowflake_loader import SnowflakeSurveyLoader
   loader = SnowflakeSurveyLoader({
       "user": "YOUR_USER",
       "password": "YOUR_PASSWORD",
       "account": "YOUR_ACCOUNT",
       "warehouse": "ANALYTICS_WH",
       "database": "CUSTOMER_INSIGHTS",
       "schema": "SURVEYS",
   })
   loader.create_table_if_needed()
   loader.load_records(records)
   ```

6. **Publish to Tableau** (optional):

   ```python
   from pathlib import Path
   from survey_analysis.tableau import publish_to_tableau_server

   publish_to_tableau_server(
       csv_path=Path("data/tableau/survey_responses.csv"),
       project="Customer Insights",
       datasource_name="Best Buy Survey",
       server_url="https://tableau.yourcompany.com",
       site_id="bestbuy",
       token_name="TABLEAU_TOKEN_NAME",
       token_secret="TABLEAU_TOKEN_SECRET",
   )
   ```

## Data Model Highlights

- **`SurveyResponse` dataclass** encapsulates all sections of the survey and exposes a `to_record()` method for flattening data into analytics-friendly dictionaries.
- **Normalization helpers** ensure choice lists and dates are standardized for storage and analytics.

## Testing

Run unit tests with `pytest`:

```bash
pytest
```

## Extending the System

- Add new derived metrics by extending `survey_analysis/analytics.py`.
- Plug in alternate storage backends by implementing the `StorageBackend` protocol in `survey_analysis/storage.py`.
- Customize Tableau exports by modifying `survey_analysis/tableau.py` or generating Hyper extracts via the Tableau Hyper API.

## Security & Operations

- Store AWS, Snowflake, and Tableau credentials in secure secret managers such as AWS Secrets Manager or HashiCorp Vault.
- Enable encryption at rest for S3 buckets and Snowflake tables.
- Use CI/CD pipelines to lint, test, and deploy analytics updates.

## License

This repository is provided as a reference architecture; customize licensing according to your organization's policies.
