"""Example CLI entrypoint for the survey analysis pipeline."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from survey_analysis.pipelines import SurveyPipeline
from survey_analysis.storage import LocalJSONStorage
from survey_analysis.tableau import export_for_tableau


def load_sample_payloads(path: Path) -> list[dict]:
    if path.is_file():
        return json.loads(path.read_text())
    payloads = []
    for json_file in path.glob("*.json"):
        payloads.append(json.loads(json_file.read_text()))
    return payloads


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the survey analysis pipeline")
    parser.add_argument("payload_path", type=Path, help="Path to survey payload JSON or directory")
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path("data/exported"),
        help="Directory for local storage",
    )
    parser.add_argument(
        "--tableau-output",
        type=Path,
        default=Path("data/tableau/survey_responses.csv"),
        help="CSV export for Tableau",
    )
    args = parser.parse_args()

    payloads = load_sample_payloads(args.payload_path)
    pipeline = SurveyPipeline(LocalJSONStorage(str(args.output_directory)))
    records = pipeline.ingest_and_store(payloads)
    metrics = pipeline.run_analytics(records)

    csv_path = export_for_tableau(records, args.tableau_output)

    print("Stored", len(records), "records to", args.output_directory)
    print("Analytics summary:")
    print(json.dumps(metrics, indent=2))
    print("Tableau export created at", csv_path)


if __name__ == "__main__":
    main()
