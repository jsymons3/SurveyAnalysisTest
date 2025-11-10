"""Utilities to prepare data for Tableau visualizations."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping


try:  # pragma: no cover - optional dependency
    import tableauserverclient as TSC  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    TSC = None


def export_for_tableau(records: Iterable[Mapping[str, object]], path: str | Path) -> Path:
    """Export survey data to a CSV file ready for Tableau ingestion."""

    import pandas as pd  # type: ignore

    frame = pd.DataFrame(list(records))
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def publish_to_tableau_server(
    *,
    csv_path: Path,
    project: str,
    datasource_name: str,
    server_url: str,
    site_id: str,
    token_name: str,
    token_secret: str,
) -> None:
    """Publish a prepared CSV to Tableau Server/Cloud."""

    if TSC is None:  # pragma: no cover - optional dependency
        raise RuntimeError("tableauserverclient is not installed")

    auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_id)
    server = TSC.Server(server_url, use_server_version=True)

    with server.auth.sign_in(auth):
        all_projects, _ = server.projects.get()
        project_id = next((proj.id for proj in all_projects if proj.name == project), None)
        if project_id is None:
            raise ValueError(f"Project '{project}' not found")

        datasource_item = TSC.DatasourceItem(project_id, name=datasource_name)
        server.datasources.publish(datasource_item, str(csv_path), mode=TSC.Server.PublishMode.Overwrite)
