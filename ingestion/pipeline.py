from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Iterable

from .config import SOURCES, SourceConfig
from .csv_loader import ingest_csv
from .kaggle_loader import ingest_kaggle
from .utils import append_metadata

LOGGER = logging.getLogger("ingestion.pipeline")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def run_for_source(config: SourceConfig, run_id: str) -> dict | None:
    LOGGER.info("Début ingestion %s", config.name)
    try:
        if config.kind == "csv_local":
            result = ingest_csv(config, run_id)
        elif config.kind == "kaggle_imdb":
            result = ingest_kaggle(config, run_id)
        else:
            raise ValueError(f"Type de source inconnu: {config.kind}")
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.exception("Ingestion échouée pour %s", config.name)
        return {
            "run_id": run_id,
            "source": config.name,
            "status": "failed",
            "content_hash": "",
            "row_count": 0,
            "byte_size": 0,
            "raw_path": "",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }

    if result.get("status") == "completed":
        append_metadata(result)
    return result


def run_pipeline(selected_sources: Iterable[str] | None = None) -> list[dict]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    selected = set(selected_sources) if selected_sources else None
    results: list[dict] = []
    for config in SOURCES:
        if selected and config.name not in selected:
            continue
        result = run_for_source(config, run_id)
        if result:
            results.append(result)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline d'ingestion batch.")
    parser.add_argument(
        "--source",
        action="append",
        help="Nom de la source à lancer (répétable). Sans option = toutes.",
    )
    args = parser.parse_args()
    results = run_pipeline(args.source)
    for record in results:
        LOGGER.info("Résultat %s: %s", record["source"], record["status"])


if __name__ == "__main__":
    main()

