from __future__ import annotations

import logging
from pathlib import Path

from .config import SourceConfig
from .utils import (
    already_ingested,
    build_metadata_record,
    compute_sha256,
    copy_with_metadata,
    count_lines,
    make_run_folder,
    write_success_marker,
)

logger = logging.getLogger(__name__)


def ingest_csv(config: SourceConfig, run_id: str) -> dict:
    if not config.input_path or not config.input_path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable pour la source {config.name}: {config.input_path}"
        )

    src = config.input_path
    file_hash = compute_sha256(src)
    if already_ingested(config.name, file_hash):
        logger.info(
            "Source %s déjà ingérée (hash=%s), saut de la copie",
            config.name,
            file_hash,
        )
        return {
            "source": config.name,
            "status": "skipped",
            "reason": "hash déjà présent",
            "content_hash": file_hash,
        }

    run_folder = make_run_folder(config.name, run_id)
    dst_path = copy_with_metadata(src, run_folder)
    rows = count_lines(dst_path) - 1  # enlever header
    byte_size = dst_path.stat().st_size

    write_success_marker(
        run_folder,
        {
            "source": config.name,
            "file": dst_path.name,
            "rows": rows,
            "hash": file_hash,
        },
    )

    record = build_metadata_record(
        run_id=run_id,
        source=config.name,
        status="completed",
        content_hash=file_hash,
        row_count=rows,
        byte_size=byte_size,
        raw_path=dst_path,
    )
    return record

