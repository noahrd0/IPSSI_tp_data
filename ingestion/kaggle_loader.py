from __future__ import annotations

import logging
from pathlib import Path

import kagglehub

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


def _download_dataset(config: SourceConfig) -> Path:
    if not config.dataset_id:
        raise ValueError("dataset_id obligatoire pour les sources Kaggle")
    dataset_path = Path(kagglehub.dataset_download(config.dataset_id))
    if config.file_name:
        file_path = dataset_path / config.file_name
    else:
        raise ValueError("file_name obligatoire pour les sources Kaggle")
    if not file_path.exists():
        raise FileNotFoundError(
            f"Fichier {config.file_name} introuvable dans {dataset_path}"
        )
    return file_path


def ingest_kaggle(config: SourceConfig, run_id: str) -> dict:
    src = _download_dataset(config)
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
    rows = count_lines(dst_path) - 1
    byte_size = dst_path.stat().st_size

    write_success_marker(
        run_folder,
        {
            "source": config.name,
            "dataset": config.dataset_id,
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

