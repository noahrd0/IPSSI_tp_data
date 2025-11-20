from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .config import METADATA_PATH, RAW_ROOT

METADATA_COLUMNS = [
    "run_id",
    "source",
    "status",
    "content_hash",
    "row_count",
    "byte_size",
    "raw_path",
    "created_at",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def compute_sha256(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def copy_with_metadata(src: Path, dst_dir: Path) -> Path:
    ensure_dir(dst_dir)
    dst_path = dst_dir / src.name
    shutil.copy2(src, dst_path)
    return dst_path


def write_success_marker(dst_dir: Path, payload: dict[str, Any]) -> None:
    marker = dst_dir / "_SUCCESS.json"
    marker.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_metadata() -> pd.DataFrame:
    if METADATA_PATH.exists():
        return pd.read_parquet(METADATA_PATH)
    return pd.DataFrame(columns=METADATA_COLUMNS)


def append_metadata(record: dict[str, Any]) -> None:
    df = load_metadata()
    df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
    ensure_dir(METADATA_PATH.parent)
    df.to_parquet(METADATA_PATH, index=False)


def already_ingested(source: str, content_hash: str) -> bool:
    df = load_metadata()
    if df.empty:
        return False
    existing = df[
        (df["source"] == source) & (df["content_hash"] == content_hash)
    ]
    return not existing.empty


def count_lines(csv_path: Path) -> int:
    count = -1
    with csv_path.open("r", encoding="utf-8", errors="ignore") as fh:
        for count, _ in enumerate(fh, start=0):
            pass
    return max(count, 0)


def make_run_folder(source: str, run_id: str) -> Path:
    path = RAW_ROOT / source / run_id
    ensure_dir(path)
    return path


def build_metadata_record(
    *,
    run_id: str,
    source: str,
    status: str,
    content_hash: str,
    row_count: int,
    byte_size: int,
    raw_path: Path,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "source": source,
        "status": status,
        "content_hash": content_hash,
        "row_count": row_count,
        "byte_size": byte_size,
        "raw_path": str(raw_path),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

