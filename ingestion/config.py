from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "data" / "raw"
CURATED_ROOT = ROOT / "data" / "curated"
METADATA_PATH = ROOT / "metadata" / "ingestions.parquet"


SourceKind = Literal["csv_local", "kaggle_imdb"]


@dataclass(frozen=True)
class SourceConfig:
    """Description d'une source à ingérer."""

    name: str
    kind: SourceKind
    input_path: Optional[Path] = None
    file_name: Optional[str] = None
    dataset_id: Optional[str] = None


SOURCES: tuple[SourceConfig, ...] = (
    SourceConfig(
        name="rt_reviews",
        kind="csv_local",
        input_path=ROOT / "rotten_tomatoes_movie_reviews.csv",
        file_name="rotten_tomatoes_movie_reviews.csv",
    ),
    SourceConfig(
        name="rt_movies",
        kind="csv_local",
        input_path=ROOT / "rotten_tomatoes_movies.csv",
        file_name="rotten_tomatoes_movies.csv",
    ),
    SourceConfig(
        name="imdb_kaggle",
        kind="kaggle_imdb",
        dataset_id="isaidhs/imdb-dataset",
        file_name="IMDB Dataset.csv",
    ),
)

