from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
CURATED_ROOT = PROJECT_ROOT / "data" / "curated"
METADATA_DB = PROJECT_ROOT / "metadata" / "lake.duckdb"

logger = logging.getLogger("etl.transform")
logging.basicConfig(
    level=os.getenv("ETL_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def latest_raw_file(source: str, file_name: str) -> Path:
    source_dir = RAW_ROOT / source
    if not source_dir.exists():
        raise FileNotFoundError(f"Aucun dump brut pour {source}")
    run_dirs = sorted(
        [d for d in source_dir.iterdir() if d.is_dir()], reverse=True
    )
    for run_dir in run_dirs:
        candidate = run_dir / file_name
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"{file_name} introuvable pour la source {source}")


def parse_currency(value: str | float | int | None) -> float | None:
    if value in (None, "", "N/A") or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    value = value.strip().replace(",", "")
    multiplier = 1.0
    if value.endswith("M"):
        multiplier = 1_000_000
        value = value[:-1]
    elif value.endswith("K"):
        multiplier = 1_000
        value = value[:-1]
    value = value.replace("$", "")
    try:
        return float(value) * multiplier
    except ValueError:
        return None


def parse_runtime(value: str | float | int | None) -> float | None:
    if value in (None, "", "N/A") or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"(\d+)", str(value))
    if match:
        return float(match.group(1))
    return None


GRADE_MAP = {
    "A+": 4.0,
    "A": 4.0,
    "A-": 3.7,
    "B+": 3.3,
    "B": 3.0,
    "B-": 2.7,
    "C+": 2.3,
    "C": 2.0,
    "C-": 1.7,
    "D+": 1.3,
    "D": 1.0,
    "D-": 0.7,
    "F": 0.0,
}


def normalize_review_score(value: str | None) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    value = str(value).strip()
    if not value or value == "N/A":
        return None
    frac = re.match(r"(?P<num>\d+(?:\.\d+)?)/(?P<den>\d+(?:\.\d+)?)", value)
    if frac:
        num = float(frac.group("num"))
        den = float(frac.group("den"))
        if den == 0:
            return None
        return num / den
    if value in GRADE_MAP:
        return GRADE_MAP[value] / 4
    try:
        return float(value)
    except ValueError:
        return None


def safe_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def safe_to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def explode_people(df: pd.DataFrame, column: str, role: str) -> pd.DataFrame:
    records: list[dict] = []
    for film_id, names in df[["film_id", column]].dropna().itertuples(index=False):
        for name in str(names).split(","):
            clean = name.strip()
            if clean:
                records.append({"film_id": film_id, "name": clean, "role": role})
    return pd.DataFrame(records)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp.parquet")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)
    logger.info("DataFrame écrit (%s lignes) -> %s", len(df), path)


def build_films_table() -> pd.DataFrame:
    rt_movies_path = latest_raw_file(
        "rt_movies", "rotten_tomatoes_movies.csv"
    )
    imdb_path = latest_raw_file("imdb_kaggle", "IMDB Dataset.csv")

    rt_df = pd.read_csv(rt_movies_path)
    rt_df = rt_df.rename(columns={"id": "rt_id"})
    rt_df["runtime_minutes"] = safe_to_numeric(rt_df["runtimeMinutes"])
    rt_df["audienceScore"] = safe_to_numeric(rt_df["audienceScore"])
    rt_df["tomatoMeter"] = safe_to_numeric(rt_df["tomatoMeter"])
    rt_df["releaseDateTheaters"] = safe_to_datetime(rt_df["releaseDateTheaters"])
    rt_df["box_office_usd"] = rt_df["boxOffice"].apply(parse_currency)
    rt_df["primary_genre"] = rt_df["genre"].str.split(",").str[0].str.strip()
    rt_df = rt_df.rename(
        columns={
            "title": "title_rt",
            "runtime_minutes": "runtime_minutes_rt",
            "box_office_usd": "box_office_usd_rt",
            "primary_genre": "primary_genre_rt",
            "releaseDateTheaters": "releaseDateTheaters_rt",
            "releaseDateStreaming": "releaseDateStreaming_rt",
            "director": "director_rt",
            "writer": "writer_rt",
        }
    )

    imdb_df = pd.read_csv(imdb_path)
    imdb_df = imdb_df.rename(
        columns={
            "Title": "title_imdb",
            "Year": "year_imdb",
        }
    )
    imdb_df["runtime_minutes"] = imdb_df["Runtime"].apply(parse_runtime)
    imdb_df["box_office_usd"] = imdb_df["BoxOffice"].apply(parse_currency)
    imdb_df["year_imdb"] = safe_to_numeric(imdb_df["year_imdb"])
    imdb_df["imdbRating"] = safe_to_numeric(imdb_df["imdbRating"])
    imdb_df["Metascore"] = safe_to_numeric(imdb_df["Metascore"])
    imdb_df = imdb_df.rename(
        columns={
            "runtime_minutes": "runtime_minutes_imdb",
            "box_office_usd": "box_office_usd_imdb",
        }
    )
    imdb_df["imdbVotes"] = (
        imdb_df["imdbVotes"].astype(str).str.replace(",", "", regex=False)
    )
    imdb_df["imdbVotes"] = safe_to_numeric(imdb_df["imdbVotes"])
    imdb_df["rt_id"] = (
        imdb_df["tomatoURL"]
        .astype(str)
        .str.extract(r"/m/([^/]+)/?")
        .iloc[:, 0]
    )

    films = pd.merge(
        rt_df,
        imdb_df,
        how="outer",
        on="rt_id",
        suffixes=("_rt", "_imdb"),
    )
    films["film_id"] = films["imdbID"].fillna(films["rt_id"])
    films["title"] = films["title_imdb"].fillna(films.get("title_rt"))
    films["runtime_minutes"] = films["runtime_minutes_imdb"].fillna(
        films.get("runtime_minutes_rt")
    )
    films["box_office_usd"] = films["box_office_usd_imdb"].fillna(
        films.get("box_office_usd_rt")
    )
    films["year"] = films["year_imdb"].fillna(
        pd.to_datetime(films.get("releaseDateTheaters_rt")).dt.year
    )
    films["revenue_per_minute"] = films["box_office_usd"] / films[
        "runtime_minutes"
    ]
    films["primary_genre"] = films.get("primary_genre_rt").fillna(
        films["Genre"].str.split(",").str[0].str.strip()
    )
    column_map = {
        "film_id": "film_id",
        "rt_id": "rt_id",
        "imdbID": "imdb_id",
        "title": "title",
        "year": "year",
        "primary_genre": "primary_genre",
        "runtime_minutes": "runtime_minutes",
        "audienceScore": "audience_score",
        "tomatoMeter": "tomato_meter",
        "imdbRating": "imdb_rating",
        "imdbVotes": "imdb_votes",
        "Metascore": "metascore",
        "box_office_usd": "box_office_usd",
        "revenue_per_minute": "revenue_per_minute",
        "director_rt": "rt_director",
        "writer_rt": "rt_writer",
        "Director": "imdb_director",
        "Writer": "imdb_writer",
        "Actors": "actors",
        "Language": "language",
        "Country": "country",
        "releaseDateTheaters_rt": "rt_release_date",
        "releaseDateStreaming_rt": "rt_streaming_date",
    }
    films = films[list(column_map.keys())].rename(columns=column_map)
    films = films.drop_duplicates(subset=["film_id"])
    return films


def build_reviews_table() -> pd.DataFrame:
    reviews_path = latest_raw_file(
        "rt_reviews", "rotten_tomatoes_movie_reviews.csv"
    )
    df = pd.read_csv(reviews_path)
    df = df.rename(
        columns={
            "id": "rt_id",
            "publicatioName": "publicationName",
        }
    )
    df["created_at"] = safe_to_datetime(df["creationDate"])
    df["score_ratio"] = df["originalScore"].apply(normalize_review_score)
    df["is_top_critic"] = df["isTopCritic"].astype(bool)
    df["sentiment"] = df["reviewState"].str.lower().map(
        {"fresh": 1, "rotten": 0}
    )
    return df[
        [
            "rt_id",
            "reviewId",
            "criticName",
            "publicationName",
            "is_top_critic",
            "score_ratio",
            "scoreSentiment",
            "sentiment",
            "reviewText",
            "created_at",
        ]
    ]


def build_people_table(films: pd.DataFrame) -> pd.DataFrame:
    directors_rt = explode_people(films, "rt_director", "director_rt")
    directors_imdb = explode_people(films, "imdb_director", "director_imdb")
    actors = explode_people(films, "actors", "actor")
    writers_rt = explode_people(films, "rt_writer", "writer_rt")
    writers_imdb = explode_people(films, "imdb_writer", "writer_imdb")
    people = pd.concat(
        [directors_rt, directors_imdb, actors, writers_rt, writers_imdb],
        ignore_index=True,
    )
    people = people.dropna()
    people["person_id"] = (
        people["name"]
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(".", "", regex=False)
    )
    return people.drop_duplicates()


def materialize_duckdb() -> None:
    CURATED_ROOT.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(METADATA_DB)
    conn.execute("PRAGMA threads=4;")
    conn.execute(
        "CREATE OR REPLACE TABLE films AS SELECT * FROM read_parquet(?)",
        (str(CURATED_ROOT / "films" / "films.parquet"),),
    )
    conn.execute(
        "CREATE OR REPLACE TABLE reviews AS SELECT * FROM read_parquet(?)",
        (str(CURATED_ROOT / "reviews" / "reviews.parquet"),),
    )
    conn.execute(
        "CREATE OR REPLACE TABLE people AS SELECT * FROM read_parquet(?)",
        (str(CURATED_ROOT / "people" / "people.parquet"),),
    )
    conn.close()
    logger.info("Tables matérialisées dans %s", METADATA_DB)


def run_etl() -> None:
    films = build_films_table()
    reviews = build_reviews_table()
    people = build_people_table(films)

    write_parquet(films, CURATED_ROOT / "films" / "films.parquet")
    write_parquet(reviews, CURATED_ROOT / "reviews" / "reviews.parquet")
    write_parquet(people, CURATED_ROOT / "people" / "people.parquet")

    materialize_duckdb()


if __name__ == "__main__":
    run_etl()

