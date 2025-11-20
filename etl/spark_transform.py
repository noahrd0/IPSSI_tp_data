from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable
import shutil
import uuid

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_URI = f"file://{PROJECT_ROOT}/data/raw"
DEFAULT_CURATED_URI = f"file://{PROJECT_ROOT}/data/curated"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ETL PySpark pour le DataLake Ciné."
    )
    parser.add_argument(
        "--raw-base-uri",
        default=os.environ.get("RAW_BASE_URI", DEFAULT_RAW_URI),
        help="Préfixe des dumps bruts (ex. hdfs://namenode:8020/datalake/raw).",
    )
    parser.add_argument(
        "--curated-base-uri",
        default=os.environ.get("CURATED_BASE_URI", DEFAULT_CURATED_URI),
        help="Destination des tables curated (ex. hdfs://.../datalake/curated).",
    )
    parser.add_argument(
        "--local-mirror",
        default=os.environ.get(
            "LOCAL_CURATED_MIRROR", str(PROJECT_ROOT / "data" / "curated")
        ),
        help="(Optionnel) Répertoire local miroir utilisé par Streamlit.",
    )
    parser.add_argument(
        "--spark-conf",
        action="append",
        default=[],
        help="Clés Spark supplémentaires au format key=value (répéter l'option).",
    )
    return parser.parse_args()


def build_spark(conf_pairs: Iterable[str]) -> SparkSession:
    builder = SparkSession.builder.appName("cine-spark-etl")
    for pair in conf_pairs:
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        builder = builder.config(key.strip(), value.strip())
    return builder.getOrCreate()


def clean_base(base_uri: str) -> str:
    return base_uri.rstrip("/")


def read_source_dir(spark: SparkSession, uri: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.csv")
        .csv(uri)
    )


def read_with_fallback(
    spark: SparkSession, candidates: tuple[str, ...]
) -> DataFrame:
    errors: list[Exception] = []
    for uri in candidates:
        try:
            return read_source_dir(spark, uri)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
    raise RuntimeError(
        f"Impossible de charger les données parmi: {candidates}"
    ) from (errors[-1] if errors else None)


@F.udf(returnType=T.DoubleType())
def parse_currency(value: str | None) -> float | None:
    if not value or value == "N/A":
        return None
    val = value.strip().replace(",", "")
    multiplier = 1.0
    if val.endswith("M"):
        multiplier = 1_000_000
        val = val[:-1]
    elif val.endswith("K"):
        multiplier = 1_000
        val = val[:-1]
    val = val.replace("$", "")
    try:
        return float(val) * multiplier
    except ValueError:
        return None


@F.udf(returnType=T.DoubleType())
def parse_runtime(value: str | None) -> float | None:
    if not value or value == "N/A":
        return None
    digits = "".join(ch for ch in value if ch.isdigit() or ch == ".")
    return float(digits) if digits else None


@F.udf(returnType=T.DoubleType())
def normalize_score(value: str | None) -> float | None:
    if not value or value == "N/A":
        return None
    text = value.strip()
    if "/" in text:
        num, den = text.split("/", 1)
        try:
            num_f = float(num)
            den_f = float(den)
            if den_f == 0:
                return None
            return num_f / den_f
        except ValueError:
            return None
    letter_scale = {
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
    if text in letter_scale:
        return letter_scale[text] / 4.0
    try:
        return float(text)
    except ValueError:
        return None


def safe_double(column: F.Column) -> F.Column:
    col_str = F.trim(column.cast("string"))
    numeric_part = F.regexp_extract(col_str, r"([-+]?\d+(?:\.\d+)?)", 1)
    return F.when(
        col_str.isNull()
        | (col_str == "")
        | (F.lower(col_str) == "n/a")
        | (numeric_part == ""),
        F.lit(None).cast("double"),
    ).otherwise(numeric_part.cast("double"))


def safe_timestamp(column: F.Column) -> F.Column:
    col_str = F.trim(column.cast("string"))
    valid = col_str.rlike(r"^\d{4}-\d{2}-\d{2}")
    cleaned = F.when(
        col_str.isNull()
        | (col_str == "")
        | (F.lower(col_str) == "n/a")
        | (~valid),
        None,
    ).otherwise(col_str)
    return F.to_timestamp(cleaned)


def safe_boolean(column: F.Column) -> F.Column:
    col_str = F.lower(F.trim(column.cast("string")))
    true_vals = F.array(F.lit("true"), F.lit("1"), F.lit("yes"), F.lit("y"))
    false_vals = F.array(F.lit("false"), F.lit("0"), F.lit("no"), F.lit("n"))
    is_true = F.array_contains(true_vals, col_str)
    is_false = F.array_contains(false_vals, col_str)
    return F.when(col_str.isNull() | (col_str == ""), None).when(is_true, F.lit(True)).when(is_false, F.lit(False)).otherwise(None)


def load_rt_movies(spark: SparkSession, raw_base: str) -> DataFrame:
    base = clean_base(raw_base)
    df = read_with_fallback(
        spark,
        (
            f"{base}/rt_movies",
            f"{clean_base(DEFAULT_RAW_URI)}/rt_movies",
        ),
    )
    df = df.withColumnRenamed("id", "rt_id")
    df = df.select(
        "rt_id",
        F.col("title").alias("title_rt"),
        safe_double(F.col("audienceScore")).alias("audience_score"),
        safe_double(F.col("tomatoMeter")).alias("tomato_meter"),
        parse_runtime(F.col("runtimeMinutes")).alias("runtime_minutes_rt"),
        safe_timestamp(F.col("releaseDateTheaters")).alias("release_date_rt"),
        safe_timestamp(F.col("releaseDateStreaming")).alias("release_streaming_rt"),
        parse_currency(F.col("boxOffice")).alias("box_office_rt"),
        F.split(F.col("genre"), ",").getItem(0).alias("primary_genre_rt"),
        F.col("director").alias("rt_director"),
        F.col("writer").alias("rt_writer"),
    )
    return df


def load_imdb(spark: SparkSession, raw_base: str) -> DataFrame:
    base = clean_base(raw_base)
    df = read_with_fallback(
        spark,
        (
            f"{base}/imdb_kaggle",
            f"{clean_base(DEFAULT_RAW_URI)}/imdb_kaggle",
        ),
    )
    df = df.withColumn(
        "rt_id",
        F.regexp_extract(F.col("tomatoURL"), r"/m/([^/]+)/?", 1),
    )
    df = df.select(
        "rt_id",
        F.col("Title").alias("title_imdb"),
        safe_double(F.col("Year")).alias("year_imdb"),
        parse_runtime(F.col("Runtime")).alias("runtime_minutes_imdb"),
        parse_currency(F.col("BoxOffice")).alias("box_office_imdb"),
        safe_double(F.col("imdbRating")).alias("imdb_rating"),
        safe_double(F.regexp_replace("imdbVotes", ",", "")).alias("imdb_votes"),
        safe_double(F.col("Metascore")).alias("metascore"),
        "imdbID",
        F.col("Director").alias("imdb_director"),
        F.col("Writer").alias("imdb_writer"),
        "Actors",
        "Language",
        "Country",
        F.col("Genre").alias("genre_imdb"),
        safe_timestamp(F.col("Released")).alias("release_date_imdb"),
    )
    return df


def load_reviews(spark: SparkSession, raw_base: str) -> DataFrame:
    base = clean_base(raw_base)
    df = read_with_fallback(
        spark,
        (
            f"{base}/rt_reviews",
            f"{clean_base(DEFAULT_RAW_URI)}/rt_reviews",
        ),
    )
    df = df.select(
        F.col("id").alias("rt_id"),
        "reviewId",
        "criticName",
        F.col("publicatioName").alias("publicationName"),
        safe_boolean(F.col("isTopCritic")).alias("is_top_critic"),
        normalize_score(F.col("originalScore")).alias("score_ratio"),
        F.col("scoreSentiment"),
        F.when(F.lower(F.col("reviewState")) == "fresh", F.lit(1))
        .when(F.lower(F.col("reviewState")) == "rotten", F.lit(0))
        .otherwise(F.lit(None))
        .alias("sentiment"),
        "reviewText",
        safe_timestamp(F.col("creationDate")).alias("created_at"),
    )
    return df


def build_films(rt_df: DataFrame, imdb_df: DataFrame) -> DataFrame:
    films = rt_df.join(imdb_df, on="rt_id", how="outer")
    films = films.withColumn(
        "film_id", F.coalesce("imdbID", "rt_id")
    ).withColumn(
        "title",
        F.coalesce("title_imdb", "title_rt"),
    )
    films = films.withColumn(
        "runtime_minutes",
        F.coalesce("runtime_minutes_imdb", "runtime_minutes_rt"),
    ).withColumn(
        "box_office_usd",
        F.coalesce("box_office_imdb", "box_office_rt"),
    )
    films = films.withColumn(
        "year",
        F.coalesce("year_imdb", F.year("release_date_rt")),
    ).withColumn(
        "primary_genre",
        F.coalesce("primary_genre_rt", F.split("genre_imdb", ",").getItem(0)),
    )
    films = films.withColumn(
        "revenue_per_minute",
        F.col("box_office_usd") / F.col("runtime_minutes"),
    )

    return films.select(
        "film_id",
        "rt_id",
        F.col("imdbID").alias("imdb_id"),
        "title",
        "year",
        "primary_genre",
        "runtime_minutes",
        "audience_score",
        "tomato_meter",
        "imdb_rating",
        "imdb_votes",
        "metascore",
        "box_office_usd",
        "revenue_per_minute",
        "rt_director",
        "rt_writer",
        "imdb_director",
        "imdb_writer",
        "Actors",
        "Language",
        "Country",
        F.col("release_date_rt").alias("rt_release_date"),
        F.col("release_streaming_rt").alias("rt_streaming_date"),
        F.col("release_date_imdb"),
    ).drop_duplicates(["film_id"])


def build_people(films: DataFrame) -> DataFrame:
    def explode(column: str, role: str) -> DataFrame:
        return (
            films.select(
                "film_id",
                F.explode(
                    F.split(F.coalesce(F.col(column), F.lit("")), ",")
                ).alias("raw_name"),
            )
            .withColumn("name", F.trim("raw_name"))
            .where(F.col("name") != "")
            .select(
                "film_id",
                F.col("name"),
                F.lit(role).alias("role"),
            )
        )

    people_df = explode("rt_director", "director_rt").unionByName(
        explode("imdb_director", "director_imdb"), allowMissingColumns=True
    )
    people_df = people_df.unionByName(
        explode("rt_writer", "writer_rt"), allowMissingColumns=True
    )
    people_df = people_df.unionByName(
        explode("imdb_writer", "writer_imdb"), allowMissingColumns=True
    )
    people_df = people_df.unionByName(
        explode("Actors", "actor"), allowMissingColumns=True
    )
    people_df = people_df.withColumn(
        "person_id",
        F.regexp_replace(F.lower("name"), r"[^\w]+", "_"),
    )
    return people_df.drop_duplicates(["film_id", "name", "role"])


def write_table(df: DataFrame, uri: str, table: str) -> None:
    target = uri.rstrip("/") + f"/{table}"
    df.write.mode("overwrite").parquet(target)


def maybe_mirror_local(df: DataFrame, local_dir: str | None, table: str) -> None:
    if not local_dir:
        return
    table_dir = Path(local_dir) / table
    tmp_dir = table_dir.parent / f".tmp_{table}_{uuid.uuid4().hex}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp_dir))
    parts = list(tmp_dir.glob("part-*.parquet"))
    if not parts:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise FileNotFoundError(f"Aucun fichier part écrit pour {table} dans {tmp_dir}")
    if table_dir.exists():
        shutil.rmtree(table_dir)
    table_dir.mkdir(parents=True, exist_ok=True)
    final_path = table_dir / f"{table}.parquet"
    if final_path.exists():
        final_path.unlink()
    parts[0].rename(final_path)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def main() -> None:
    args = parse_args()
    spark = build_spark(args.spark_conf)

    rt_movies = load_rt_movies(spark, args.raw_base_uri)
    imdb = load_imdb(spark, args.raw_base_uri)
    reviews = load_reviews(spark, args.raw_base_uri)

    films = build_films(rt_movies, imdb)
    people = build_people(films)

    write_table(films, args.curated_base_uri, "films")
    write_table(reviews, args.curated_base_uri, "reviews")
    write_table(people, args.curated_base_uri, "people")

    maybe_mirror_local(films, args.local_mirror, "films")
    maybe_mirror_local(reviews, args.local_mirror, "reviews")
    maybe_mirror_local(people, args.local_mirror, "people")

    spark.stop()


if __name__ == "__main__":
    main()

