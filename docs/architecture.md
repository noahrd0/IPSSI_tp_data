# Architecture DataLake Ciné

## 1. Sources de données

| Source | Type | Format | Champs clés |
| --- | --- | --- | --- |
| Rotten Tomatoes Reviews | Historique CSV local | `rotten_tomatoes_movie_reviews.csv` | `reviewId`, `criticName`, `originalScore`, `reviewState`, `reviewText`, `scoreSentiment` |
| Rotten Tomatoes Films | Historique CSV local | `rotten_tomatoes_movies.csv` | `title`, `audienceScore`, `tomatoMeter`, `runtimeMinutes`, `genre`, `director`, `boxOffice` |
| Kaggle IMDB Dataset | Pull batch via `kagglehub` | `IMDB Dataset.csv` | `Title`, `Year`, `Runtime`, `Genre`, `Actors`, `Ratings.*`, `BoxOffice`, `tomatoURL` |

Chaque ingestion conserve un dump brut daté sous `data/raw/<source>/YYYYMMDD/` avant toute transformation pour garantir la traçabilité.

## 2. Modèle logique

- **Fact_Film** : identifiant natural (`imdbID` ou `id` RT), dimensions (`title`, `year`, `genre`, `runtime`, `language`, `country`).
- **Fact_CriticReview** : clé étrangère vers film, attributs critiques (`criticName`, `publicationName`, `score`, `sentiment`).
- **Dim_People** : acteurs, réalisateurs (dédoublonnage + rôles).
- **Fact_Performance** : métriques de sortie (`boxOffice`, `audienceScore`, `tomatoMeter`, `imdbRating`, ROI calculé via budget si disponible ou proxy revenus/runtime).
- **Metadata_Ingestion** : provenance, datestamp, hash des fichiers, statut job.

## 3. Architecture logique

```mermaid
graph LR
  subgraph Ingestion
    CSV1[CSV Loader RT Reviews]
    CSV2[CSV Loader RT Movies]
    KAG[Kaggle Loader IMDB]
  end

  subgraph Raw Storage
    RAW_L[data/raw/<source>/YYYYMMDD]
    HDFS[/HDFS /datalake/raw/]
    META[metadata/ingestions.parquet]
  end

  subgraph ETL/Persistance
    SPARK[PySpark Jobs]
    HCUR[/HDFS /datalake/curated/]
    DUCK[DuckDB + Parquet (miroir local)]
  end

  subgraph Insight
    ST[Streamlit Dashboard]
  end

  CSV1 --> RAW_L
  CSV2 --> RAW_L
  KAG --> RAW_L
  RAW_L --> HDFS
  HDFS --> SPARK --> HCUR --> DUCK --> ST
  META --> ST
```

## 4. Technologies & responsabilités

| Couche | Technologies | Rôle |
| --- | --- | --- |
| Ingestion | Python 3, `pandas`, `kagglehub`, orchestrateur custom | Batch tolérant aux pannes, copie brute, logs |
| Persistance | Hadoop HDFS, PySpark, DuckDB, Parquet | Normalisation distribuée, stockage long terme, miroir analytique |
| Insight | Streamlit, Plotly, DuckDB SQL | Filtres, édition, injections, rapports interactifs |

## 5. Résilience & reprise

- Reprise sur incident assurée par marqueurs de complétion (`_SUCCESS`) dans chaque dossier raw.
- Scripts idempotents : le loader saute un dump si le hash du fichier existe déjà en métadonnées.
- Les transformations relisent toujours la dernière version stable (hash validé) et écrivent en mode "swap" (`data/curated/tmp` puis `data/curated/<table>`).

## 6. Diagrammes & documentation

Le fichier présent sert de référence pour la soutenance ; une exportation image du diagramme Mermaid sera incluse dans les slides (génération via `mmdc` ou export Streamlit) pour le board.
