# Runbook – Ciné DataLake

## Pré-requis
- Python 3.12+
- Java 11+ (pour Spark)
- Hadoop pseudo-distribué (namenode/datanode démarrés localement)
- Spark 3+/4+ (le binaire `spark-submit` doit être accessible)
- `python3 -m venv .venv && source .venv/bin/activate`
- `pip install -r requirements.txt`

### Initialisation HDFS
```bash
hdfs namenode -format
start-dfs.sh   # ou start-all.sh selon la distribution

hdfs dfs -mkdir -p /datalake/raw
hdfs dfs -mkdir -p /datalake/curated
hdfs dfs -chmod -R 775 /datalake
```

Puis synchroniser les dumps bruts (après `make ingest`) :
```bash
hdfs dfs -put -f data/raw/* /datalake/raw
```

## Pipeline batch
```bash
# 1. Ingestion (copie brute + métadonnées)
make ingest

# 2a. Transformation locale (pandas / fallback)
make etl

# 2b. Transformation distribuée (Spark + HDFS)
HDFS_RAW=hdfs://localhost:9000/datalake/raw \
HDFS_CURATED=hdfs://localhost:9000/datalake/curated \
SPARK_MASTER=yarn \
make spark-etl
# Ou pour tout enchaîner (démarrage HDFS → ingestion → safemode wait → push HDFS → ETL → dashboard) :
make spark-pipeline
# ou spark-submit manuel :
# spark-submit --master yarn etl/spark_transform.py \
#    --raw-base-uri hdfs://localhost:9000/datalake/raw \
#    --curated-base-uri hdfs://localhost:9000/datalake/curated \
#    --local-mirror /home/.../data/curated
```

- Les dumps sont stockés dans `data/raw/<source>/<timestamp>/` avec `*_SUCCESS.json`.
- Les métadonnées d’ingestion sont historisées dans `metadata/ingestions.parquet` (hash, taille, nombre de lignes).
- La version Spark écrit les tables en Parquet dans HDFS (`/datalake/curated/{films,reviews,people}`) **et** les réplique localement (`data/curated/...`) pour alimenter DuckDB/Streamlit.
- `etl/transform.py` reste disponible comme mode dégradé local lorsque Spark/Hadoop ne sont pas démarrés.

## Dashboard Streamlit
```bash
make dashboard
```
Fonctionnalités :
- **Filtres** (année, genre, score IMDB) appliqués à toutes les vues.
- **Exploration** : table films + aperçu critiques.
- **Rapports** :
  1. Corrélation `imdb_rating` vs `tomato_meter` (scatter + trendline).
  2. Chronologie sorties vs box office (barres + ligne IMDB moyenne).
  3. Distribution genres/durées (box plot).
  4. Rentabilité par acteurs/réalisateurs (Top 20, recettes cumulées + ROI proxy).
- **Administration** : formulaire d’injection de critiques + édition box-office/audience. Les contributions sont stockées dans `data/curated/user_contributions/` et fusionnées à chaud.

## Reprise & résilience
- Les scripts d’ingestion vérifient le hash SHA-256 avant copie : rerunner `make ingest` n duplique pas les dumps.
- En cas d’échec, un enregistrement `status=failed` est ajouté à `ingestions.parquet` avec l’erreur pour audit.
- Côté Spark : les lectures se font via des motifs `raw/<source>/*/*.csv` afin d’être idempotentes ; l’écriture Parquet utilise `mode=overwrite` par table, mais il est recommandé de versionner les dossiers (`/curated/<table>/run_id=`) pour la prod.
- Pensez à arrêter proprement HDFS (`stop-dfs.sh`) et à purger les répertoires `/tmp` si un job Spark échoue.

## Présentation & démo
1. **Architecture** : s’appuyer sur `docs/architecture.md` (Mermaid) + capture du dashboard.
2. **Démonstration live** :
   - `make ingest && make etl`
   - `make dashboard`
   - Ajouter une critique manuelle puis modifier le box-office d’un film pour illustrer la couche Insight.
3. **Rapports** : préparer 2-3 études approfondies (ex. corrélation positive ~0.7, glissement des recettes par décennie, réalisateurs les plus rentables).
4. **Rôles** : chaque binôme décrit son périmètre (ingestion, ETL, insight, doc) et les enseignements.

## Dépannage
- **`kagglehub` auth** : utiliser `kagglehub login` si nécessaire avant `make ingest`.
- **Mémoire pandas** : définir `ETL_LOG_LEVEL=DEBUG` pour suivre la progression. Pour tester sur un sous-échantillon local, passer `N_ROWS` via variable d’environnement et adapter `etl/transform.py`.
- **Spark/HDFS** :
  - vérifier `hdfs dfs -ls /datalake/raw/rt_movies` pour confirmer la présence des fichiers,
  - utiliser `--spark-conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000` si Spark ne détecte pas le namenode,
  - exporter `JAVA_HOME` avant `spark-submit` si nécessaire (`export JAVA_HOME=/usr/lib/jvm/...`),
  - consulter les logs Yarn (`yarn logs -applicationId ...`) en cas d’échec distribué.
- **Streamlit** : si les caches deviennent incohérents, supprimer `data/curated/user_contributions/*` et relancer l’app.
