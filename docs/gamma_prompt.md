# Prompt pour Gamma - Présentation DataLake Ciné

Crée une présentation PowerPoint professionnelle pour un board d'entreprise sur notre projet DataLake Big Data. Voici les spécifications :

## Contexte du projet

**Sujet** : Architecture DataLake pour l'analyse de données cinématographiques

**Objectif** : Concevoir une solution complète d'ingestion, persistance et traitement de données de films avec un dashboard interactif et des rapports analytiques significatifs.

**Sources de données** (3 sources de types différents) :
1. **Rotten Tomatoes Reviews** : ~1,4M critiques de films (CSV historique local)
2. **Rotten Tomatoes Movies** : ~143K films avec scores, box office, métadonnées (CSV historique local)
3. **Kaggle IMDB Dataset** : ~5,4K films avec ratings IMDB, Metascore, box office (API Kaggle via kagglehub)

## Architecture en 3 couches

### 1. Couche Ingestion (Batch résiliente)
- **Technologies** : Python 3, pandas, kagglehub
- **Fonctionnalités** :
  - Ingestion batch tolérante aux interruptions
  - Conservation d'une copie brute (raw) datée dans `data/raw/<source>/YYYYMMDD/`
  - Métadonnées d'ingestion (hash SHA256, timestamp, statut) stockées séparément
  - Système idempotent : saut automatique si hash déjà présent
  - Traçabilité complète via `metadata/ingestions.parquet`

### 2. Couche Persistance (ETL distribué)
- **Technologies** : Hadoop HDFS (pseudo-distribué), PySpark, DuckDB, Parquet
- **Fonctionnalités** :
  - ETL distribué avec PySpark pour traitement à grande échelle
  - Stockage distribué sur HDFS (`/datalake/raw` et `/datalake/curated`)
  - Normalisation des données : fusion RT + IMDB, nettoyage, déduplication
  - Tables curated : `films`, `reviews`, `people`
  - Miroir local Parquet pour accès rapide du dashboard
  - Séparation stricte données/métadonnées
  - Indexation automatique via DuckDB

### 3. Couche Insight (Dashboard interactif)
- **Technologies** : Streamlit, Plotly, DuckDB SQL
- **Fonctionnalités** :
  - Dashboard interactif avec filtres multi-critères (année, genre, réalisateur, acteur)
  - Inspection des données avec visualisations dynamiques
  - **Injection de nouvelles données** : formulaire pour ajouter des critiques manuelles
  - **Édition de données existantes** : modification de box office, scores par film
  - 5 rapports analytiques significatifs (voir section suivante)

## Rapports analytiques produits

### 1. Corrélation IMDB vs Rotten Tomatoes
- Scatter plot pondéré par box office avec trendline OLS
- Normalisation des scores (IMDB 0-10 → 0-100, Tomato 0-100)
- Analyse de corrélation entre les deux écosystèmes critiques
- Impact des notes sur les revenus générés

### 2. Analyse temporelle : Sorties vs Box Office
- Évolution du box office total par année
- Superposition avec moyenne des scores IMDB
- Identification des tendances (pics blockbusters, creux pandémie)

### 3. Comparatif Genres vs Durées
- Box plot des durées de films par genre principal
- Analyse des stratégies de programmation par genre

### 4. Étude de rentabilité par talents
- Agrégation par acteurs, réalisateurs, scénaristes
- Métriques : box office total, revenus par minute
- Identification des talents les plus générateurs de revenus
- Proxy ROI via `revenue_per_minute` (budget non disponible)

### 5. Matrice de corrélation complète
- Toutes les variables numériques du dataset
- Mise en évidence des corrélations fortes (>0.5 et <-0.5)
- Insights sur les relations entre métriques

## Technologies et responsabilités par couche

| Couche | Technologies | Rôle |
|--------|-------------|------|
| **Ingestion** | Python, pandas, kagglehub | Batch résilient, copie brute, logs métadonnées |
| **Persistance** | Hadoop HDFS, PySpark, DuckDB, Parquet | Normalisation distribuée, stockage long terme, miroir analytique |
| **Insight** | Streamlit, Plotly, DuckDB SQL | Filtres, édition, injections, rapports interactifs |

## Points techniques à mettre en avant

1. **Résilience** :
   - Marqueurs de complétion (`_SUCCESS`) dans chaque dossier raw
   - Scripts idempotents basés sur hash SHA256
   - Reprise sur incident garantie

2. **Architecture distribuée** :
   - Cluster Hadoop HDFS pseudo-distribué
   - Traitement PySpark distribué
   - Pipeline automatisé via Makefile (`make spark-pipeline`)

3. **Qualité des données** :
   - Normalisation robuste des formats hétérogènes (scores, devises, durées)
   - Gestion des valeurs manquantes et formats inconsistants
   - Dédoublonnage intelligent (fusion RT + IMDB via `tomatoURL`)

4. **Traçabilité** :
   - Métadonnées d'ingestion séparées
   - Versioning des dumps bruts
   - Historique des modifications (édition via dashboard)

## Structure de la présentation demandée (10 slides maximum)

### Slide 1 : Titre et contexte
- Titre : "DataLake Ciné : Architecture Big Data pour l'Analyse de Données Cinématographiques"
- Sous-titre : Ingestion, Persistance et Insights avec Hadoop, Spark et Streamlit
- Équipe et date

### Slide 2 : Problématique et sources de données
- Besoin d'analyser des données cinématographiques multi-sources
- Objectif : architecture scalable et résiliente
- **3 sources hétérogènes** : 
  - Rotten Tomatoes Reviews (~1,4M critiques, CSV local)
  - Rotten Tomatoes Movies (~143K films, CSV local)
  - Kaggle IMDB Dataset (~5,4K films, API Kaggle)
- Types de données variés : critiques textuelles, scores numériques, métadonnées structurées

### Slide 3 : Architecture globale (diagramme)
- Schéma Mermaid montrant les 3 couches
- Flux : Ingestion → Raw Storage (local + HDFS) → ETL/Persistance (Spark) → Curated (HDFS + miroir local) → Insight (Streamlit)
- Technologies par couche clairement identifiées
- Points clés : résilience, traçabilité, scalabilité

### Slide 4 : Architecture technique (3 couches)
- **Ingestion** : Batch résilient, copie brute versionnée, métadonnées séparées (extrait code : `ingest_csv`)
- **Persistance** : PySpark distribué, HDFS, normalisation robuste (extrait code : `build_films` ou UDF `safe_double`)
- **Insight** : Dashboard Streamlit interactif, filtres, injection/édition de données

### Slide 5 : Rapports analytiques - Corrélation et Temporel
- **Corrélation IMDB vs Rotten Tomatoes** : Scatter plot pondéré par box office, normalisation 0-100, impact sur revenus
- **Analyse temporelle** : Évolution box office par année + moyenne scores IMDB, identification des tendances

### Slide 6 : Rapports analytiques - Genres et Rentabilité
- **Genres vs Durées** : Box plot par genre, stratégies de programmation
- **Rentabilité par talents** : Top acteurs/réalisateurs par box office et revenus/minute, proxy ROI

### Slide 7 : Matrice de corrélation
- Heatmap de toutes les variables numériques
- Mise en évidence des corrélations fortes (>0.5 et <-0.5)
- Insights sur les relations entre métriques (scores, box office, durées, etc.)

### Slide 8 : Résultats et insights business
- Quels genres/acteurs/réalisateurs génèrent le plus de revenus
- Corrélation entre scores critiques et box office
- Tendances temporelles identifiées (pics blockbusters, creux pandémie)
- Comparaison avec études existantes (si applicable)

### Slide 9 : Démonstration technique
- Pipeline automatisé : `make spark-pipeline` (HDFS + Spark ETL)
- Gestion d'edge cases : valeurs manquantes, formats inconsistants, déduplication
- Fonctionnalités dashboard : injection de critiques, édition de données

### Slide 10 : Conclusion et perspectives
- Architecture scalable et résiliente (Hadoop/Spark)
- Insights business exploitables pour l'industrie cinématographique
- Améliorations possibles : ML pour prédiction de box office, streaming temps réel, intégration API supplémentaires

## Style et ton

- **Ton** : Professionnel, adapté à un board d'entreprise
- **Style visuel** : Moderne, épuré, avec schémas clairs
- **Équilibre** : 60% technique (architecture, code), 40% business (insights, résultats)
- **Densité** : 10 slides maximum - chaque slide doit être informatif mais lisible, éviter la surcharge
- **Extraits de code** : Snippets stratégiques (max 5-10 lignes), pas de code complet
- **Graphiques** : Utiliser les visualisations Plotly du dashboard (scatter, barres, box plots, heatmap)

## Notes importantes

- Chaque slide doit être autonome mais s'inscrire dans une narration cohérente
- Mettre en avant la robustesse technique (résilience, traçabilité)
- Justifier les choix technologiques (Hadoop/Spark pour scalabilité, Streamlit pour rapidité de développement)
- Les rapports doivent répondre à des questions business concrètes
- Préparer des réponses aux questions sur la scalabilité, la maintenance, les coûts

## Implication individuelle (à adapter selon l'équipe)

- Chaque membre doit pouvoir expliquer sa partie (ingestion, ETL, dashboard, rapports)
- Justifier les choix techniques et les apprentissages
- Montrer la collaboration (merges Git, répartition des tâches)

---

**Commande pour générer la présentation** : Utilise ce prompt dans Gamma pour créer une présentation de **10 slides maximum**, professionnelle et visuellement attrayante, prête pour une soutenance devant un board d'entreprise. Chaque slide doit être dense en information mais clair et lisible.

