# Audit des sources

## Rotten Tomatoes – `rotten_tomatoes_movie_reviews.csv`
- ~1,446,425 lignes, 11 colonnes (ID film + métadonnées critiques).
- Champs notables : `reviewId`, `criticName`, `isTopCritic`, `originalScore`, `reviewText`, `scoreSentiment`.
- Qualité :
  - `originalScore` souvent vide ou dans plusieurs formats (`3.5/4`, `B+`, `7/10`).
  - `publicatioName` contient des variantes (doit être normalisé).
  - `reviewText` avec caractères HTML encodés (`&#44;`, `&#8217;`).

## Rotten Tomatoes – `rotten_tomatoes_movies.csv`
- ~143,259 lignes, 16 colonnes autour des films.
- Champs notables : `audienceScore`, `tomatoMeter`, `runtimeMinutes`, `genre`, `boxOffice`.
- Qualité :
  - `rating`, `boxOffice`, `distributor` très clairsemés.
  - `genre` multi-valeurs séparées par virgules.
  - `releaseDateStreaming` parfois vide (prévoir parsing robuste).

## Kaggle IMDB Dataset – `IMDB Dataset.csv`
- 5,383 lignes, 27 colonnes (films + séries). Source téléchargée via `kagglehub`.
- Champs notables : `Ratings.Source`, `Ratings.Value`, `imdbRating`, `imdbVotes`, `tomatoURL` (permet jointure sur Rotten Tomatoes).
- Qualité :
  - `Awards`, `BoxOffice`, `Production` textuels (normalisation requise).
  - `Ratings.*` pivoté : nécessite flatten pour aligner sur colonnes `imdbRating`, `metascore`, `tomatoMeter`.

## Alignement & modèle
- Clé primaire préférée : `imdbID` (IMDB) + mapping vers `tomatoURL` ou slug `id` RT.
- Normalisation prévue : tables `films`, `reviews`, `people`, `box_office_metrics`, `ingestions`.
- Résilience : chaque dump brut versionné + hash dans `metadata/ingestions.parquet`.

## Diagramme
- Voir `docs/architecture.md` (section Mermaid) pour la représentation des trois couches.
