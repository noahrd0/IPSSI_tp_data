# Rapports Insight

## 1. Corrélation IMDB vs Rotten Tomatoes
- Scatter pondéré par `box_office_usd`, trendline (OLS).
- Permet de justifier la cohérence entre les deux écosystèmes critiques.
- Indicateurs : coefficient `R²`, outliers à mettre en avant pendant la soutenance.

## 2. Chronologie sorties vs box office
- Barres par année (`total_box`) + ligne `avg_imdb`.
- Narratif : repérer les creux (ex. pandémie) ou pics (blockbusters années 2000).

## 3. Genres vs durées
- Box plot `runtime_minutes` par `primary_genre`.
- À utiliser pour expliquer la stratégie de programmation (ex. documentaires courts vs drames longs).

## 4. Rentabilité par talents
- Agrégation `people` × `films` sur `box_office_usd` et `revenue_per_minute`.
- Inputs : rôles (acteurs, réalisateurs RT/IMDB, scénaristes) sélectionnables dans le dashboard.
- Permet d’isoler les talents les plus générateurs de revenus, même sans budget explicite (proxy ROI via minutes).

## 5. Formulaires d’injection/édition
- Ajout de critiques (stockées dans `user_contributions/user_reviews.parquet`).
- Override de `box_office_usd` & `audience_score` par film (idempotent, traçable via `updated_at`).
- À démontrer lors de l’oral pour répondre au critère "injection / édition".
