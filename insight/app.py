from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CURATED_DIR = PROJECT_ROOT / "data" / "curated"
USER_DATA_DIR = CURATED_DIR / "user_contributions"


def _load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_films() -> pd.DataFrame:
    base = pd.read_parquet(CURATED_DIR / "films" / "films.parquet")
    overrides = _load_parquet(USER_DATA_DIR / "film_overrides.parquet")
    if overrides.empty:
        return base
    base = base.set_index("film_id")
    overrides = overrides.set_index("film_id")
    base.update(overrides)
    return base.reset_index()


@st.cache_data(show_spinner=False)
def load_reviews() -> pd.DataFrame:
    base = pd.read_parquet(CURATED_DIR / "reviews" / "reviews.parquet")
    user_reviews = _load_parquet(USER_DATA_DIR / "user_reviews.parquet")
    if user_reviews.empty:
        return base
    return pd.concat([base, user_reviews], ignore_index=True)


@st.cache_data(show_spinner=False)
def load_people() -> pd.DataFrame:
    return pd.read_parquet(CURATED_DIR / "people" / "people.parquet")


def append_parquet(row: dict, path: Path, key: Optional[str] = None) -> None:
    df = _load_parquet(path)
    new_df = pd.DataFrame([row])
    if key and not df.empty:
        df = df[df[key] != row[key]]
    df = pd.concat([df, new_df], ignore_index=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    st.cache_data.clear()


def render_filters(films: pd.DataFrame) -> pd.DataFrame:
    years = films["year"].dropna().astype(int)
    min_year, max_year = years.min(), years.max()
    year_range = st.sidebar.slider(
        "Filtrer par année de sortie",
        min_value=int(min_year),
        max_value=int(max_year),
        value=(int(min_year), int(max_year)),
    )
    genres = sorted(films["primary_genre"].dropna().unique())
    selected_genres = st.sidebar.multiselect("Genres", genres)
    min_score, max_score = st.sidebar.slider(
        "Score IMDB min/max",
        min_value=0.0,
        max_value=10.0,
        value=(0.0, 10.0),
    )
    df = films.copy()
    df = df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]
    if selected_genres:
        df = df[df["primary_genre"].isin(selected_genres)]
    df = df[
        (df["imdb_rating"].fillna(0) >= min_score)
        & (df["imdb_rating"].fillna(0) <= max_score)
    ]
    return df


def render_exploration(films: pd.DataFrame, reviews: pd.DataFrame) -> None:
    st.subheader("Exploration interactive")
    st.write(
        f"{len(films):,} films et {len(reviews):,} critiques après filtres."
    )
    st.dataframe(
        films[
            [
                "title",
                "year",
                "primary_genre",
                "imdb_rating",
                "tomato_meter",
                "audience_score",
                "box_office_usd",
            ]
        ].sort_values("imdb_rating", ascending=False),
        width='stretch',
    )
    st.write("Échantillon des critiques associées (top 200)")
    st.dataframe(
        reviews[
            ["criticName", "publicationName", "sentiment", "created_at"]
        ]
        .sort_values("created_at", ascending=False)
        .head(200),
        width='stretch',
    )


def render_corr_report(films: pd.DataFrame) -> None:
    st.subheader("Corrélation IMDB vs Rotten Tomatoes")
    df = films.dropna(subset=["imdb_rating", "tomato_meter"])
    if df.empty:
        st.info("Aucune donnée disponible après filtres.")
        return
    df = df.assign(
        bubble_size=(
            pd.to_numeric(df["box_office_usd"], errors="coerce")
            .fillna(0)
            .clip(lower=0)
            + 1
        )
    )
    fig = px.scatter(
        df,
        x="imdb_rating",
        y="tomato_meter",
        size="bubble_size",
        size_max=60,
        hover_data=["title", "year", "primary_genre"],
        trendline="ols",
    )
    st.plotly_chart(fig, width='stretch')

    st.markdown("#### Impact des notes sur les revenus")
    revenue_df = films.dropna(
        subset=["box_office_usd", "imdb_rating", "tomato_meter"]
    ).copy()
    if revenue_df.empty:
        st.info("Aucune donnée de box-office disponible pour ce rapport.")
        return
    revenue_long = revenue_df.melt(
        id_vars=["title", "box_office_usd", "imdb_votes", "primary_genre", "year"],
        value_vars=["imdb_rating", "tomato_meter"],
        var_name="metric",
        value_name="score",
    )
    revenue_long["metric"] = revenue_long["metric"].map(
        {"imdb_rating": "IMDB", "tomato_meter": "TomatoMeter"}
    )
    revenue_long.loc[revenue_long["metric"] == "IMDB", "score"] = (
        revenue_long.loc[revenue_long["metric"] == "IMDB", "score"] * 10
    )
    fig2 = px.scatter(
        revenue_long,
        x="score",
        y="box_office_usd",
        color="metric",
        symbol="metric",
        hover_data=["title", "year", "primary_genre"],
        trendline="ols",
        labels={"score": "Score (0-100)", "box_office_usd": "Box office ($)"},
    )
    st.plotly_chart(fig2, width='stretch')

    st.markdown("#### Matrice de corrélations (toutes variables numériques)")
    numeric_df = films.copy()
    for column in numeric_df.columns:
        if not pd.api.types.is_numeric_dtype(numeric_df[column]):
            numeric_df[column] = pd.to_numeric(
                numeric_df[column], errors="coerce"
            )
    numeric_df = numeric_df.dropna(axis=1, how="all")
    valid_cols = [
        col for col in numeric_df.columns if numeric_df[col].count() >= 2
    ]
    numeric_df = numeric_df[valid_cols]
    if numeric_df.empty:
        st.info("Aucune colonne numérique exploitable pour la corrélation.")
        return
    corr_matrix = numeric_df.corr().round(2)
    if corr_matrix.isna().all().all():
        st.info("Impossible de calculer la corrélation sur les mesures actuelles.")
        return
    heatmap = px.imshow(
        corr_matrix,
        text_auto=True,
        color_continuous_scale=[
            (0.0, "#8B0000"),
            (0.25, "#F08080"),
            (0.5, "#F5F5F5"),
            (0.75, "#87CEEB"),
            (1.0, "#00008B"),
        ],
        zmin=-1,
        zmax=1,
    )
    heatmap.update_layout(margin=dict(l=40, r=40, t=40, b=40))
    st.plotly_chart(heatmap, width='stretch')

    def highlight_extreme(val: float) -> str:
        if pd.isna(val):
            return ""
        if val >= 0.5:
            return "background-color: #c6efce; font-weight: bold;"
        if val <= -0.5:
            return "background-color: #ffc7ce; font-weight: bold;"
        return ""

    styled_matrix = corr_matrix.style.format("{:.2f}").applymap(highlight_extreme)
    st.dataframe(styled_matrix, width='stretch')

    strong_links = (
        corr_matrix.stack()
        .reset_index()
        .rename(columns={"level_0": "metric_a", "level_1": "metric_b", 0: "value"})
    )
    strong_links = strong_links[
        (strong_links["metric_a"] < strong_links["metric_b"])
        & (strong_links["value"].abs() >= 0.5)
    ].sort_values("value", key=lambda s: s.abs(), ascending=False)
    if not strong_links.empty:
        st.markdown("**Corrélations fortes (|ρ| ≥ 0.5):**")
        for _, row in strong_links.iterrows():
            color = "✅" if row["value"] > 0 else "⚠️"
            st.write(
                f"{color} {row['metric_a']} / {row['metric_b']} : "
                f"{row['value']:.2f}"
            )


def render_temporal_report(films: pd.DataFrame) -> None:
    st.subheader("Analyse temporelle (sorties vs box office)")
    df = films.dropna(subset=["year"])
    summary = (
        df.groupby("year")
        .agg(
            total_box=("box_office_usd", "sum"),
            count=("film_id", "count"),
            avg_imdb=("imdb_rating", "mean"),
        )
        .reset_index()
    )
    fig = px.bar(
        summary,
        x="year",
        y="total_box",
        hover_data=["count", "avg_imdb"],
        labels={"total_box": "Box Office cumulé ($)"},
    )
    st.plotly_chart(fig, width='stretch')
    fig2 = px.line(
        summary,
        x="year",
        y="avg_imdb",
        markers=True,
        labels={"avg_imdb": "IMDB moyen"},
    )
    st.plotly_chart(fig2, width='stretch')


def render_genre_report(films: pd.DataFrame) -> None:
    st.subheader("Distribution genres vs durées")
    df = films.dropna(subset=["primary_genre", "runtime_minutes"])
    fig = px.box(
        df,
        x="primary_genre",
        y="runtime_minutes",
        color="primary_genre",
    )
    st.plotly_chart(fig, width='stretch')
    
    st.markdown("---")
    st.markdown("#### Genres les plus rentables")
    
    # Calcul de revenue_per_minute si absent
    films_analysis = films.copy()
    if "revenue_per_minute" not in films_analysis.columns:
        films_analysis["revenue_per_minute"] = (
            films_analysis["box_office_usd"] / films_analysis["runtime_minutes"]
        )
    
    # Calcul de la rentabilité par genre
    genre_profitability = (
        films_analysis.dropna(subset=["primary_genre", "box_office_usd"])
        .groupby("primary_genre")
        .agg(
            total_box_office=("box_office_usd", "sum"),
            avg_box_office=("box_office_usd", "mean"),
            count_films=("film_id", "count"),
            avg_revenue_per_minute=("revenue_per_minute", "mean"),
        )
        .reset_index()
        .sort_values("total_box_office", ascending=False)
    )
    
    # Affichage des genres les plus rentables
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Top 10 genres par recettes totales**")
        top_total = genre_profitability.head(10)
        fig1 = px.bar(
            top_total,
            x="total_box_office",
            y="primary_genre",
            orientation="h",
            hover_data=["count_films", "avg_box_office"],
            labels={
                "total_box_office": "Recettes totales ($)",
                "primary_genre": "Genre",
                "count_films": "Nombre de films",
                "avg_box_office": "Recettes moyennes ($)",
            },
        )
        fig1.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        st.markdown("**Top 10 genres par recettes moyennes**")
        top_avg = (
            genre_profitability[genre_profitability["count_films"] >= 5]
            .sort_values("avg_box_office", ascending=False)
            .head(10)
        )
        if not top_avg.empty:
            fig2 = px.bar(
                top_avg,
                x="avg_box_office",
                y="primary_genre",
                orientation="h",
                hover_data=["count_films", "total_box_office"],
                labels={
                    "avg_box_office": "Recettes moyennes ($)",
                    "primary_genre": "Genre",
                    "count_films": "Nombre de films",
                    "total_box_office": "Recettes totales ($)",
                },
            )
            fig2.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Pas assez de données pour calculer les moyennes fiables.")
    
    # Tableau récapitulatif
    st.markdown("**Tableau récapitulatif par genre**")
    genre_profitability_display = genre_profitability.copy()
    genre_profitability_display["total_box_office"] = (
        genre_profitability_display["total_box_office"] / 1_000_000
    ).round(2)
    genre_profitability_display["avg_box_office"] = (
        genre_profitability_display["avg_box_office"] / 1_000_000
    ).round(2)
    genre_profitability_display["avg_revenue_per_minute"] = (
        genre_profitability_display["avg_revenue_per_minute"] / 1_000_000
    ).round(2)
    genre_profitability_display = genre_profitability_display.rename(
        columns={
            "total_box_office": "Recettes totales (M$)",
            "avg_box_office": "Recettes moyennes (M$)",
            "count_films": "Nombre de films",
            "avg_revenue_per_minute": "Revenus/min (M$)",
        }
    )
    st.dataframe(
        genre_profitability_display.style.format(
            {
                "Recettes totales (M$)": "{:.2f}",
                "Recettes moyennes (M$)": "{:.2f}",
                "Revenus/min (M$)": "{:.2f}",
            }
        ),
        use_container_width=True,
        height=400,
    )


def render_rentability_report(films: pd.DataFrame, people: pd.DataFrame) -> None:
    st.subheader("Rentabilité par acteurs / réalisateurs / budget proxy")
    
    # Évolution de la rentabilité moyenne au fil des années
    st.markdown("#### Évolution de la rentabilité moyenne au fil des années")
    films_yearly = films.dropna(subset=["year", "box_office_usd"])
    
    # Calcul de revenue_per_minute si absent
    if "revenue_per_minute" not in films_yearly.columns:
        films_yearly["revenue_per_minute"] = (
            films_yearly["box_office_usd"] / films_yearly["runtime_minutes"]
        )
    
    yearly_profitability = (
        films_yearly.groupby("year")
        .agg(
            avg_box_office=("box_office_usd", "mean"),
            avg_revenue_per_minute=("revenue_per_minute", "mean"),
            count_films=("film_id", "count"),
            median_box_office=("box_office_usd", "median"),
        )
        .reset_index()
        .sort_values("year")
    )
    
    # Filtrer les années avec au moins 5 films pour une meilleure représentativité
    yearly_profitability_filtered = yearly_profitability[
        yearly_profitability["count_films"] >= 5
    ]
    
    st.markdown("**Recettes moyennes par année**")
    fig_yearly_avg = px.line(
        yearly_profitability_filtered,
        x="year",
        y="avg_box_office",
        markers=True,
        hover_data=["count_films", "median_box_office"],
        labels={
            "year": "Année",
            "avg_box_office": "Recettes moyennes ($)",
            "count_films": "Nombre de films",
            "median_box_office": "Recettes médianes ($)",
        },
    )
    fig_yearly_avg.update_traces(line_color="#1f77b4", line_width=3)
    fig_yearly_avg.update_layout(
        xaxis_title="Année",
        yaxis_title="Recettes moyennes ($)",
        hovermode="x unified",
    )
    st.plotly_chart(fig_yearly_avg, width='stretch')
    
    # Statistiques récapitulatives
    st.markdown("**Tendances observées**")
    if not yearly_profitability_filtered.empty:
        first_period = yearly_profitability_filtered.head(
            len(yearly_profitability_filtered) // 3
        )
        last_period = yearly_profitability_filtered.tail(
            len(yearly_profitability_filtered) // 3
        )
        
        first_avg = first_period["avg_box_office"].mean()
        last_avg = last_period["avg_box_office"].mean()
        evolution_pct = ((last_avg - first_avg) / first_avg * 100) if first_avg > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                "Période ancienne (moyenne)",
                f"${first_avg/1_000_000:.2f}M",
                delta=f"{len(first_period)} ans",
            )
        with col2:
            st.metric(
                "Période récente (moyenne)",
                f"${last_avg/1_000_000:.2f}M",
                delta=f"{len(last_period)} ans",
            )
        with col3:
            st.metric(
                "Évolution",
                f"{evolution_pct:.1f}%",
                delta="hausse" if evolution_pct > 0 else "baisse",
                delta_color="normal" if evolution_pct > 0 else "inverse",
            )
    
    st.markdown("---")
    st.markdown("#### Rentabilité par talents")
    
    base = films[["film_id", "box_office_usd", "runtime_minutes"]].copy()
    base["revenue_per_minute"] = base["box_office_usd"] / base[
        "runtime_minutes"
    ]
    merged = people.merge(base, on="film_id", how="left")
    role_labels = {
        "actor": "Acteurs",
        "director_rt": "Réalisateurs (RT)",
        "director_imdb": "Réalisateurs (IMDB)",
        "writer_rt": "Scénaristes (RT)",
        "writer_imdb": "Scénaristes (IMDB)",
    }
    role = st.selectbox(
        "Choisir le rôle",
        options=list(role_labels.keys()),
        format_func=lambda x: role_labels.get(x, x),
    )
    df = (
        merged[merged["role"] == role]
        .groupby("name")
        .agg(
            films=("film_id", "nunique"),
            total_box=("box_office_usd", "sum"),
            avg_roi=("revenue_per_minute", "mean"),
        )
        .reset_index()
        .sort_values("total_box", ascending=False)
        .head(20)
    )
    fig = px.bar(
        df,
        x="name",
        y="total_box",
        hover_data=["films", "avg_roi"],
        labels={"total_box": "Recettes cumulées ($)"},
    )
    st.plotly_chart(fig, width='stretch')
    st.dataframe(df, width='stretch')


def handle_new_review_form(films: pd.DataFrame) -> None:
    st.markdown("### Ajouter une critique manuelle")
    film_lookup = films.set_index("film_id")

    def format_film(fid: str) -> str:
        title = film_lookup.loc[fid]["title"]
        title_str = str(title) if pd.notna(title) else "Titre inconnu"
        return f"{title_str} ({fid})"

    with st.form("new_review"):
        film = st.selectbox(
            "Film",
            options=films["film_id"].tolist(),
            format_func=format_film,
        )
        critic = st.text_input("Critique")
        publication = st.text_input("Publication")
        score = st.number_input("Score (0-1)", min_value=0.0, max_value=1.0, step=0.1)
        sentiment = st.selectbox("Sentiment", ["fresh", "rotten"])
        text = st.text_area("Texte")
        submitted = st.form_submit_button("Enregistrer")
        if submitted:
            row = {
                "film_id": film,
                "rt_id": film,
                "reviewId": f"user_{pd.Timestamp.utcnow().value}",
                "criticName": critic,
                "publicationName": publication,
                "is_top_critic": False,
                "score_ratio": score,
                "scoreSentiment": sentiment.upper(),
                "sentiment": 1 if sentiment == "fresh" else 0,
                "reviewText": text,
                "created_at": pd.Timestamp.utcnow(),
            }
            append_parquet(row, USER_DATA_DIR / "user_reviews.parquet")
            st.success("Critique ajoutée.")


def handle_edit_film_form(films: pd.DataFrame) -> None:
    st.markdown("### Éditer les métriques d'un film")
    film_lookup = films.set_index("film_id")

    def format_film(fid: str) -> str:
        title = film_lookup.loc[fid]["title"]
        title_str = str(title) if pd.notna(title) else "Titre inconnu"
        return f"{title_str} ({fid})"

    with st.form("edit_film"):
        film = st.selectbox(
            "Film à éditer",
            options=films["film_id"].tolist(),
            format_func=format_film,
        )
        current_box = film_lookup.loc[film]["box_office_usd"]
        current_box = 0.0 if pd.isna(current_box) else float(current_box)
        current_audience = film_lookup.loc[film]["audience_score"]
        current_audience = 0.0 if pd.isna(current_audience) else float(current_audience)
        new_box = st.number_input(
            "Box office ($)",
            min_value=0.0,
            value=current_box,
        )
        new_audience = st.slider(
            "Audience score",
            min_value=0.0,
            max_value=100.0,
            value=current_audience,
        )
        submitted = st.form_submit_button("Appliquer")
        if submitted:
            row = {
                "film_id": film,
                "box_office_usd": new_box,
                "audience_score": new_audience,
                "updated_at": pd.Timestamp.utcnow(),
            }
            append_parquet(row, USER_DATA_DIR / "film_overrides.parquet", key="film_id")
            st.success("Mise à jour enregistrée.")


def render_admin(films: pd.DataFrame) -> None:
    st.subheader("Administration des données")
    handle_new_review_form(films)
    handle_edit_film_form(films)


def main() -> None:
    st.set_page_config(page_title="Ciné DataLake", layout="wide")
    st.title("Ciné DataLake – Insight Layer")
    films = load_films()
    reviews = load_reviews()
    people = load_people()

    filtered_films = render_filters(films)
    filtered_reviews = reviews[reviews["rt_id"].isin(filtered_films["rt_id"])]

    page = st.sidebar.selectbox(
        "Choisir une page",
        [
            "Exploration",
            "Rapport: Corrélation",
            "Rapport: Chronologie",
            "Rapport: Genres",
            "Rapport: Rentabilité",
            "Administration",
        ],
    )

    if page == "Exploration":
        render_exploration(filtered_films, filtered_reviews)
    elif page == "Rapport: Corrélation":
        render_corr_report(filtered_films)
    elif page == "Rapport: Chronologie":
        render_temporal_report(filtered_films)
    elif page == "Rapport: Genres":
        render_genre_report(filtered_films)
    elif page == "Rapport: Rentabilité":
        render_rentability_report(filtered_films, people)
    else:
        render_admin(films)


if __name__ == "__main__":
    main()

