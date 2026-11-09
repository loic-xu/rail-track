"""RailTracker — The Citizen Transit Punctuality Dashboard for Paris Commuter Rail.

Inspired by CovidTracker's visual clarity and citizen data-journalism principles.
Powered by DuckDB, dbt, and Streamlit.
"""

import time
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# 1. Page Configuration
st.set_page_config(
    page_title="RailTracker — Ponctualité RER Île-de-France",
    page_icon="🚆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 2. Custom CSS (Dark-Mode Native & CovidTracker Minimalist Style)
st.markdown(
    """
    <style>
    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }
    .tracker-header {
        font-size: 2.3rem;
        font-weight: 800;
        color: #38BDF8;
        text-shadow: 0 0 24px rgba(56, 189, 248, 0.35);
        margin-bottom: 0.2rem;
        letter-spacing: -0.02em;
    }
    .tracker-subtitle {
        font-size: 1.05rem;
        color: #94A3B8;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: #1E293B;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 1.2rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
        text-align: center;
        transition: transform 0.15s ease-in-out, border-color 0.15s ease-in-out;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 12px -1px rgba(0, 0, 0, 0.4);
        border-color: #38BDF8;
    }
    .metric-title {
        font-size: 0.82rem;
        font-weight: 700;
        text-transform: uppercase;
        color: #94A3B8;
        letter-spacing: 0.05em;
        margin-bottom: 0.4rem;
    }
    .metric-value {
        font-size: 2.3rem;
        font-weight: 800;
        line-height: 1.1;
        margin-bottom: 0.4rem;
    }
    .metric-badge {
        display: inline-block;
        padding: 0.25rem 0.65rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .badge-green {
        background-color: rgba(34, 197, 94, 0.18);
        color: #4ADE80;
        border: 1px solid rgba(74, 222, 128, 0.35);
    }
    .badge-orange {
        background-color: rgba(249, 115, 22, 0.18);
        color: #FB923C;
        border: 1px solid rgba(251, 146, 60, 0.35);
    }
    .badge-red {
        background-color: rgba(239, 68, 68, 0.18);
        color: #F87171;
        border: 1px solid rgba(248, 113, 113, 0.35);
    }
    .badge-blue {
        background-color: rgba(56, 189, 248, 0.18);
        color: #38BDF8;
        border: 1px solid rgba(56, 189, 248, 0.35);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# 3. Resilient Database Query Execution (Non-locking for Airflow/dbt concurrency)
def get_db_path() -> str:
    """Resolve database path with fallback to demo database."""
    prod_db = Path("data/gold/rail_track.duckdb")
    if prod_db.exists():
        return str(prod_db)

    demo_db = Path("demo/rail_track_demo.duckdb")
    if demo_db.exists():
        return str(demo_db)

    return ":memory:"


def query_df(sql: str) -> pd.DataFrame:
    """Execute query with transient connection to release file lock immediately.

    DuckDB enforces lock exclusivity between writers (Airflow/dbt) and readers.
    Opening on-demand releases the file lock in < 5ms. Retries with backoff
    to gracefully handle transient writer locks during ETL batches.
    """
    db_path = get_db_path()
    last_err: Exception | None = None
    for attempt in range(5):
        try:
            with duckdb.connect(db_path, read_only=True) as con:
                return con.sql(sql).df()
        except duckdb.Error as err:
            last_err = err
            time.sleep(0.08 * (attempt + 1))

    demo_db = Path("demo/rail_track_demo.duckdb")
    if db_path != str(demo_db) and demo_db.exists():
        try:
            with duckdb.connect(str(demo_db), read_only=True) as con:
                return con.sql(sql).df()
        except Exception:
            pass

    if last_err:
        raise last_err
    return pd.DataFrame()


# 4. Header Section
col_head, col_badges = st.columns([2.6, 1.4])
with col_head:
    st.markdown(
        '<div class="tracker-header">🚆 RailTracker France</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="tracker-subtitle">'
        "L'Observatoire Citoyen & Indépendant de la Ponctualité du RER en Île-de-France"
        "</div>",
        unsafe_allow_html=True,
    )

with col_badges:
    st.markdown(
        """
        <div style="text-align: right; padding-top: 0.8rem;">
            <span style="font-size: 0.85rem; color: #94A3B8;">
                📡 Source : Île-de-France Mobilités (PRIM)
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("---")


# 5. Sidebar Filters & Architecture
with st.sidebar:
    sidebar_badge_html = (
        '<div style="background: rgba(56, 189, 248, 0.12); '
        "border: 1px solid rgba(56, 189, 248, 0.3); border-radius: 8px; "
        'padding: 0.6rem; text-align: center; margin-bottom: 1rem;">'
        '<span style="font-size: 1.05rem; font-weight: 700; color: #38BDF8;">'
        "🚆 Île-de-France Mobilités</span><br>"
        '<span style="font-size: 0.75rem; color: #94A3B8;">'
        "Flux OpenData PRIM</span>"
        "</div>"
    )
    st.markdown(sidebar_badge_html, unsafe_allow_html=True)
    st.markdown("### 🎛️ Filtres Réseau & Date")

    if st.button("🔄 Actualiser les données", width="stretch"):
        st.rerun()

    # Dynamic date selection from database
    dates_df = query_df(
        """
        SELECT
            d.date_key,
            d.full_date
        FROM dim_date d
        WHERE d.date_key IN (
            SELECT DISTINCT date_key FROM fact_train_punctuality
        )
        ORDER BY d.date_key DESC
        """
    )

    date_options: list[str] = []
    date_map: dict[str, int | None] = {}
    if not dates_df.empty:
        today_date = pd.Timestamp.now(tz="Europe/Paris").date()
        yesterday_date = today_date - pd.Timedelta(days=1)

        for idx, row in dates_df.iterrows():
            d_key = int(row["date_key"])
            d_val = row["full_date"].strftime("%d/%m/%Y")
            f_date = (
                row["full_date"].date()
                if hasattr(row["full_date"], "date")
                else row["full_date"]
            )
            if f_date == today_date:
                label = f"Aujourd'hui ({d_val})"
            elif f_date == yesterday_date:
                label = f"Hier ({d_val})"
            else:
                label = f"Journée du {d_val}"
            date_options.append(label)
            date_map[label] = d_key
        date_options.append("Tout l'historique")
        date_map["Tout l'historique"] = None

    selected_date = st.selectbox(
        "Date d'observation :",
        options=date_options,
        index=0,
    )

    date_filter_sql = ""
    if selected_date and date_map.get(selected_date):
        date_filter_sql = f"AND f.date_key = {date_map[selected_date]}"

    selected_line = st.selectbox(
        "Ligne de RER :",
        options=["Toutes les lignes (RER A & B)", "RER A", "RER B"],
        index=0,
    )

    line_filter_sql = ""
    if selected_line == "RER A":
        line_filter_sql = "AND t.line_name = 'A'"
    elif selected_line == "RER B":
        line_filter_sql = "AND t.line_name = 'B'"

    st.markdown("---")
    st.markdown("### ℹ️ Méthodologie")
    st.caption(
        "**Ponctualité Trains & Retard :** Mesurés à l'arrivée au terminus par mission "
        "(train), selon la norme officielle d'exploitation RATP/SNCF "
        "(seuil contractuel : **5 minutes**)."
    )
    st.caption(
        "**CartoTracker & Baromètre :** Mesurés par arrêt en gare afin d'identifier "
        "les points d'accumulation de retard sur la ligne."
    )

    st.markdown("---")
    st.markdown("### 🛠️ Architecture")
    st.markdown(
        """
        - **In-process OLAP :** DuckDB
        - **Transformation Silver :** Polars
        - **Modélisation :** dbt Core (Kimball)
        - **Orchestration :** Apache Airflow
        - **Visualisation :** Streamlit + Plotly
        """
    )


# 6. Global KPI Query (Train-level aggregation at terminus)
kpi_query = f"""
WITH train_journeys AS (
    SELECT
        f.journey_id,
        count(*) as stops_in_train,
        arg_max(f.arrival_delay_min, f.stop_sequence) as terminus_delay_min,
        bool_or(f.is_cancelled) as is_cancelled
    FROM fact_train_punctuality f
    JOIN dim_train t ON f.train_key = t.train_key
    WHERE 1=1 {date_filter_sql} {line_filter_sql}
    GROUP BY f.journey_id
)
SELECT
    coalesce(sum(stops_in_train), 0) as total_stops,
    count(*) as total_trains,
    coalesce(
        round(
            100.0 * avg(
                CASE WHEN NOT is_cancelled AND terminus_delay_min <= 5.0
                THEN 1 ELSE 0 END
            ),
            1
        ),
        100.0
    ) as punctuality_pct,
    coalesce(round(avg(terminus_delay_min), 2), 0.0) as avg_delay_min,
    coalesce(
        round(
            100.0 * avg(CASE WHEN is_cancelled THEN 1 ELSE 0 END),
            2
        ),
        0.0
    ) as cancel_pct
FROM train_journeys;
"""
kpi_data = query_df(kpi_query).iloc[0]

punctuality = float(kpi_data["punctuality_pct"])
avg_delay = float(kpi_data["avg_delay_min"])
total_stops = int(kpi_data["total_stops"])
total_trains = int(kpi_data["total_trains"])
cancel_pct = float(kpi_data["cancel_pct"])

if punctuality >= 95.0:
    punct_badge = "badge-green"
    punct_status = "🟢 Conforme IDFM"
elif punctuality >= 90.0:
    punct_badge = "badge-orange"
    punct_status = "🟠 Vigilance"
else:
    punct_badge = "badge-red"
    punct_status = "🔴 Critique"

if avg_delay <= 1.0:
    delay_badge = "badge-green"
elif avg_delay <= 2.0:
    delay_badge = "badge-orange"
else:
    delay_badge = "badge-red"

delay_sign = "+" if avg_delay > 0 else ""


# 7. KPI Cards (CovidTracker Style - Dark Mode Native)
c1, c2, c3, c4 = st.columns(4)

with c1:
    if punctuality >= 95.0:
        punct_color = "#4ADE80"
    elif punctuality >= 90.0:
        punct_color = "#FB923C"
    else:
        punct_color = "#F87171"

    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Ponctualité Trains (&lt; 5 min)</div>
            <div class="metric-value" style="color: {punct_color};">
                {punctuality:.1f} %
            </div>
            <span class="metric-badge {punct_badge}">{punct_status}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

with c2:
    if avg_delay <= 1.0:
        delay_color = "#4ADE80"
    elif avg_delay <= 2.0:
        delay_color = "#FB923C"
    else:
        delay_color = "#F87171"

    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Retard Moyen par Train</div>
            <div class="metric-value" style="color: {delay_color};">
                {delay_sign}{avg_delay:.2f} min
            </div>
            <span class="metric-badge {delay_badge}">Au terminus</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

with c3:
    cancel_badge = (
        "badge-green"
        if cancel_pct == 0.0
        else ("badge-orange" if cancel_pct <= 2.0 else "badge-red")
    )
    cancel_label = f"{cancel_pct:.1f} % annulés" if cancel_pct > 0 else "0 % annulé"
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Trains Suivis</div>
            <div class="metric-value" style="color: #60A5FA;">
                {total_trains:,}
            </div>
            <span class="metric-badge {cancel_badge}">{cancel_label}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

with c4:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Arrêts Observés</div>
            <div class="metric-value" style="color: #94A3B8;">
                {total_stops:,}
            </div>
            <span class="metric-badge badge-blue">Dernier cycle d'observation</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)


# 8. Main Visualizations: Map & Hourly Thermometer
col_map, col_chart = st.columns([1.05, 1.15])

with col_map:
    st.markdown("### 🗺️ CartoTracker : Santé des Gares en Direct")
    map_query = f"""
    SELECT
        s.station_name,
        s.latitude,
        s.longitude,
        string_agg(DISTINCT t.line_name, ', ') as lines,
        count(*) as stops_count,
        round(
            avg(COALESCE(f.arrival_delay_min, f.departure_delay_min)), 2
        ) as avg_delay_min,
        round(
            100.0 * avg(
                CASE WHEN NOT f.is_cancelled AND NOT f.is_late_5min
                THEN 1 ELSE 0 END
            ),
            1
        ) as punctuality_pct
    FROM fact_train_punctuality f
    JOIN dim_station s ON f.station_key = s.station_key
    JOIN dim_train t ON f.train_key = t.train_key
    WHERE s.latitude IS NOT NULL AND s.latitude != 0
    {date_filter_sql}
    {line_filter_sql}
    GROUP BY s.station_name, s.latitude, s.longitude
    HAVING count(*) >= 5;
    """
    map_df = query_df(map_query)

    if not map_df.empty:

        def classify_status(delay: float) -> str:
            if delay <= 1.0:
                return "Fluide (< 1 min)"
            elif delay <= 2.0:
                return "Ralentissement (1-2 min)"
            return "Retard marqué (> 2 min)"

        map_df["Statut"] = map_df["avg_delay_min"].apply(classify_status)

        map_kwargs = {
            "data_frame": map_df,
            "lat": "latitude",
            "lon": "longitude",
            "color": "Statut",
            "size": "stops_count",
            "size_max": 16,
            "hover_name": "station_name",
            "hover_data": {
                "latitude": False,
                "longitude": False,
                "lines": True,
                "avg_delay_min": ":.2f min",
                "punctuality_pct": ":.1f %",
                "stops_count": True,
            },
            "color_discrete_map": {
                "Fluide (< 1 min)": "#22C55E",
                "Ralentissement (1-2 min)": "#F59E0B",
                "Retard marqué (> 2 min)": "#EF4444",
            },
            "zoom": 9.8,
            "center": {"lat": 48.8566, "lon": 2.3522},
            "height": 460,
        }
        if hasattr(px, "scatter_map"):
            map_kwargs["map_style"] = "carto-darkmatter"
            fig_map = px.scatter_map(**map_kwargs)
        else:
            map_kwargs["mapbox_style"] = "carto-darkmatter"
            fig_map = px.scatter_mapbox(**map_kwargs)

        fig_map.update_layout(
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15, 23, 42, 0.5)",
            legend=dict(
                yanchor="top",
                y=0.98,
                xanchor="left",
                x=0.02,
                bgcolor="rgba(15, 23, 42, 0.85)",
                font=dict(color="#F8FAFC", size=11),
                bordercolor="#334155",
                borderwidth=1,
            ),
        )
        st.plotly_chart(fig_map, width="stretch")
    else:
        st.info("Aucune donnée cartographique disponible pour ce filtre.")


with col_chart:
    st.markdown("### 📈 Le Thermomètre Horaire de la Journée")
    # For today, filter out future scheduled trains so the curve ends at current time
    future_filter_sql = ""
    if selected_date and selected_date.startswith("Aujourd'hui"):
        future_filter_sql = (
            "AND terminus_time <= (SELECT max(recorded_at) FROM fact_train_punctuality)"
        )

    hourly_query = f"""
    WITH train_terminus AS (
        SELECT
            f.journey_id,
            arg_max(
                COALESCE(f.aimed_arrival_time, f.aimed_departure_time),
                f.stop_sequence
            ) as terminus_time,
            arg_max(f.arrival_delay_min, f.stop_sequence) as terminus_delay_min,
            bool_or(f.is_cancelled) as is_cancelled
        FROM fact_train_punctuality f
        JOIN dim_train t ON f.train_key = t.train_key
        WHERE 1=1 {date_filter_sql} {line_filter_sql}
        GROUP BY f.journey_id
    )
    SELECT
        EXTRACT(
            hour FROM timezone('Europe/Paris', terminus_time)
        ) as hour_of_day,
        round(
            100.0 * avg(
                CASE WHEN NOT is_cancelled AND terminus_delay_min <= 5.0
                THEN 1 ELSE 0 END
            ),
            1
        ) as punctuality_pct,
        round(avg(terminus_delay_min), 2) as avg_delay_min,
        count(*) as trains_count
    FROM train_terminus
    WHERE 1=1 {future_filter_sql}
    GROUP BY hour_of_day
    ORDER BY hour_of_day;
    """
    hourly_df = query_df(hourly_query)

    if not hourly_df.empty:
        hourly_df["delay_str"] = hourly_df["avg_delay_min"].apply(
            lambda d: f"+{d:.2f} min" if d > 0 else f"{d:.2f} min"
        )
        fig_hourly = go.Figure()

        matin_annotation = (
            "🌅 <b>Pointe Matin</b><br>"
            "<span style='font-size:10px;color:#CBD5E1;'>07h30 - 09h30</span>"
        )
        soir_annotation = (
            "🌆 <b>Pointe Soir</b><br>"
            "<span style='font-size:10px;color:#CBD5E1;'>16h30 - 19h30</span>"
        )

        # Morning peak hours (07:30 - 09:30)
        fig_hourly.add_vrect(
            x0=7.5,
            x1=9.5,
            fillcolor="rgba(245, 158, 11, 0.16)",
            opacity=1.0,
            line_width=1.5,
            line_dash="dot",
            line_color="rgba(245, 158, 11, 0.7)",
            annotation_text=matin_annotation,
            annotation_position="top left",
            annotation_font=dict(color="#FBBF24", size=11),
        )

        # Evening peak hours (16:30 - 19:30)
        fig_hourly.add_vrect(
            x0=16.5,
            x1=19.5,
            fillcolor="rgba(245, 158, 11, 0.16)",
            opacity=1.0,
            line_width=1.5,
            line_dash="dot",
            line_color="rgba(245, 158, 11, 0.7)",
            annotation_text=soir_annotation,
            annotation_position="top left",
            annotation_font=dict(color="#FBBF24", size=11),
        )

        # IDFM contractual punctuality target (95%)
        fig_hourly.add_hline(
            y=95.0,
            line_dash="dash",
            line_color="#22C55E",
            line_width=1.5,
            annotation_text="🎯 Objectif IDFM (95 %)",
            annotation_position="bottom left",
            annotation_font=dict(color="#4ADE80", size=11),
        )

        hover_hourly = (
            "Ponctualité : <b>%{y:.1f} %</b><br>"
            "Retard moyen : <b>%{customdata[0]}</b><br>"
            "Trains au terminus : <b>%{customdata[1]}</b><extra></extra>"
        )

        # CovidTracker-style punctuality curve (train-level aggregation at terminus)
        fig_hourly.add_trace(
            go.Scatter(
                x=hourly_df["hour_of_day"],
                y=hourly_df["punctuality_pct"],
                customdata=hourly_df[["delay_str", "trains_count"]].values,
                mode="lines+markers",
                name="Taux de Ponctualité",
                line=dict(color="#38BDF8", width=3.5, shape="spline"),
                marker=dict(
                    size=8,
                    color="#38BDF8",
                    line=dict(color="#FFFFFF", width=1.5),
                ),
                fill="tozeroy",
                fillcolor="rgba(56, 189, 248, 0.08)",
                hovertemplate=hover_hourly,
            )
        )

        y_min = max(0, int(hourly_df["punctuality_pct"].min() - 4))
        y_min = min(y_min, 88)

        fig_hourly.update_layout(
            template="plotly_dark",
            height=460,
            margin={"r": 20, "t": 20, "l": 20, "b": 40},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15, 23, 42, 0.5)",
            font=dict(color="#F8FAFC"),
            xaxis=dict(
                title=dict(
                    text="Heure de la journée (Paris CET)",
                    font=dict(color="#94A3B8"),
                ),
                tickmode="linear",
                tick0=0,
                dtick=2,
                ticksuffix="h",
                range=[5.5, 23.5],
                gridcolor="#334155",
                tickfont=dict(color="#94A3B8", size=11),
            ),
            yaxis=dict(
                title=dict(
                    text="Ponctualité (< 5 min)",
                    font=dict(color="#94A3B8"),
                ),
                ticksuffix=" %",
                range=[y_min, 103],
                gridcolor="#334155",
                tickfont=dict(color="#94A3B8", size=11),
            ),
            hovermode="x unified",
            showlegend=False,
        )
        st.plotly_chart(fig_hourly, width="stretch")


# 9. Top Bottleneck Stations (Delay Barometer - High Contrast & Outside Labels)
st.markdown("---")
st.markdown("### 🏆 Le Baromètre : Top 10 des Gares les plus ralenties")

top_stations_query = f"""
SELECT
    s.station_name as "Gare",
    string_agg(DISTINCT t.line_name, ', ') as "Ligne",
    count(*) as "Nombre d'Arrêts",
    round(
        avg(COALESCE(f.arrival_delay_min, f.departure_delay_min)), 2
    ) as "Retard Moyen (min)",
    round(
        100.0 * avg(
            CASE WHEN NOT f.is_cancelled AND NOT f.is_late_5min
            THEN 1 ELSE 0 END
        ),
        1
    ) as "Taux Ponctualité (%)"
FROM fact_train_punctuality f
JOIN dim_station s ON f.station_key = s.station_key
JOIN dim_train t ON f.train_key = t.train_key
WHERE s.station_name != 'Station non répertoriée'
{date_filter_sql}
{line_filter_sql}
GROUP BY s.station_name
HAVING count(*) >= 10
ORDER BY avg(COALESCE(f.arrival_delay_min, f.departure_delay_min)) DESC
LIMIT 10;
"""
top_df = query_df(top_stations_query)

if not top_df.empty:
    top_df["Gare_Affichage"] = (
        top_df["Ligne"].apply(lambda line_code: f"RER {line_code} │ ") + top_df["Gare"]
    )

    max_delay = float(top_df["Retard Moyen (min)"].max())
    x_limit = max_delay * 1.25 if max_delay > 0 else 1.0

    fig_bar = px.bar(
        top_df,
        x="Retard Moyen (min)",
        y="Gare_Affichage",
        orientation="h",
        color="Retard Moyen (min)",
        range_color=[0, max(max_delay, 2.5)],
        color_continuous_scale=[
            [0, "#F59E0B"],
            [0.4, "#F97316"],
            [0.8, "#EF4444"],
            [1.0, "#B91C1C"],
        ],
        text="Retard Moyen (min)",
        height=430,
    )
    fig_bar.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15, 23, 42, 0.5)",
        font=dict(color="#F8FAFC"),
        yaxis=dict(
            autorange="reversed",
            tickfont=dict(color="#F8FAFC", size=13),
            title=None,
        ),
        xaxis=dict(
            range=[0, x_limit],
            gridcolor="#334155",
            tickfont=dict(color="#94A3B8", size=11),
            title=dict(
                text="Retard Moyen Constaté (minutes)",
                font=dict(color="#94A3B8", size=12),
            ),
        ),
        margin={"r": 30, "t": 10, "l": 20, "b": 20},
        coloraxis_showscale=False,
    )

    hover_bar = "<b>%{y}</b><br>Retard moyen : <b>+%{x:.2f} min</b><extra></extra>"
    fig_bar.update_traces(
        texttemplate="<b>+%{x:.2f} min</b>",
        textposition="outside",
        textfont=dict(color="#F8FAFC", size=13),
        hovertemplate=hover_bar,
    )
    st.plotly_chart(fig_bar, width="stretch")
else:
    st.info("Aucun retard significatif détecté pour cette sélection.")

st.markdown("---")
footer_html = (
    '<div style="text-align: center; color: #94A3B8; font-size: 0.85rem; '
    'padding-top: 1rem; padding-bottom: 1.5rem;">'
    "🚆 <b>RailTracker France</b> • "
    "L'Observatoire Citoyen & Indépendant de la Ponctualité du RER<br>"
    '<span style="color: #64748B;">'
    "Données OpenData Île-de-France Mobilités (PRIM)</span></div>"
)
st.markdown(footer_html, unsafe_allow_html=True)
