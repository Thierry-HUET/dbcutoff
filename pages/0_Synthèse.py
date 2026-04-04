"""
bench_viz.py — Page principale : Synthèse des performances

Lancement
---------
    streamlit run bench_viz.py
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from storage.db_store import fetch_runs
from storage.db_store import fetch_versions
from viz_common import (
    load_data, load_runs, render_sidebar,
    make_figure, label_op, db_color, DB_COLORS,
    SQL_DBS, NOSQL_DBS, db_category,
)

st.set_page_config(
    page_title="DB Cutoff — Synthèse",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Sélection de session
# ---------------------------------------------------------------------------
run_options = load_runs()
selected_label = st.sidebar.selectbox(
    "Session de benchmark",
    options=["— Toutes —"] + list(run_options.keys()),
)
run_id = run_options.get(selected_label) if selected_label != "— Toutes —" else None

df_raw, df_agg = load_data(run_id)

if df_raw.empty:
    st.title("📊 DB Cutoff Analyzer — Synthèse")
    st.info("Aucun résultat disponible. Lancez d'abord `bench_runner.py`.")
    st.stop()

sel_ops, sel_dbs = render_sidebar(df_raw, "Synthèse")

df = df_raw[
    df_raw["operation"].isin(sel_ops) &
    df_raw["db_name"].isin(sel_dbs)
].copy()

df_agg = df_agg[
    df_agg["operation"].isin(sel_ops) &
    df_agg["db_name"].isin(sel_dbs)
].copy()

# ---------------------------------------------------------------------------
# En-tête
# ---------------------------------------------------------------------------
st.title("📊 DB Cutoff Analyzer — Synthèse")
st.caption("Vue consolidée SQL et NoSQL — source INSEE (StockEtablissementHistorique)")

# Versions des bases testées
versions = fetch_versions()
if versions:
    badges = " &nbsp;·&nbsp; ".join(
        f'<span style="background:{DB_COLORS.get(db,"#999")};color:white;'
        f'padding:2px 8px;border-radius:4px;font-size:0.85em">'
        f'{db} {ver}</span>'
        for db, ver in sorted(versions.items())
    )
    st.markdown(badges, unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Mesures totales",  len(df))
c2.metric("Bases testées",    df["db_name"].nunique())
c3.metric("Opérations",       df["operation"].nunique())
c4.metric("Volumes testés",   df["volume"].nunique())

st.divider()

# ---------------------------------------------------------------------------
# Tableau de synthèse : meilleur temps médian par opération et par base
# ---------------------------------------------------------------------------
st.subheader("🏆 Meilleures performances par opération")

if not df_agg.empty:
    pivot = (
        df_agg.groupby(["db_name", "operation"])["duration_med"]
        .min()
        .reset_index()
    )

    # Pour chaque opération, trouver la base la plus rapide
    best = (
        pivot.loc[pivot.groupby("operation")["duration_med"].idxmin()]
        .rename(columns={"db_name": "Meilleure base", "duration_med": "Durée min (s)"})
    )
    best["Opération"]    = best["operation"].map(label_op)
    best["Catégorie"]    = best["Meilleure base"].map(db_category)
    best["Durée min (s)"] = best["Durée min (s)"].round(4)

    # ---------------------------------------------------------------------------
    # Tableau croisé groupé par type d'opération
    # ---------------------------------------------------------------------------

    # Mapping opération → groupe d'affichage (ordre souhaité dans le tableau)
    OP_GROUPS = {
        "write_bulk":             "✍️ Écriture",
        "write_row_by_row":       "✍️ Écriture",
        "read_full":              "📖 Lecture",
        "read_filtered":          "📖 Lecture",
        "read_full_indexed":      "📖 Lecture",
        "read_filtered_indexed":  "📖 Lecture",
        "vector_insert":          "🔢 Vectoriel",
        "vector_search_exact":    "🔢 Vectoriel",
        "vector_search_approx":   "🔢 Vectoriel",
    }
    GROUP_ORDER = ["✍️ Écriture", "📖 Lecture", "🔢 Vectoriel"]

    pivot_wide = pivot.copy()
    pivot_wide["groupe"]    = pivot_wide["operation"].map(OP_GROUPS).fillna("Autre")
    pivot_wide["operation"] = pivot_wide["operation"].map(label_op)
    pivot_wide = pivot_wide.pivot(
        index=["groupe", "operation"], columns="db_name", values="duration_med"
    )
    pivot_wide.index.names  = ["Groupe", "Opération"]
    pivot_wide.columns.name = None

    # Trier par groupe dans l'ordre défini
    pivot_wide = pivot_wide.reindex(
        [g for g in GROUP_ORDER if g in pivot_wide.index.get_level_values("Groupe")],
        level="Groupe",
    )

    def highlight_minmax(row):
        """Vert = meilleur, Rouge = pire sur la ligne (NaN ignorés)."""
        styles = []
        valid  = row.dropna()
        if valid.empty:
            return [""] * len(row)
        mn, mx = valid.min(), valid.max()
        for v in row:
            if v != v:
                styles.append("")
            elif mn == mx:
                styles.append("")
            elif v == mn:
                styles.append("background-color: #d4f0d4; font-weight:bold")
            elif v == mx:
                styles.append("background-color: #f9d4d4; font-weight:bold")
            else:
                styles.append("")
        return styles

    st.dataframe(
        pivot_wide.style
        .apply(highlight_minmax, axis=1)
        .format("{:.4f}", na_rep="—"),
        width="stretch",
    )
    st.caption(
        "🟢 Cellule verte = meilleure performance · "
        "🔴 Cellule rouge = moins bonne performance · "
        "— = non testé"
    )

st.divider()

# ---------------------------------------------------------------------------
# Graphe global toutes opérations — vue d'ensemble
# ---------------------------------------------------------------------------
st.subheader("Volume / Temps — toutes opérations (SQL + NoSQL)")

fig_all = make_figure(df_agg, ops=None, height=520)
if fig_all:
    st.plotly_chart(fig_all, width="stretch")

st.divider()

# ---------------------------------------------------------------------------
# Comparatif SQL vs NoSQL sur une opération clé
# ---------------------------------------------------------------------------
st.subheader("Comparatif SQL vs NoSQL")

ops_disponibles = sorted(df_agg["operation"].unique())
op_choisie = st.selectbox(
    "Opération à comparer",
    options=ops_disponibles,
    format_func=label_op,
    index=ops_disponibles.index("read_filtered") if "read_filtered" in ops_disponibles else 0,
)

df_op = df_agg[df_agg["operation"] == op_choisie].copy()

if not df_op.empty:
    col_sql, col_nosql = st.columns(2)

    for col, cat, emoji in [
        (col_sql,   "SQL",   "🗄️"),
        (col_nosql, "NoSQL", "🍃"),
    ]:
        df_cat = df_op[df_op["categorie"] == cat]
        with col:
            st.markdown(f"**{emoji} {cat}**")
            if df_cat.empty:
                st.info(f"Aucune base {cat} dans les filtres.")
            else:
                fig_cat = go.Figure()
                _unk: dict = {}
                for (db, op_), grp in df_cat.groupby(["db_name", "operation"]):
                    from viz_common import make_trace
                    fig_cat.add_trace(make_trace(db, op_, grp, _unk))
                from viz_common import BASE_LAYOUT
                fig_cat.update_layout(
                    **BASE_LAYOUT,
                    xaxis_title="Volume (lignes)",
                    yaxis_title="Durée médiane (s)",
                    height=360,
                    showlegend=True,
                )
                st.plotly_chart(fig_cat, width="stretch")

st.divider()

# ---------------------------------------------------------------------------
# Tableau récapitulatif exportable
# ---------------------------------------------------------------------------
st.subheader("Données brutes (médiane)")

df_tableau = df_agg.copy()
df_tableau["operation"] = df_tableau["operation"].map(label_op)
df_tableau = df_tableau.rename(columns={
    "db_name":      "Base de données",
    "operation":    "Opération",
    "categorie":    "Catégorie",
    "indexed":      "Indexé",
    "volume":       "Volume (lignes)",
    "duration_med": "Durée médiane (s)",
    "n":            "Nb mesures",
})

st.dataframe(
    df_tableau.sort_values(["Catégorie", "Base de données", "Opération", "Volume (lignes)"])
    .style.format({"Durée médiane (s)": "{:.4f}", "Volume (lignes)": "{:,}"}),
    width="stretch",
    hide_index=True,
)

csv = df_tableau.to_csv(index=False).encode("utf-8")
st.download_button(
    "⬇️  Exporter les résultats (CSV)",
    data=csv,
    file_name="db_cutoff_synthese.csv",
    mime="text/csv",
)