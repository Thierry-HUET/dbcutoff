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
    # ---------------------------------------------------------------------------
    # Comparaison au volume maximum COMMUN à toutes les bases
    # → évite qu'une base paraisse bonne parce qu'elle est mesurée
    #   uniquement à petit volume (ex: MongoDB à 100 lignes vs PostgreSQL à 10M)
    # ---------------------------------------------------------------------------

    # Volume max commun : le plus grand volume présent chez TOUTES les bases
    # pour chaque opération
    def pivot_at_common_max(df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for op, grp in df.groupby("operation"):
            # Volumes disponibles par base pour cette opération
            vol_by_db = grp.groupby("db_name")["volume"].max()
            if vol_by_db.empty:
                continue
            # Volume commun = le plus petit des max (présent chez toutes les bases)
            common_vol = vol_by_db.min()
            subset = grp[grp["volume"] == common_vol]
            for _, row in subset.iterrows():
                rows.append({
                    "db_name":      row["db_name"],
                    "operation":    op,
                    "duration_med": row["duration_med"],
                    "volume_ref":   common_vol,
                })
        return pd.DataFrame(rows)

    pivot = pivot_at_common_max(df_agg)

    if pivot.empty:
        pivot = (
            df_agg.groupby(["db_name", "operation"])["duration_med"]
            .min().reset_index()
        )
        volume_note = "volume minimum"
    else:
        volume_note = f"volume commun le plus élevé par opération"

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

    # --- Tableau en valeurs brutes (secondes) ---
    pivot_wide_raw = pivot.copy()
    pivot_wide_raw["groupe"]    = pivot_wide_raw["operation"].map(OP_GROUPS).fillna("Autre")
    pivot_wide_raw["operation"] = pivot_wide_raw["operation"].map(label_op)
    pivot_wide_raw = pivot_wide_raw.pivot(
        index=["groupe", "operation"], columns="db_name", values="duration_med"
    )
    pivot_wide_raw.index.names  = ["Groupe", "Opération"]
    pivot_wide_raw.columns.name = None
    pivot_wide_raw = pivot_wide_raw.reindex(
        [g for g in GROUP_ORDER if g in pivot_wide_raw.index.get_level_values("Groupe")],
        level="Groupe",
    )

    # --- Tableau en pourcentage (min de la ligne = 100%) ---
    def to_pct(row):
        """Convertit une ligne en % par rapport au minimum (meilleur = 100%)."""
        valid = row.dropna()
        if valid.empty or valid.min() == 0:
            return row
        return (row / valid.min() * 100).round(0)

    pivot_wide_pct = pivot_wide_raw.apply(to_pct, axis=1)

    def highlight_minmax_pct(row):
        """Vert = 100% (meilleur) · Rouge = valeur max (pire) · NaN ignorés."""
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
            elif v == mn:   # = 100% → meilleur
                styles.append("background-color: #d4f0d4; font-weight:bold")
            elif v == mx:   # = valeur max → pire
                styles.append("background-color: #f9d4d4; font-weight:bold")
            else:
                styles.append("")
        return styles

    # Basculer entre les deux vues
    vue = st.radio(
        "Affichage",
        ["Pourcentage (min = 100%)", "Durée brute (secondes)"],
        horizontal=True,
    )

    if vue == "Pourcentage (min = 100%)":
        df_display = pivot_wide_pct
        fmt        = "{:.0f} %"
        note_fmt   = "100% = meilleure performance de la ligne"
    else:
        df_display = pivot_wide_raw
        fmt        = "{:.4f} s"
        note_fmt   = "valeurs en secondes (médiane)"

    st.dataframe(
        df_display.style
        .apply(highlight_minmax_pct, axis=1)
        .format(fmt, na_rep="—"),
        width="stretch",
    )
    st.caption(
        f"🟢 = meilleure performance ({note_fmt}) · "
        f"🔴 = moins bonne · — = non testé · "
        f"Référence : {volume_note}"
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