"""
pages/3_Vectoriel.py — Benchmark vectoriel : toutes bases supportées
"""

import streamlit as st
from viz_common import (
    load_data, load_runs, render_sidebar,
    make_figure, label_op,
)

st.set_page_config(page_title="DB Cutoff — Vectoriel", page_icon="🔢", layout="wide")

run_options    = load_runs()
selected_label = st.sidebar.selectbox("Session", ["— Toutes —"] + list(run_options.keys()))
run_id         = run_options.get(selected_label) if selected_label != "— Toutes —" else None

df_raw, df_agg = load_data(run_id)

if df_raw.empty:
    st.title("🔢 Benchmark Vectoriel")
    st.info("Aucun résultat. Lancez `bench_runner.py`.")
    st.stop()

OPS_VECTOR = ["vector_insert", "vector_search_exact", "vector_search_approx"]

df_raw = df_raw[df_raw["operation"].isin(OPS_VECTOR)].copy()
df_agg = df_agg[df_agg["operation"].isin(OPS_VECTOR)].copy()

if df_raw.empty:
    st.title("🔢 Benchmark Vectoriel")
    st.info(
        "Aucune mesure vectorielle disponible.\n\n"
        "Bases supportées :\n"
        "- **PostgreSQL** : installer pgvector (`brew install pgvector`)\n"
        "- **DuckDB** : support natif, aucun prérequis"
    )
    st.stop()

sel_ops, sel_dbs = render_sidebar(df_raw, "Vectoriel")
df_agg = df_agg[
    df_agg["operation"].isin(sel_ops) &
    df_agg["db_name"].isin(sel_dbs)
].copy()

# ---------------------------------------------------------------------------
st.title("🔢 Benchmark Vectoriel")
st.caption(
    "Vecteurs normalisés de dimension 128 · Graine fixe (seed=42) pour la reproductibilité\n\n"
    "**PostgreSQL** : index HNSW via pgvector · "
    "**DuckDB** : array_cosine_similarity() sans index ANN natif"
)

c1, c2, c3 = st.columns(3)
c1.metric("Bases testées",   df_raw["db_name"].nunique())
c2.metric("Mesures totales", len(df_raw[df_raw["db_name"].isin(sel_dbs)]))
c3.metric("Volumes testés",  df_raw["volume"].nunique())
st.divider()

# ---------------------------------------------------------------------------
st.subheader("Vue d'ensemble — insertion et recherche vectorielle")

fig_all = make_figure(
    df_agg, ops=OPS_VECTOR,
    x_label="Volume (vecteurs)", height=500,
)
if fig_all:
    st.plotly_chart(fig_all, width="stretch")
    st.caption(
        "Ligne pleine = insertion · "
        "Tirets = recherche exacte (brute force) · "
        "Pointillés = recherche approx. (ANN / HNSW)"
    )

st.divider()

# ---------------------------------------------------------------------------
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Insertion vectorielle")
    fig_ins = make_figure(
        df_agg, ops=["vector_insert"],
        x_label="Volume (vecteurs)", height=380,
    )
    if fig_ins:
        st.plotly_chart(fig_ins, width="stretch")

with col_right:
    st.subheader("Recherche : exacte vs approx.")
    fig_search = make_figure(
        df_agg, ops=["vector_search_exact", "vector_search_approx"],
        x_label="Volume (vecteurs)", height=380,
    )
    if fig_search:
        st.plotly_chart(fig_search, width="stretch")
        st.caption("Ligne pleine = exacte · Pointillés = approximative")

st.divider()

# ---------------------------------------------------------------------------
st.subheader("Impact de l'index ANN — recherche approx. vs exacte")
st.caption("Écart entre les deux courbes = gain apporté par l'index HNSW")

fig_cmp = make_figure(
    df_agg,
    ops=["vector_search_exact", "vector_search_approx"],
    x_label="Volume (vecteurs)", height=400,
)
if fig_cmp:
    st.plotly_chart(fig_cmp, width="stretch")

st.divider()

# ---------------------------------------------------------------------------
st.subheader("Données brutes — Vectoriel (médiane)")

df_tableau = df_agg.copy()
df_tableau["operation"] = df_tableau["operation"].map(label_op)
df_tableau = df_tableau.rename(columns={
    "db_name": "Base", "operation": "Opération",
    "volume": "Volume (vecteurs)", "duration_med": "Durée médiane (s)", "n": "Nb mesures",
})
st.dataframe(
    df_tableau.sort_values(["Base", "Opération", "Volume (vecteurs)"])
    .style.format({"Durée médiane (s)": "{:.4f}", "Volume (vecteurs)": "{:,}"}),
    width="stretch", hide_index=True,
)

csv = df_tableau.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Exporter (CSV)", data=csv, file_name="vectoriel.csv", mime="text/csv")