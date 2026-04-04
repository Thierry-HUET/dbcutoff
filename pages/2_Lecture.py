"""
pages/2_Lecture.py — Benchmark en lecture : toutes bases confondues
"""

import streamlit as st
from viz_common import (
    load_data, load_runs, render_sidebar,
    make_figure, label_op,
)

st.set_page_config(page_title="DB Cutoff — Lecture", page_icon="📖", layout="wide")

run_options    = load_runs()
selected_label = st.sidebar.selectbox("Session", ["— Toutes —"] + list(run_options.keys()))
run_id         = run_options.get(selected_label) if selected_label != "— Toutes —" else None

df_raw, df_agg = load_data(run_id)

if df_raw.empty:
    st.title("📖 Benchmark Lecture")
    st.info("Aucun résultat. Lancez `bench_runner.py`.")
    st.stop()

OPS_LECTURE      = ["read_full", "read_filtered", "read_full_indexed", "read_filtered_indexed"]
OPS_SANS_INDEX   = ["read_full", "read_filtered"]
OPS_AVEC_INDEX   = ["read_full_indexed", "read_filtered_indexed"]

df_raw  = df_raw[df_raw["operation"].isin(OPS_LECTURE)].copy()
df_agg  = df_agg[df_agg["operation"].isin(OPS_LECTURE)].copy()

if df_raw.empty:
    st.title("📖 Benchmark Lecture")
    st.info("Aucune mesure de lecture disponible.")
    st.stop()

sel_ops, sel_dbs = render_sidebar(df_raw, "Lecture")
df_agg = df_agg[
    df_agg["operation"].isin(sel_ops) &
    df_agg["db_name"].isin(sel_dbs)
].copy()

# ---------------------------------------------------------------------------
st.title("📖 Benchmark Lecture")
st.caption(
    "Comparaison toutes bases — lecture complète et filtrée, avec et sans index\n\n"
    "Filtre appliqué : `etat_administratif = 'F'` (établissements fermés, ~5% — haute sélectivité)"
)

c1, c2, c3 = st.columns(3)
c1.metric("Bases testées",   df_raw["db_name"].nunique())
c2.metric("Mesures totales", len(df_raw[df_raw["db_name"].isin(sel_dbs)]))
c3.metric("Volumes testés",  df_raw["volume"].nunique())
st.divider()

# ---------------------------------------------------------------------------
st.subheader("Toutes lectures — vue d'ensemble")

fig_all = make_figure(df_agg, ops=OPS_LECTURE, height=500)
if fig_all:
    st.plotly_chart(fig_all, width="stretch")
    st.caption(
        "Ligne pleine = sans index · Tirets = avec index · "
        "Cercle = lecture complète · Losange = lecture filtrée"
    )

st.divider()

# ---------------------------------------------------------------------------
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Sans index")
    fig_sans = make_figure(df_agg, ops=OPS_SANS_INDEX, height=400)
    if fig_sans:
        st.plotly_chart(fig_sans, width="stretch")
    else:
        st.info("Aucune donnée.")

with col_right:
    st.subheader("Avec index")
    fig_avec = make_figure(df_agg, ops=OPS_AVEC_INDEX, height=400)
    if fig_avec:
        st.plotly_chart(fig_avec, width="stretch")
    else:
        st.info("Aucune donnée.")

st.divider()

# ---------------------------------------------------------------------------
st.subheader("Impact de l'index — lecture filtrée")
st.caption("Superposition sans index (plein) vs avec index (tirets) par base — écart = gain apporté par l'index")

fig_index = make_figure(
    df_agg,
    ops=["read_filtered", "read_filtered_indexed"],
    height=420,
)
if fig_index:
    st.plotly_chart(fig_index, width="stretch")

st.divider()

# ---------------------------------------------------------------------------
st.subheader("Données brutes — Lecture (médiane)")

df_tableau = df_agg.copy()
df_tableau["operation"] = df_tableau["operation"].map(label_op)
df_tableau = df_tableau.rename(columns={
    "db_name": "Base", "operation": "Opération",
    "volume": "Volume (lignes)", "duration_med": "Durée médiane (s)", "n": "Nb mesures",
})
st.dataframe(
    df_tableau.sort_values(["Base", "Opération", "Volume (lignes)"])
    .style.format({"Durée médiane (s)": "{:.4f}", "Volume (lignes)": "{:,}"}),
    width="stretch", hide_index=True,
)

csv = df_tableau.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Exporter (CSV)", data=csv, file_name="lecture.csv", mime="text/csv")