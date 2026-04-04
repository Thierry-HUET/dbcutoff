"""
pages/1_Écriture.py — Benchmark en écriture : toutes bases confondues
"""

import streamlit as st
from viz_common import (
    load_data, load_runs, render_sidebar,
    make_figure, label_op, db_color,
)

st.set_page_config(page_title="DB Cutoff — Écriture", page_icon="✍️", layout="wide")

# Sélection de session
run_options    = load_runs()
selected_label = st.sidebar.selectbox("Session", ["— Toutes —"] + list(run_options.keys()))
run_id         = run_options.get(selected_label) if selected_label != "— Toutes —" else None

df_raw, df_agg = load_data(run_id)

if df_raw.empty:
    st.title("✍️ Benchmark Écriture")
    st.info("Aucun résultat. Lancez `bench_runner.py`.")
    st.stop()

OPS_ECRITURE = ["write_bulk", "write_row_by_row"]
df_raw  = df_raw[df_raw["operation"].isin(OPS_ECRITURE)].copy()
df_agg  = df_agg[df_agg["operation"].isin(OPS_ECRITURE)].copy()

if df_raw.empty:
    st.title("✍️Benchmark Écriture")
    st.info("Aucune mesure d'écriture disponible.")
    st.stop()

sel_ops, sel_dbs = render_sidebar(df_raw, "Écriture")
df_agg = df_agg[
    df_agg["operation"].isin(sel_ops) &
    df_agg["db_name"].isin(sel_dbs)
].copy()

# ---------------------------------------------------------------------------
st.title("✍️ Benchmark Écriture")
st.caption("Comparaison toutes bases — écriture en lot vs ligne par ligne")

c1, c2, c3 = st.columns(3)
c1.metric("Bases testées",  df_raw["db_name"].nunique())
c2.metric("Mesures totales", len(df_raw[df_raw["db_name"].isin(sel_dbs)]))
c3.metric("Volumes testés", df_raw["volume"].nunique())
st.divider()

# ---------------------------------------------------------------------------
st.subheader("Écriture en lot vs ligne par ligne — toutes bases")

fig = make_figure(df_agg, ops=OPS_ECRITURE, height=500)
if fig:
    st.plotly_chart(fig, width="stretch")
    st.caption(
        "Ligne pleine = écriture en lot · Pointillés = ligne par ligne\n\n"
        "**PostgreSQL** : COPY protocol · "
        "**DuckDB** : INSERT SELECT FROM DataFrame · "
        "**MySQL** : executemany() · "
        "**MongoDB** : insert_many(ordered=False) · "
        "**CouchDB** : _bulk_docs"
    )

st.divider()

# ---------------------------------------------------------------------------
st.subheader("Écriture en lot — détail par base")

fig_bulk = make_figure(df_agg, ops=["write_bulk"], height=420)
if fig_bulk:
    st.plotly_chart(fig_bulk, width="stretch")

st.subheader("Écriture ligne par ligne — détail par base")

fig_row = make_figure(df_agg, ops=["write_row_by_row"], height=420)
if fig_row:
    st.plotly_chart(fig_row, width="stretch")
    st.caption("L'overhead réseau/protocole par ligne est particulièrement visible sur CouchDB (1 requête HTTP / document).")

st.divider()

# ---------------------------------------------------------------------------
st.subheader("Données brutes — Écriture (médiane)")

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
st.download_button("⬇️ Exporter (CSV)", data=csv, file_name="ecriture.csv", mime="text/csv")