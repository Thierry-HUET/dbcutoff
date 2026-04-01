"""
bench_viz.py — Dashboard Streamlit : visualisation des résultats DB Cutoff

Lancement
---------
    streamlit run bench_viz.py
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from storage.db_store import init_storage, fetch_results, fetch_runs

# ---------------------------------------------------------------------------
# Config page
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="DB Cutoff Analyzer",
    page_icon="📊",
    layout="wide",
)

# Palette
PRIMARY = "#006699"
COLORS = [
    "#006699", "#00aacc", "#e07b00", "#cc3300",
    "#55aa55", "#9933cc", "#cc9900", "#006644",
]

# ---------------------------------------------------------------------------
# Traduction des noms d'opérations internes → libellés affichés
# ---------------------------------------------------------------------------
LABELS_OPERATIONS = {
    "write_bulk":            "Écriture en lot",
    "write_row_by_row":      "Écriture ligne par ligne",
    "read_full":             "Lecture complète (sans index)",
    "read_filtered":         "Lecture filtrée (sans index)",
    "read_full_indexed":     "Lecture complète (avec index)",
    "read_filtered_indexed": "Lecture filtrée (avec index)",
}

def label_op(code: str) -> str:
    return LABELS_OPERATIONS.get(code, code)

# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------
init_storage()

# ---------------------------------------------------------------------------
# Sidebar — filtres
# ---------------------------------------------------------------------------
st.sidebar.title("🔧 Filtres")

runs = fetch_runs()
run_options = {f"{r['run_id'][:8]}… | {r['db_name']} | {r['started_at']}": r["run_id"] for r in runs}

selected_run_label = st.sidebar.selectbox(
    "Session de benchmark",
    options=["— Toutes —"] + list(run_options.keys()),
)
selected_run_id = run_options.get(selected_run_label) if selected_run_label != "— Toutes —" else None

all_results = fetch_results()
df_all = pd.DataFrame(all_results)

if df_all.empty:
    st.title("📊 DB Cutoff Analyzer")
    st.info("Aucun résultat disponible. Lancez d'abord `bench_runner.py`.")
    st.stop()

if selected_run_id:
    df_all = df_all[df_all["run_id"] == selected_run_id]

# Filtres opérations — libellés français affichés, codes utilisés en interne
all_ops_codes = sorted(df_all["operation"].unique())
all_ops_labels = [label_op(op) for op in all_ops_codes]
label_to_code = dict(zip(all_ops_labels, all_ops_codes))

selected_ops_labels = st.sidebar.multiselect("Opérations", all_ops_labels, default=all_ops_labels)
selected_ops = [label_to_code[l] for l in selected_ops_labels]

# Filtres bases
all_dbs = sorted(df_all["db_name"].unique())
selected_dbs = st.sidebar.multiselect("Bases de données", all_dbs, default=all_dbs)

# Appliquer filtres
df = df_all[
    df_all["operation"].isin(selected_ops) &
    df_all["db_name"].isin(selected_dbs) &
    (df_all["duration_s"] >= 0)   # exclure erreurs (-1)
].copy()

# ---------------------------------------------------------------------------
# Agrégation : médiane par (db, operation, volume)
# ---------------------------------------------------------------------------
df_agg = (
    df.groupby(["db_name", "operation", "indexed", "volume"], as_index=False)
    .agg(duration_med=("duration_s", "median"), n=("duration_s", "count"))
)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("📊 DB Cutoff Analyzer")
st.caption("Identification visuelle du point de rupture des bases de données — source INSEE")

col1, col2, col3 = st.columns(3)
col1.metric("Mesures totales", len(df))
col2.metric("Bases testées", df["db_name"].nunique())
col3.metric("Opérations", df["operation"].nunique())

st.divider()

# ---------------------------------------------------------------------------
# Graphe principal : volume vs temps (échelle log/log)
# ---------------------------------------------------------------------------
st.subheader("Volume / Temps — toutes opérations")

fig = go.Figure()
color_map: dict[str, str] = {}
c_idx = 0

for (db, op), grp in df_agg.groupby(["db_name", "operation"]):
    key = f"{db} · {label_op(op)}"
    if key not in color_map:
        color_map[key] = COLORS[c_idx % len(COLORS)]
        c_idx += 1
    color = color_map[key]

    grp_sorted = grp.sort_values("volume")
    fig.add_trace(go.Scatter(
        x=grp_sorted["volume"],
        y=grp_sorted["duration_med"],
        mode="lines+markers",
        name=key,
        line=dict(color=color, width=2),
        marker=dict(size=6),
        hovertemplate=(
            f"<b>{key}</b><br>"
            "Volume : %{x:,} lignes<br>"
            "Durée (médiane) : %{y:.3f} s<extra></extra>"
        ),
    ))

fig.update_layout(
    xaxis=dict(
        title="Volume (lignes)",
        type="log",
        gridcolor="#e0e0e0",
        title_font=dict(color=PRIMARY),
    ),
    yaxis=dict(
        title="Durée médiane (s)",
        type="log",
        gridcolor="#e0e0e0",
        title_font=dict(color=PRIMARY),
    ),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    plot_bgcolor="white",
    paper_bgcolor="white",
    height=520,
    margin=dict(l=60, r=20, t=60, b=60),
    font=dict(family="Avenir, Helvetica Neue, sans-serif", color="#222"),
    hovermode="x unified",
)
st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Graphe comparatif : lecture avec vs sans index
# ---------------------------------------------------------------------------
st.subheader("Lecture : avec vs sans index")

df_read = df_agg[df_agg["operation"].str.startswith("read_")].copy()

if not df_read.empty:
    fig2 = go.Figure()
    c_idx2 = 0

    for (db, op), grp in df_read.groupby(["db_name", "operation"]):
        dash = "dash" if "indexed" in op else "solid"
        key = f"{db} · {label_op(op)}"
        color = COLORS[c_idx2 % len(COLORS)]
        c_idx2 += 1

        grp_sorted = grp.sort_values("volume")
        fig2.add_trace(go.Scatter(
            x=grp_sorted["volume"],
            y=grp_sorted["duration_med"],
            mode="lines+markers",
            name=key,
            line=dict(color=color, dash=dash, width=2),
            marker=dict(size=6),
            hovertemplate=(
                f"<b>{key}</b><br>"
                "Volume : %{x:,} lignes<br>"
                "Durée (médiane) : %{y:.3f} s<extra></extra>"
            ),
        ))

    fig2.update_layout(
        xaxis=dict(title="Volume (lignes)", type="log", gridcolor="#e0e0e0"),
        yaxis=dict(title="Durée médiane (s)", type="log", gridcolor="#e0e0e0"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=420,
        font=dict(family="Avenir, Helvetica Neue, sans-serif", color="#222"),
    )
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Aucune opération de lecture disponible dans les filtres actuels.")

# ---------------------------------------------------------------------------
# Graphe : écriture en lot vs ligne par ligne
# ---------------------------------------------------------------------------
st.subheader("Écriture : en lot vs ligne par ligne")

df_write = df_agg[df_agg["operation"].str.startswith("write_")].copy()

if not df_write.empty:
    fig3 = go.Figure()
    c_idx3 = 0

    for (db, op), grp in df_write.groupby(["db_name", "operation"]):
        color = COLORS[c_idx3 % len(COLORS)]
        c_idx3 += 1
        dash = "dot" if "row" in op else "solid"
        grp_sorted = grp.sort_values("volume")
        fig3.add_trace(go.Scatter(
            x=grp_sorted["volume"],
            y=grp_sorted["duration_med"],
            mode="lines+markers",
            name=f"{db} · {label_op(op)}",
            line=dict(color=color, dash=dash, width=2),
            marker=dict(size=6),
            hovertemplate=(
                f"<b>{db} · {label_op(op)}</b><br>"
                "Volume : %{x:,} lignes<br>"
                "Durée (médiane) : %{y:.3f} s<extra></extra>"
            ),
        ))

    fig3.update_layout(
        xaxis=dict(title="Volume (lignes)", type="log", gridcolor="#e0e0e0"),
        yaxis=dict(title="Durée médiane (s)", type="log", gridcolor="#e0e0e0"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=420,
        font=dict(family="Avenir, Helvetica Neue, sans-serif", color="#222"),
    )
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Aucune opération d'écriture disponible dans les filtres actuels.")

# ---------------------------------------------------------------------------
# Graphique vectoriel
# ---------------------------------------------------------------------------
st.subheader("Recherche vectorielle : exacte vs approximative")

df_vec = df_agg[df_agg["operation"].str.startswith("vector_")].copy()

if not df_vec.empty:
    fig_vec = go.Figure()
    c_idx_v = 0

    for (db, op), grp in df_vec.groupby(["db_name", "operation"]):
        couleur = COLORS[c_idx_v % len(COLORS)]
        c_idx_v += 1
        tiret = "dot" if "approx" in op else "solid"
        libelle = f"{db} · {label_op(op)}"
        grp_trie = grp.sort_values("volume")
        fig_vec.add_trace(go.Scatter(
            x=grp_trie["volume"],
            y=grp_trie["duration_med"],
            mode="lines+markers",
            name=libelle,
            line=dict(color=couleur, dash=tiret, width=2),
            marker=dict(size=6),
            hovertemplate=(
                f"<b>{libelle}</b><br>"
                "Volume : %{x:,} vecteurs<br>"
                "Durée (médiane) : %{y:.3f} s<extra></extra>"
            ),
        ))

    fig_vec.update_layout(
        xaxis=dict(title="Volume (vecteurs)", type="log", gridcolor="#e0e0e0"),
        yaxis=dict(title="Durée médiane (s)", type="log", gridcolor="#e0e0e0"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=420,
        font=dict(family="Avenir, Helvetica Neue, sans-serif", color="#222"),
    )
    st.plotly_chart(fig_vec, use_container_width=True)
    st.caption(
        "Ligne pleine = recherche exacte (brute force) · "
        "Ligne pointillée = recherche approximative (ANN / HNSW) · "
        "Dimension des vecteurs : 128"
    )
else:
    st.info("Aucune mesure vectorielle disponible. "
            "Relancer le benchmark sur une base compatible (PostgreSQL avec pgvector, DuckDB).")

# ---------------------------------------------------------------------------
# Tableau récapitulatif
# ---------------------------------------------------------------------------
st.subheader("Données brutes (médiane)")

df_tableau = df_agg.copy()
df_tableau["operation"] = df_tableau["operation"].map(label_op)
df_tableau = df_tableau.rename(columns={
    "db_name":      "Base de données",
    "operation":    "Opération",
    "indexed":      "Indexé",
    "volume":       "Volume (lignes)",
    "duration_med": "Durée médiane (s)",
    "n":            "Nb mesures",
})

st.dataframe(
    df_tableau.sort_values(["Base de données", "Opération", "Volume (lignes)"])
    .style.format({"Durée médiane (s)": "{:.4f}", "Volume (lignes)": "{:,}"}),
    use_container_width=True,
    hide_index=True,
)

# ---------------------------------------------------------------------------
# Téléchargement CSV
# ---------------------------------------------------------------------------
csv = df_tableau.to_csv(index=False).encode("utf-8")
st.download_button(
    "⬇️  Exporter les résultats (CSV)",
    data=csv,
    file_name="db_cutoff_results.csv",
    mime="text/csv",
)