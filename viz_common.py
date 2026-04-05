"""
viz_common.py — Fonctions et constantes partagées entre les pages Streamlit
"""

import pandas as pd
import plotly.graph_objects as go

from storage.db_store import init_storage, fetch_results, fetch_runs

# ---------------------------------------------------------------------------
# Catégorisation des bases (conservée pour la synthèse)
# ---------------------------------------------------------------------------
SQL_DBS   = {"postgresql", "duckdb", "mysql"}
NOSQL_DBS = {"mongodb", "couchdb", "neo4j", "cassandra"}

def db_category(db_name: str) -> str:
    if db_name in SQL_DBS:
        return "SQL"
    if db_name in NOSQL_DBS:
        return "NoSQL"
    return "Autre"

# ---------------------------------------------------------------------------
# Couleur fixe par base
# ---------------------------------------------------------------------------
DB_COLORS: dict[str, str] = {
    "postgresql": "#006699",
    "duckdb":     "#e07b00",
    "mongodb":    "#55aa55",
    "mysql":      "#cc3300",
    "couchdb":    "#9933cc",
    "neo4j":      "#cc9900",
    "cassandra":  "#006644",
}
_FALLBACK = ["#00aacc", "#e05599", "#44aaaa", "#aa6600", "#4466cc"]

def db_color(db_name: str, seen: dict) -> str:
    if db_name in DB_COLORS:
        return DB_COLORS[db_name]
    if db_name not in seen:
        seen[db_name] = _FALLBACK[len(seen) % len(_FALLBACK)]
    return seen[db_name]

# ---------------------------------------------------------------------------
# Style de ligne / marqueur par opération
# ---------------------------------------------------------------------------
OP_DASH: dict[str, str] = {
    "write_bulk":             "solid",
    "write_row_by_row":       "dot",
    "read_full":              "solid",
    "read_filtered":          "dash",
    "read_full_indexed":      "longdash",
    "read_filtered_indexed":  "dashdot",
    "vector_insert":          "solid",
    "vector_search_exact":    "dash",
    "vector_search_approx":   "dot",
}
OP_SYMBOL: dict[str, str] = {
    "write_bulk":             "circle",
    "write_row_by_row":       "square",
    "read_full":              "circle",
    "read_filtered":          "diamond",
    "read_full_indexed":      "triangle-up",
    "read_filtered_indexed":  "cross",
    "vector_insert":          "circle",
    "vector_search_exact":    "diamond",
    "vector_search_approx":   "square",
}

# ---------------------------------------------------------------------------
# Traduction des opérations
# ---------------------------------------------------------------------------
LABELS_OPERATIONS: dict[str, str] = {
    "write_bulk":             "Écriture en lot",
    "write_row_by_row":       "Écriture ligne par ligne",
    "read_full":              "Lecture complète (sans index)",
    "read_filtered":          "Lecture filtrée (sans index)",
    "read_full_indexed":      "Lecture complète (avec index)",
    "read_filtered_indexed":  "Lecture filtrée (avec index)",
    "vector_insert":          "Insertion vectorielle",
    "vector_search_exact":    "Recherche vectorielle exacte",
    "vector_search_approx":   "Recherche vectorielle approx. (ANN)",
}

def label_op(code: str) -> str:
    return LABELS_OPERATIONS.get(code, code)

# ---------------------------------------------------------------------------
# Mise en page commune des graphiques
# ---------------------------------------------------------------------------
BASE_LAYOUT = dict(
    xaxis=dict(type="log", gridcolor="#e0e0e0"),
    yaxis=dict(type="log", gridcolor="#e0e0e0"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    plot_bgcolor="white",
    paper_bgcolor="white",
    font=dict(family="Avenir, Helvetica Neue, sans-serif", color="#222"),
)

def make_trace(db: str, op: str, grp: pd.DataFrame, unknown: dict) -> go.Scatter:
    """Trace Scatter avec couleur fixe par base et style par opération."""
    libelle = f"{db} · {label_op(op)}"
    color   = db_color(db, unknown)
    tri     = grp.sort_values("volume")
    return go.Scatter(
        x=tri["volume"],
        y=tri["duration_med"],
        mode="lines+markers",
        name=libelle,
        line=dict(color=color, dash=OP_DASH.get(op, "solid"), width=2),
        marker=dict(size=6, symbol=OP_SYMBOL.get(op, "circle"), color=color),
        hovertemplate=(
            f"<b>{libelle}</b><br>"
            "Volume : %{x:,}<br>"
            "Durée (médiane) : %{y:.3f} s<extra></extra>"
        ),
    )

def make_figure(
    df_agg: pd.DataFrame,
    ops: list[str] | None = None,
    x_label: str = "Volume (lignes)",
    y_label: str = "Durée médiane (s)",
    height: int = 460,
) -> go.Figure | None:
    """
    Construit un graphique log/log pour les opérations listées dans ops.
    ops=None → toutes les opérations.
    Retourne None si aucune donnée.
    """
    subset = (
        df_agg[df_agg["operation"].isin(ops)].copy()
        if ops
        else df_agg.copy()
    )
    if subset.empty:
        return None

    fig = go.Figure()
    unknown: dict = {}
    for (db, op), grp in subset.groupby(["db_name", "operation"]):
        fig.add_trace(make_trace(db, op, grp, unknown))

    fig.update_layout(
        **BASE_LAYOUT,
        xaxis_title=x_label,
        yaxis_title=y_label,
        height=height,
        hovermode="x unified",
    )
    return fig

# ---------------------------------------------------------------------------
# Chargement et agrégation des données
# ---------------------------------------------------------------------------

def load_data(run_id: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Charge et agrège les résultats depuis SQLite. Retourne (df_raw, df_agg)."""
    init_storage()
    rows = fetch_results()
    df = pd.DataFrame(rows)
    if df.empty:
        return df, df

    if run_id:
        df = df[df["run_id"] == run_id]

    df = df[df["duration_s"] >= 0].copy()
    df["categorie"] = df["db_name"].map(db_category)

    df_agg = (
        df.groupby(["db_name", "operation", "indexed", "volume", "categorie"], as_index=False)
        .agg(duration_med=("duration_s", "median"), n=("duration_s", "count"))
    )
    return df, df_agg

def load_runs() -> dict[str, str]:
    """Retourne un dict label → run_id pour la selectbox."""
    runs = fetch_runs()
    return {
        f"{r['run_id'][:8]}… | {r['db_name']} | {r['started_at']}": r["run_id"]
        for r in runs
    }

def render_sidebar(df_all: pd.DataFrame, page_title: str) -> tuple[list, list]:
    """Filtres communs : opérations + bases. Retourne (sel_ops, sel_dbs)."""
    import streamlit as st

    st.sidebar.title(f"🔧 Filtres — {page_title}")

    all_ops_codes  = sorted(df_all["operation"].unique())
    all_ops_labels = [label_op(op) for op in all_ops_codes]
    label_to_code  = dict(zip(all_ops_labels, all_ops_codes))

    sel_labels = st.sidebar.multiselect("Opérations", all_ops_labels, default=all_ops_labels)
    sel_ops    = [label_to_code[l] for l in sel_labels]

    all_dbs = sorted(df_all["db_name"].unique())
    sel_dbs = st.sidebar.multiselect("Bases de données", all_dbs, default=all_dbs)

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Couleur par base**")
    _unk: dict = {}
    for db in all_dbs:
        c = db_color(db, _unk)
        st.sidebar.markdown(
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{c};border-radius:3px;margin-right:6px"></span>{db}',
            unsafe_allow_html=True,
        )

    return sel_ops, sel_dbs