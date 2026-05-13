"""
Chargement des données source pour les tests W1
Toutes les sources sont au format Parquet.

Sources configurées dans .env :
  INSEE_FILE  — StockEtablissementHistorique (Parquet)
  AFNIC_FILE  — domaines (Parquet)
  BODACC_FILE — annonces commerciales (Parquet)
"""
from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

MAX_ROWS: int = int(os.getenv("MAX_ROWS", "10000").replace("_", ""))
VECTOR_MAX_ROWS: int = int(os.getenv("VECTOR_MAX_ROWS", "10000").replace("_", ""))

SOURCE_MAP = {
    "insee":  os.getenv("INSEE_FILE", ""),
    "afnic":  os.getenv("AFNIC_FILE", ""),
    "bodacc": os.getenv("BODACC_FILE", ""),
}

DataSource = Literal["insee", "afnic", "bodacc"]


def _check(path: str, source: str) -> Path:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Source '{source}' introuvable : {path}")
    return p


def _sanitize(value: Any) -> Any:
    """Convertit toute valeur non JSON-serialisable en None."""
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _sanitize_record(record: dict) -> dict:
    return {k: _sanitize(v) for k, v in record.items()}


def load(source: DataSource, max_rows: int | None = None) -> list[dict]:
    """
    Charge les données d'une source Parquet et retourne une list[dict].

    Args:
        source   : "insee" | "afnic" | "bodacc"
        max_rows : nombre de lignes max (défaut : MAX_ROWS ou VECTOR_MAX_ROWS)

    Returns:
        list[dict] de longueur <= max_rows, toutes valeurs JSON-serialisables
    """
    if source not in SOURCE_MAP:
        raise ValueError(f"Source inconnue : {source}. Valeurs valides : {list(SOURCE_MAP)}")

    path_str = SOURCE_MAP[source]
    if not path_str:
        raise EnvironmentError(f"Variable d'environnement manquante pour la source '{source}'")

    path = _check(path_str, source)

    if max_rows is None:
        max_rows = VECTOR_MAX_ROWS if source == "afnic" else MAX_ROWS

    df = pd.read_parquet(path)
    df = df.head(max_rows)

    # Convertit toutes les colonnes objet en str pour éviter les types numpy
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).where(df[col].notna(), None)

    records = df.to_dict(orient="records")
    return [_sanitize_record(r) for r in records]


def sample_ids(records: list[dict], n: int = 200) -> list[str]:
    """
    Extrait n identifiants depuis une liste de records.
    Cherche dans l'ordre : 'id', 'siren', 'siret', première clé disponible.
    """
    if not records:
        return []
    id_keys = ["id", "siren", "siret"]
    key = next((k for k in id_keys if k in records[0]), None)
    if key is None:
        key = next(iter(records[0]))
    return [str(r[key]) for r in records[:n] if r.get(key) is not None]
