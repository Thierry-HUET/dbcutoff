"""
loaders/insee_loader.py — Lecture et échantillonnage du fichier INSEE
StockEtablissementHistorique_utf8.csv (18 colonnes)
"""

import pandas as pd
from pathlib import Path
from config import INSEE_COLUMNS

# Mapping noms CSV → noms internes normalisés
_COL_MAP = {
    "siret":                           "siret",
    "dateDebut":                       "date_debut",
    "dateFin":                         "date_fin",
    "etatAdministratifEtablissement":  "etat_administratif",
    "enseigne1Etablissement":          "enseigne1",
    "activitePrincipaleEtablissement": "activite_principale",
    "caractereEmployeurEtablissement": "caractere_employeur",
}

_DATE_COLS = ["date_debut", "date_fin"]


def load_sample(filepath: str | Path, n_rows: int) -> pd.DataFrame:
    """
    Lit n_rows lignes du fichier INSEE et retourne un DataFrame normalisé.

    Paramètres
    ----------
    filepath : chemin vers StockEtablissementHistorique_utf8.csv
    n_rows   : nombre de lignes à charger (hors en-tête)

    Retour
    ------
    pd.DataFrame avec 7 colonnes normalisées, types cohérents.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(
            f"Fichier INSEE introuvable : {path}\n"
            "Définir INSEE_FILE ou placer le fichier dans data/"
        )

    df = pd.read_csv(
        path,
        usecols=INSEE_COLUMNS,
        nrows=n_rows,
        dtype=str,
        low_memory=False,
    )

    # Renommer vers noms internes
    df = df.rename(columns=_COL_MAP)

    # Conversion dates (format YYYY-MM-DD dans le fichier INSEE)
    for col in _DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Nettoyage : chaînes vides → None
    df = df.replace({"": None})

    return df