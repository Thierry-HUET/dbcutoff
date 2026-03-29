"""
loaders/insee_loader.py — Lecture optimisée du fichier INSEE
StockEtablissementHistorique_utf8.csv (93M lignes, ~9.5 Go)

Stratégie :
    - Lecture unique du fichier jusqu'à MAX_ROWS via chunks pandas
    - Typage minimal (str) à la lecture pour réduire la RAM
    - Conversion dates uniquement sur le DataFrame final
    - Les sous-volumes sont extraits par df.head(n) sans relire le fichier
"""

import logging
import pandas as pd
from pathlib import Path
from config import INSEE_COLUMNS

log = logging.getLogger(__name__)

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

# Taille des chunks de lecture (lignes) — équilibre RAM / overhead itération
_CHUNK_SIZE = 500_000


def load_sample(filepath: str | Path, n_rows: int) -> pd.DataFrame:
    """
    Lit jusqu'à n_rows lignes du fichier INSEE en une passe unique via chunks.

    Paramètres
    ----------
    filepath : chemin vers StockEtablissementHistorique_utf8.csv
    n_rows   : nombre de lignes à charger (hors en-tête)

    Retour
    ------
    pd.DataFrame avec 7 colonnes normalisées.

    Notes
    -----
    - Lecture par chunks de 500k lignes pour maîtriser le pic RAM
    - Utiliser df.head(k) sur le résultat pour obtenir un sous-volume
      sans relire le fichier
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(
            f"Fichier INSEE introuvable : {path}\n"
            "Définir INSEE_FILE dans .env ou placer le fichier dans data/"
        )

    log.info("Lecture INSEE : %d lignes demandées (chunks de %s)…",
             n_rows, f"{_CHUNK_SIZE:,}")

    chunks = []
    rows_read = 0

    reader = pd.read_csv(
        path,
        usecols=INSEE_COLUMNS,
        dtype=str,          # lecture brute str — conversion différée
        chunksize=_CHUNK_SIZE,
        low_memory=False,
    )

    for chunk in reader:
        remaining = n_rows - rows_read
        if remaining <= 0:
            break

        if len(chunk) > remaining:
            chunk = chunk.iloc[:remaining]

        chunks.append(chunk)
        rows_read += len(chunk)

        if rows_read % 2_000_000 == 0:
            log.info("  … %s lignes lues", f"{rows_read:,}")

    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    log.info("Lecture terminée : %s lignes chargées.", f"{len(df):,}")

    # Renommer
    df = df.rename(columns=_COL_MAP)

    # Conversion dates (format YYYY-MM-DD dans le fichier INSEE)
    for col in _DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Nettoyage : chaînes vides → None
    df = df.replace({"": None})

    return df