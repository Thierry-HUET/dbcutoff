# DB Cutoff Analyzer

Identification visuelle du point de rupture (cutoff) des bases de données
par comparaison de courbes volume/temps.

## Structure

```
db_cutoff/
├── bench_runner.py        # Orchestrateur du benchmark
├── bench_viz.py           # Dashboard Streamlit
├── config.py              # Paramètres & registre des DB
├── requirements.txt
├── storage/
│   ├── __init__.py
│   └── db_store.py        # SQLite — résultats & config
├── connectors/
│   ├── __init__.py
│   ├── base.py            # Classe abstraite DBConnector
│   └── postgres.py        # Connecteur PostgreSQL (psycopg v3)
└── loaders/
    ├── __init__.py
    └── insee_loader.py    # Lecture fichier INSEE
```

## Prérequis

```bash
pip install -r requirements.txt
```

Fichier source INSEE à placer dans `data/` ou via variable d'env :
```bash
export INSEE_FILE=/chemin/vers/StockEtablissementHistorique_utf8.csv
```

## Configuration PostgreSQL

```bash
export POSTGRES_DSN="postgresql://user:password@host:5432/db_cutoff"
```

Créer la base au préalable :
```sql
CREATE DATABASE db_cutoff;
```

## Lancer le benchmark

```bash
# Toutes les bases activées dans config.py
python bench_runner.py

# Une base spécifique
python bench_runner.py --db postgresql

# Volumes et répétitions personnalisés
python bench_runner.py --db postgresql --volumes 100,1000,10000,100000 --reps 5
```

## Visualiser les résultats

```bash
streamlit run bench_viz.py
```

## Ajouter un connecteur

1. Créer `connectors/mon_sgbd.py` héritant de `DBConnector`
2. Implémenter les 8 méthodes abstraites
3. Activer dans `config.py` → liste `DATABASES` :
   ```python
   { "name": "mon_sgbd", "module": "connectors.mon_sgbd", "enabled": True, "dsn": "..." }
   ```

## Opérations benchmarkées

| Opération            | Type      | Indexé |
|----------------------|-----------|--------|
| write_bulk           | Écriture  | Non    |
| write_row_by_row     | Écriture  | Non    |
| read_full            | Lecture   | Non    |
| read_filtered        | Lecture   | Non    |
| read_full_indexed    | Lecture   | Oui    |
| read_filtered_indexed| Lecture   | Oui    |