# DB Cutoff Analyzer

---

## 1. Objectif

Identifier visuellement le **point de rupture** (*cutoff*) de différentes bases de données par comparaison de courbes volume/temps.

Le point de rupture est le seuil à partir duquel les performances d'une base de données se dégradent de façon non linéaire — en lecture, en écriture, avec ou sans index, en lot ou ligne par ligne. L'identification est visuelle, par observation des inflexions sur des graphiques log/log volume vs durée.

Ce projet poursuit trois objectifs complémentaires :

- **Identifier le point de rupture** de chaque moteur en fonction du type d'opération et du volume de données
- **Aider à la sélection d'un moteur** adapté à un besoin donné, en comparant les profils de performance entre bases SQL et NoSQL
- **Aider au paramétrage d'un moteur** en comparant les résultats obtenus selon les configurations testées (index, mode d'écriture, volume de lot, support vectoriel)

**Bases de données ciblées**

| Base       | Type              | Connecteur   | Vectoriel         | Statut    |
|------------|-------------------|--------------|-------------------|-----------|
| PostgreSQL | SQL               | psycopg v3   | ✅ pgvector       | ✅ Actif  |
| DuckDB     | SQL colonnaire    | duckdb       | ✅ Natif          | ✅ Actif  |
| MongoDB    | NoSQL document    | pymongo      | ⚠️ Atlas only    | ✅ Actif  |
| MySQL      | SQL               | pymysql      | ⚠️ MySQL 9+ only | ✅ Actif  |
| CouchDB    | NoSQL document    | requests     | ❌ Non supporté   | ✅ Actif  |
| Neo4j      | NoSQL graphe      | —            | —                 | 🔜 Prévu |
| Cassandra  | NoSQL colonne     | —            | —                 | 🔜 Prévu |

**Source de données** : fichier `StockEtablissementHistorique_utf8.csv` produit par l'INSEE (~93 millions de lignes, 9,5 Go).

---

## 2. Prérequis et utilisation

### 2.1 Prérequis

#### Structure du projet

```
db_cutoff/
├── bench_runner.py        # Orchestrateur du benchmark
├── bench_viz.py           # Tableau de bord Streamlit
├── config.py              # Paramètres & registre des bases
├── requirements.txt       # Dépendances pip
├── pyproject.toml         # Dépendances Poetry
├── .env                   # Variables d'environnement (à créer depuis .env.example)
├── .env.example           # Modèle de configuration
├── storage/
│   └── db_store.py        # SQLite — persistance des résultats
├── connectors/
│   ├── base.py            # Classe abstraite DBConnector
│   ├── postgres.py        # Connecteur PostgreSQL (+ pgvector)
│   ├── duckdb.py          # Connecteur DuckDB (+ vecteurs natifs)
│   ├── mongodb.py         # Connecteur MongoDB
│   ├── mysql.py           # Connecteur MySQL / MariaDB
│   └── couchdb.py         # Connecteur CouchDB (API REST)
└── loaders/
    └── insee_loader.py    # Lecture du fichier INSEE par chunks
```

#### Dépendances Python

| Paquet              | Rôle                                    |
|---------------------|-----------------------------------------|
| `psycopg[binary]`   | Connecteur PostgreSQL v3                |
| `duckdb`            | Connecteur DuckDB                       |
| `pymongo`           | Connecteur MongoDB                      |
| `pymysql`           | Connecteur MySQL / MariaDB              |
| `requests`          | Connecteur CouchDB (API REST)           |
| `pandas`            | Manipulation des données                |
| `numpy`             | Génération des vecteurs de benchmark    |
| `streamlit`         | Tableau de bord de visualisation        |
| `plotly`            | Graphiques interactifs                  |
| `python-dotenv`     | Chargement du fichier `.env`            |

**Installation avec pip**

```bash
pip install -r requirements.txt
```

**Installation avec Poetry** (recommandé)

```bash
# Installer Poetry si nécessaire
curl -sSL https://install.python-poetry.org | python3 -

# Installer les dépendances du projet
poetry install

# Activer l'environnement virtuel
poetry shell

# Ou exécuter directement sans activation
poetry run ./bench_runner.py
poetry run streamlit run bench_viz.py
```

#### Services requis

- **PostgreSQL** ≥ 14 — serveur local ou distant
- **MongoDB** ≥ 6 — serveur local ou distant
- **MySQL** ≥ 8.0 ou MariaDB ≥ 10.5 — serveur local ou distant
- **CouchDB** ≥ 3.3 — serveur local ou distant
- **DuckDB** — aucun serveur requis (fichier local)

#### Prérequis vectoriels (PostgreSQL)

Le benchmark vectoriel PostgreSQL nécessite l'extension **pgvector** installée sur le serveur :

```bash
# macOS
brew install pgvector

# Debian / Ubuntu
apt install postgresql-pgvector

# Docker
# Utiliser l'image pgvector/pgvector à la place de postgres
```

L'extension est activée automatiquement par `bench_runner.py` si elle est disponible. Si elle est absente, le benchmark vectoriel est ignoré avec un message explicite — le benchmark scalaire continue normalement.

#### Fichier INSEE

Télécharger `StockEtablissementHistorique_utf8.csv` sur [data.gouv.fr](https://www.data.gouv.fr) et le placer dans `data/` ou définir `INSEE_FILE` dans `.env`.

#### Configuration `.env`

Copier `.env.example` vers `.env` et adapter les valeurs :

```bash
cp .env.example .env
```

```ini
# Fichier INSEE
INSEE_FILE=/chemin/vers/StockEtablissementHistorique_utf8.csv

# Volume maximum chargé en RAM (adapter selon la RAM disponible)
# 10 000 000 ≈ 2,7 Go | 20 000 000 ≈ 5,3 Go | 50 000 000 ≈ 13,4 Go
MAX_ROWS=10000000

# PostgreSQL
POSTGRES_DSN=postgresql://utilisateur@localhost:5432/db_cutoff

# DuckDB (chemin fichier ou :memory:)
DUCKDB_DSN=./data/cutoff.duckdb

# MongoDB (avec authentification si nécessaire)
MONGODB_DSN=mongodb://utilisateur:motdepasse@localhost:27017/db_cutoff?authSource=admin
# MongoDB en Replica Set local : ajouter directConnection=true
MONGODB_DIRECT_CONNECTION=true

# MySQL / MariaDB
MYSQL_DSN=mysql+pymysql://utilisateur:motdepasse@localhost:3306/db_cutoff

# CouchDB
COUCHDB_DSN=http://admin:admin@localhost:5984/db_cutoff
```

> La base de données est créée automatiquement par `bench_runner.py` si elle n'existe pas.

---

### 2.2 Utilisation

#### Lancer le benchmark

```bash
# Toutes les bases activées dans config.py
./bench_runner.py

# Une base spécifique
./bench_runner.py --db postgresql
./bench_runner.py --db duckdb
./bench_runner.py --db mongodb
./bench_runner.py --db mysql
./bench_runner.py --db couchdb

# Volumes et répétitions personnalisés
./bench_runner.py --db postgresql --volumes 100,1000,10000,100000 --reps 5

# Sélectionner une ou plusieurs opérations spécifiques
./bench_runner.py --db postgresql --ops read_full
./bench_runner.py --db postgresql --ops read_full,read_filtered

# Désactiver le benchmark vectoriel
./bench_runner.py --db postgresql --no-vector

# Combinaisons
./bench_runner.py --db postgresql --ops write_bulk --no-vector --reps 5
```

**Opérations disponibles pour `--ops`** :

| Valeur                  | Description                   |
|-------------------------|-------------------------------|
| `write_bulk`            | Écriture en lot               |
| `write_row_by_row`      | Écriture ligne par ligne      |
| `read_full`             | Lecture complète sans index   |
| `read_filtered`         | Lecture filtrée sans index    |
| `read_full_indexed`     | Lecture complète avec index   |
| `read_filtered_indexed` | Lecture filtrée avec index    |

#### Visualiser les résultats

```bash
streamlit run bench_viz.py
# ou avec Poetry :
poetry run streamlit run bench_viz.py
```

Le tableau de bord propose quatre graphiques log/log (toutes opérations, lecture, écriture, vectoriel) et un tableau récapitulatif exportable en CSV. Les sessions de benchmark sont sélectionnables dans la barre latérale pour comparer les exécutions.

#### Ajouter un connecteur

1. Créer `connectors/mon_sgbd.py` héritant de `DBConnector`
2. Implémenter les 12 méthodes obligatoires :

| Méthode                   | Rôle                                        |
|---------------------------|---------------------------------------------|
| `ensure_database()`       | Crée la base si absente                     |
| `connect()`               | Ouvre la connexion                          |
| `disconnect()`            | Ferme la connexion                          |
| `setup()`                 | Crée la table / collection                  |
| `teardown()`              | Supprime la table / collection              |
| `drop_database()`         | Supprime la base entière après le benchmark |
| `write_bulk()`            | Insertion en lot                            |
| `write_row_by_row()`      | Insertion ligne par ligne                   |
| `read_full()`             | Lecture complète                            |
| `read_filtered()`         | Lecture avec filtre                         |
| `read_full_indexed()`     | Lecture complète avec index                 |
| `read_filtered_indexed()` | Lecture filtrée avec index                  |

3. Implémenter optionnellement les méthodes vectorielles et activer `has_vector_support = True` :

| Méthode                | Rôle                                           |
|------------------------|------------------------------------------------|
| `vector_setup()`       | Crée la table vectorielle et l'extension       |
| `vector_teardown()`    | Supprime la table vectorielle                  |
| `vector_insert()`      | Insère n vecteurs normalisés (dim=128)         |
| `vector_search_exact()`| Recherche kNN exacte (brute force)             |
| `vector_search_approx()`| Recherche kNN approximative (ANN / HNSW)      |

4. Déclarer le connecteur dans `config.py` :

```python
{
    "name": "mon_sgbd",
    "module": "connectors.mon_sgbd",
    "enabled": True,
    "dsn": os.environ.get("MON_SGBD_DSN", "...valeur_par_defaut..."),
}
```

---

## 3. Mode opératoire

Le déroulement type d'une campagne de benchmark est le suivant :

1. Configurer `.env` (chemin INSEE, DSN des bases, `MAX_ROWS`)
2. Démarrer les services requis (PostgreSQL, MongoDB, MySQL, CouchDB)
3. Lancer `bench_runner.py` — la base est créée, testée puis supprimée automatiquement
4. Ouvrir le tableau de bord Streamlit pour analyser les courbes
5. Comparer les sessions entre bases ou entre configurations dans la barre latérale

Pour une comparaison rigoureuse entre bases, exécuter les benchmarks dans des conditions système identiques (charge CPU/RAM, même machine, sans autre processus concurrent).

**Exécutions parallèles** : il est possible de lancer deux bases différentes simultanément (ex: PostgreSQL + MongoDB) grâce au mode WAL de SQLite. En revanche, DuckDB verrouille son fichier en écriture exclusive et doit toujours être lancé seul.

---

## 4. Résultats attendus

### Forme des courbes

Sur un graphique log/log (volume en abscisse, durée en ordonnée) :

- **Comportement linéaire** : la base absorbe la montée en volume sans dégradation — la courbe est une droite
- **Point de rupture** : inflexion marquée où la pente s'accentue brutalement — la base commence à saturer (mémoire, I/O, verrous, etc.)
- **Plateau ou effondrement** : au-delà du cutoff, les temps explosent ou les requêtes échouent

### Comportements attendus par base

| Base       | Écriture en lot               | Lecture sans index            | Avec index                    |
|------------|-------------------------------|-------------------------------|-------------------------------|
| PostgreSQL | Très rapide (COPY protocol)   | Dégradation vers ~1M lignes   | Gain notable                  |
| DuckDB     | Très rapide (DataFrame natif) | Excellent (moteur colonnaire) | Gain limité (stats intégrées) |
| MongoDB    | Rapide (insert_many)          | Dégradation vers ~500k lignes | Gain fort sur lecture filtrée |
| MySQL      | Rapide (executemany)          | Dégradation vers ~500k lignes | Gain notable                  |
| CouchDB    | Modéré (_bulk_docs par chunks)| Lent (overhead HTTP par doc)  | Gain via index Mango           |

### Benchmark vectoriel

| Base       | Insertion          | Recherche exacte              | Recherche approx.             |
|------------|--------------------|-------------------------------|-------------------------------|
| PostgreSQL | COPY (rapide)      | Scan L2 (`<->`)               | Index HNSW (créé automatiquement) |
| DuckDB     | DataFrame natif    | `array_cosine_similarity()`   | SAMPLE 10% (pas d'ANN natif)  |

Le filtre de lecture utilise la valeur `F` (établissements fermés, ~5% des lignes) pour maximiser la sélectivité et rendre l'impact des index visible sur les courbes.

### Lecture du tableau de bord

- **Graphique global** : toutes les opérations superposées — permet de situer le cutoff relatif entre bases
- **Graphique lecture** : comparaison avec/sans index — ligne pleine = sans index, ligne pointillée = avec index
- **Graphique écriture** : lot vs ligne par ligne — l'écart entre les deux courbes mesure le coût du protocole
- **Graphique vectoriel** : insertion, recherche exacte et approx. — ligne pointillée = ANN

### Aide à la sélection d'un moteur

Les courbes permettent de répondre à des questions concrètes :

- *Mon cas d'usage est majoritairement en lecture filtrée sur grands volumes* → comparer les courbes `Lecture filtrée` entre bases
- *Je dois ingérer de grands volumes rapidement* → comparer les courbes `Écriture en lot`
- *J'ai besoin de recherche sémantique / similarité vectorielle* → comparer les courbes vectorielles PostgreSQL vs DuckDB
- *Mon volume de données restera sous 100k lignes* → tous les moteurs sont équivalents, le cutoff n'est pas le critère discriminant

### Aide au paramétrage

La comparaison entre sessions permet d'évaluer l'impact de paramètres spécifiques :

- Activation ou non d'un index sur la colonne filtrée
- Paramètres serveur PostgreSQL (`max_wal_size`, `shared_buffers`) en modifiant la config entre deux runs
- Dimension des vecteurs (modifier `VECTOR_DIM` dans `base.py`)

---

## 5. Méthodologie

### Protocole de mesure

Chaque mesure correspond à une opération isolée sur un volume donné, répétée `REPETITIONS` fois (défaut : 3). La **médiane** est retenue pour atténuer les pics liés au système (cache OS, GC, etc.).

Le cycle complet pour chaque base :

```
ensure_database() → connect() → setup() → [benchmark scalaire]
→ [benchmark vectoriel si supporté] → teardown() → disconnect() → drop_database()
```

La base est **supprimée après chaque session** pour garantir des conditions identiques entre les runs.

### Opérations scalaires benchmarkées

| Code interne            | Libellé affiché                | Type     | Indexé |
|-------------------------|--------------------------------|----------|--------|
| `write_bulk`            | Écriture en lot                | Écriture | Non    |
| `write_row_by_row`      | Écriture ligne par ligne       | Écriture | Non    |
| `read_full`             | Lecture complète (sans index)  | Lecture  | Non    |
| `read_filtered`         | Lecture filtrée (sans index)   | Lecture  | Non    |
| `read_full_indexed`     | Lecture complète (avec index)  | Lecture  | Oui    |
| `read_filtered_indexed` | Lecture filtrée (avec index)   | Lecture  | Oui    |

### Opérations vectorielles benchmarkées

| Code interne            | Libellé affiché                          | Index   |
|-------------------------|------------------------------------------|---------|
| `vector_insert`         | Insertion vectorielle                    | Non     |
| `vector_search_exact`   | Recherche vectorielle exacte (brute force)| Non    |
| `vector_search_approx`  | Recherche vectorielle approx. (ANN)      | HNSW    |

Vecteurs générés aléatoirement, normalisés (norme L2 = 1), dimension 128. La graine (`seed=42`) est fixe pour garantir la reproductibilité entre runs.

### Optimisations appliquées par connecteur

**PostgreSQL**
- `autovacuum_enabled = false` sur les tables de benchmark : les rechargements répétés (`DELETE FROM` + réinsertion) génèrent des tuples morts qui déclenchent l'autovacuum en cours de mesure et polluent les temps
- DDL (`CREATE/DROP TABLE`, `CREATE/DROP INDEX`) exécutés en `autocommit` pour éviter les deadlocks avec les verrous de transaction psycopg v3
- `DELETE FROM` à la place de `TRUNCATE` (qui pose un `AccessExclusiveLock` incompatible avec les index)
- Vérification automatique de la disponibilité de pgvector avant le benchmark vectoriel

**Filtre de lecture**
- Valeur filtrée : `etat_administratif = 'F'` (établissements fermés, ~5% des lignes)
- Raison : haute sélectivité → PostgreSQL et MongoDB utilisent l'index ; avec `'A'` (95%), un scan séquentiel serait préféré même avec index, rendant les courbes avec/sans index identiques

### Chargement des données

Le fichier INSEE est lu **une seule fois** par chunks de 500 000 lignes jusqu'au volume maximum (`MAX_ROWS`). Les sous-volumes sont extraits par `df.head(n)` sans relire le fichier. Seules 7 colonnes sur 18 sont retenues :

| Colonne source (INSEE)            | Colonne interne       |
|-----------------------------------|-----------------------|
| `siret`                           | `siret` (clé, indexée)|
| `dateDebut`                       | `date_debut`          |
| `dateFin`                         | `date_fin`            |
| `etatAdministratifEtablissement`  | `etat_administratif`  |
| `enseigne1Etablissement`          | `enseigne1`           |
| `activitePrincipaleEtablissement` | `activite_principale` |
| `caractereEmployeurEtablissement` | `caractere_employeur` |

### Persistance des résultats

Tous les résultats sont stockés dans une base **SQLite locale** (`storage/results.db`) indépendante des bases benchmarkées. Chaque session reçoit un identifiant UUID unique. Les résultats sont conservés entre les runs et comparables dans le tableau de bord.

### Paramètres ajustables

| Paramètre         | Défaut       | Description                             |
|-------------------|--------------|-----------------------------------------|
| `MAX_ROWS`        | 10 000 000   | Volume maximum chargé en RAM            |
| `VOLUMES`         | 100 → 10M    | Paliers testés (filtrés par `MAX_ROWS`) |
| `REPETITIONS`     | 3            | Répétitions par mesure                  |
| `TIMEOUT_SECONDS` | 600          | Timeout par opération                   |
| `VECTOR_DIM`      | 128          | Dimension des vecteurs de benchmark     |
| `--ops`           | toutes       | Filtre les opérations scalaires         |
| `--no-vector`     | désactivé    | Ignore le benchmark vectoriel           |