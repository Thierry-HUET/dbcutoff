# Spécification fonctionnelle — DB Benchmarker

Niveaux d'exigence (RFC 2119) :
- **MUST** — exigence absolue, non négociable
- **SHALL** — exigence forte, dérogation possible sur justification documentée
- **SHOULD** — recommandé par défaut, écart acceptable si justifié
- **MAY** — optionnel, laissé à l'appréciation de l'implémenteur
- **MUST NOT** — interdiction absolue

---

## 0. Paramétrage de session Claude

### 0.1 Alertes

- Claude **MUST** alerter dans la discussion lorsque la consommation de tokens atteint 80% de la fenêtre de contexte

### 0.2 Comportement

- Claude **MUST** désactiver l'explicabilité par défaut (ne pas narrer ni résumer ses actions)
- Claude **MAY** réactiver l'explicabilité sur demande explicite ("explique ce que tu fais")
- Claude **MUST NOT** remercier, féliciter ou produire de formules de politesse superflues
- Claude **MUST NOT** résumer ce qu'il vient de faire sauf demande explicite

---

## 1. Objectif

Comparer les performances de bases de données hétérogènes (SQL, NoSQL, vectorielles, graphe, KV) sur des jeux de données réels, afin de déterminer dans quels contextes d'usage chaque base est pertinente.

Les critères d'évaluation **MUST** couvrir :
- la scalabilité sur de gros volumes (jusqu'à 93M de lignes)
- la résistance à la concurrence (requêtes simultanées)
- les performances selon le type de requête (relationnelle, vectorielle, graphe, etc.)

---

## 2. Architecture générale

### 2.1 Vue d'ensemble

```
[Données Parquet]
       │
       ▼
[Serveur de test]  ◄──►  [Interface Streamlit]
       │
       ▼
[GraphQL par DB]  ◄──►  [Conteneur DB (Docker)]
       │
       ▼
[SQLite — stockage des résultats]
```

### 2.2 Composants

| Composant | Rôle | Technologie |
|---|---|---|
| Interface utilisateur | Lancer les tests, visualiser les résultats | Streamlit |
| Serveur de test | Orchestration des scénarios, collecte des métriques | Python / Poetry |
| Adaptateur GraphQL | Couche d'abstraction uniforme entre serveur de test et chaque DB | Un serveur GraphQL dédié par DB |
| Bases de données | Cibles des benchmarks | Docker (sidecar) |
| Stockage des résultats | Persistance des métriques | SQLite |

### 2.3 Environnement

- Le projet **MUST** utiliser Poetry comme gestionnaire de dépendances
- Chaque base de données **MUST** tourner dans un conteneur Docker isolé
- Chaque conteneur DB **MUST** être associé à son propre serveur GraphQL (pattern sidecar)
- L'ensemble des services **SHALL** être orchestré via Docker Compose
- Le projet **MUST NOT** dépendre d'une installation locale d'une base de données en dehors de Docker

---

## 3. Données de test

| Fichier | Volume | Usage principal |
|---|---|---|
| StockInsee.parquet | ~93M lignes | Tests relationnels, graphe, lecture/écriture |
| bodacc.parquet | ~7M lignes | Tests relationnels |
| domaines.parquet | ~9M lignes | Tests vectoriels, clé-valeur |

- StockInsee et bodacc **MUST** être liés via le champ `siren` (clé commune)
- Les fichiers source **MUST** être au format Parquet
- L'ingestion **SHALL** utiliser pandas ou polars pour la lecture Parquet
- Les fichiers de données **MUST NOT** être versionnés dans le dépôt git

---

## 4. Bases de données testées

| Base | Catégorie | Type de tests applicables |
|---|---|---|
| PostgreSQL | SQL relationnel | Lecture/écriture, relationnel, spatial |
| MariaDB | SQL relationnel | Lecture/écriture, relationnel |
| MySQL | SQL relationnel | Lecture/écriture, relationnel |
| DuckDB | SQL analytique | Lecture/écriture, analytique (OLAP) |
| CrateDB | SQL distribué | Lecture/écriture, relationnel, full-text |
| MongoDB | Document | Lecture/écriture, document |
| CouchDB | Document | Lecture/écriture, document |
| ArangoDB | Multi-modèle | Relationnel, graphe, document |
| Neo4j | Graphe | Graphe |
| Cassandra | Colonne large | Lecture/écriture, concurrence élevée |
| Valkey | Clé-valeur | Lecture/écriture, KV, cache |
| Elasticsearch | Recherche / full-text | Recherche, vectoriel |
| Milvus | Vectoriel | Vectoriel |
| Qdrant | Vectoriel | Vectoriel |

- Chaque base **MUST** exposer un adaptateur GraphQL conforme au schéma défini en section 9
- L'ajout d'une nouvelle base **SHALL** suivre le même pattern sidecar sans modification du serveur de test
- Une base **MAY** être exclue d'un run si son conteneur est absent ou désactivé

---

## 5. Catalogue de tests

### 5.1 Tests universels (toutes les bases)

Ces tests **MUST** être exécutés sur chaque base sans exception.

| Code | Description | Niveau |
|---|---|---|
| W1 | Insertion en masse (bulk insert) — ingestion du jeu de données complet | **MUST** |
| W2 | Écriture unitaire — insertion d'un enregistrement unique, mesure de latence | **MUST** |
| R1 | Lecture par clé primaire / identifiant unique | **MUST** |
| R2 | Lecture filtrée — condition simple sur un champ indexé | **MUST** |
| R3 | Lecture filtrée — condition sur un champ non indexé | **MUST** |
| R4 | Comptage global (`COUNT *`) | **MUST** |
| C1 | Concurrence — N requêtes R1 simultanées (N = 10, 50, 100) | **MUST** |

### 5.2 Tests relationnels

Bases concernées : PostgreSQL, MariaDB, MySQL, DuckDB, CrateDB, ArangoDB

| Code | Description | Niveau |
|---|---|---|
| REL1 | Jointure simple StockInsee ↔ bodacc sur `siren` | **MUST** |
| REL2 | Jointure avec agrégation (GROUP BY, COUNT, SUM) | **MUST** |
| REL3 | Requête imbriquée (sous-requête ou CTE) | **SHALL** |
| REL4 | Jointure sous concurrence (N = 10, 50) | **SHOULD** |

### 5.3 Tests graphe

Bases concernées : Neo4j, ArangoDB

| Code | Description | Niveau |
|---|---|---|
| GR1 | Traversée en profondeur 1 (voisins directs) | **MUST** |
| GR2 | Traversée en profondeur 2 | **MUST** |
| GR3 | Calcul du chemin le plus court entre deux nœuds | **SHALL** |
| GR4 | Détection de communautés | **MAY** |

### 5.4 Tests vectoriels

Bases concernées : Milvus, Qdrant, Elasticsearch

| Code | Description | Niveau |
|---|---|---|
| VEC1 | Insertion de vecteurs (domaines.parquet, encodage à préciser) | **MUST** |
| VEC2 | Recherche KNN (K = 10) | **MUST** |
| VEC3 | Recherche KNN sous concurrence (N = 10, 50) | **SHALL** |
| VEC4 | Recherche hybride (vecteur + filtre scalaire) | **MAY** |

### 5.5 Tests clé-valeur

Bases concernées : Valkey

| Code | Description | Niveau |
|---|---|---|
| KV1 | SET unitaire | **MUST** |
| KV2 | GET unitaire | **MUST** |
| KV3 | SET/GET sous concurrence (N = 100, 500) | **MUST** |
| KV4 | Expiration de clé (TTL) | **SHOULD** |

### 5.6 Tests analytiques (OLAP)

Bases concernées : DuckDB, CrateDB

| Code | Description | Niveau |
|---|---|---|
| OLAP1 | Agrégation sur colonne numérique (SUM, AVG, MIN, MAX) table complète | **MUST** |
| OLAP2 | GROUP BY multi-colonnes | **MUST** |
| OLAP3 | Fenêtrage (WINDOW FUNCTION) | **MAY** |

---

## 6. Métriques collectées

Le serveur de test **MUST** collecter les métriques suivantes pour chaque exécution :

| Métrique | Unité | Niveau |
|---|---|---|
| latence_p50 | ms | **MUST** |
| latence_p95 | ms | **MUST** |
| latence_p99 | ms | **MUST** |
| throughput | req/s | **MUST** |
| durée_totale | s | **MUST** |
| erreurs | count | **MUST** |
| concurrence | int | **MUST** (si applicable) |
| volume_lignes | int | **SHOULD** |

- Les métriques **MUST** être calculées côté serveur de test, pas côté GraphQL
- Le serveur de test **MUST NOT** agréger les métriques avant persistance — les valeurs brutes **SHALL** être stockées

---

## 7. Stockage des résultats

- Les résultats **MUST** être persistés dans une base SQLite locale (`results.db`)
- Chaque exécution de test **MUST** produire une ligne dans la table `results`
- Le fichier `results.db` **SHOULD** être exclu du versionnement git

Schéma **MUST** :

```sql
CREATE TABLE results (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL,     -- identifiant de session de benchmark
    db_name     TEXT NOT NULL,     -- ex. "postgresql", "neo4j"
    test_code   TEXT NOT NULL,     -- ex. "R1", "REL2", "VEC3"
    concurrence INTEGER,
    volume      INTEGER,
    latence_p50 REAL,
    latence_p95 REAL,
    latence_p99 REAL,
    throughput  REAL,
    duree_s     REAL,
    erreurs     INTEGER,
    timestamp   TEXT NOT NULL
);
```

- Le `run_id` **SHALL** être un UUID v4 généré à chaque lancement de session
- La table **MAY** être étendue sans rupture de compatibilité (colonnes additionnelles)

---

## 8. Interface Streamlit

### 8.1 Exigences générales

- L'interface **MUST** permettre de lancer les tests sans passer par la CLI
- L'interface **MUST** afficher la progression en temps réel pendant l'exécution
- L'interface **MUST** permettre la visualisation comparative des résultats
- L'interface **SHALL** permettre la comparaison entre plusieurs runs historiques
- L'interface **MUST NOT** bloquer pendant l'exécution d'un test (pas de freeze UI)

### 8.2 Pages

| Page | Contenu | Niveau |
|---|---|---|
| Configuration | Sélection des DBs, tests, niveau de concurrence | **MUST** |
| Exécution | Lancement, logs en direct, barre de progression | **MUST** |
| Résultats | Graphiques comparatifs par métrique et par test | **MUST** |
| Historique | Tableau des runs passés, comparaison entre runs | **SHALL** |

### 8.3 Visualisations

- La page Résultats **MUST** proposer au minimum un graphique en barres comparant les latences p50/p95/p99 par base
- La page Résultats **SHOULD** proposer un graphique de throughput par test
- La page Résultats **MAY** proposer un radar chart synthétique par base de données

---

## 9. Couche GraphQL

### 9.1 Exigences

- Chaque base de données **MUST** disposer de son propre serveur GraphQL dédié
- Le schéma GraphQL exposé **MUST** être identique pour toutes les bases
- Le serveur de test **MUST** interagir exclusivement via GraphQL — aucun driver DB direct n'est autorisé
- Chaque adaptateur **MUST** traduire les requêtes GraphQL vers le langage natif de sa DB (SQL, Cypher, MQL, AQL, etc.)
- Un adaptateur **MUST NOT** exposer des opérations spécifiques à une seule DB dans le schéma commun

### 9.2 Schéma minimal obligatoire

```graphql
type Query {
  getById(id: String!): Record          # MUST
  filter(field: String!, value: String!, limit: Int): [Record]  # MUST
  count: Int                            # MUST
  search(query: String!, limit: Int): [Record]  # SHOULD
}

type Mutation {
  insertOne(data: JSON!): Boolean       # MUST
  insertBulk(data: [JSON!]!): Int       # MUST
}
```

- Les opérations supplémentaires (ex. traversée de graphe, KNN) **MAY** être ajoutées dans un schéma étendu par adaptateur
- Le schéma étendu **MUST NOT** casser la compatibilité avec le schéma commun

---

## 10. To Do List

- [ ] Confirmer le volume exact de bodacc.parquet
- [ ] Définir le modèle d'embedding pour domaines.parquet (vectoriel)
- [ ] Spécifier le schéma GraphQL étendu (opérations spécifiques par type de DB)
- [ ] Préciser les seuils de concurrence définitifs par catégorie de test
- [ ] Valider la liste des 14 bases (exclusions éventuelles à documenter)
- [ ] Spécifier les tests spatiaux PostGIS (PostgreSQL)
- [ ] Définir la stratégie de warm-up avant mesure (éviter les biais de cold start)
