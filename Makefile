# =============================================================================
# DB Benchmarker — Makefile (gmake)
# =============================================================================
SHELL := /bin/bash
.DEFAULT_GOAL := help

SIDECAR_POSTGRES_URL := http://localhost:8001/graphql
TEST_SERVER_URL      := http://localhost:5400
SIDECAR_PORT         := 8001
TEST_SERVER_PORT     := 5400
SOURCE               := insee
TESTS                :=
CONCURRENCE          := 10

# -----------------------------------------------------------------------------
# Aide
# -----------------------------------------------------------------------------
.PHONY: help
help:
	@echo ""
	@echo "  DB Benchmarker"
	@echo ""
	@echo "  Infrastructure"
	@echo "    make up              Lance PostgreSQL (Docker)"
	@echo "    make down            Arrête PostgreSQL"
	@echo "    make down-v          Arrête PostgreSQL + supprime les volumes"
	@echo "    make ps              État des conteneurs"
	@echo ""
	@echo "  Dépendances"
	@echo "    make lock            Régénère poetry.lock"
	@echo "    make install         Installe les dépendances Poetry"
	@echo ""
	@echo "  Sidecar PostgreSQL"
	@echo "    make sidecar         Lance le sidecar PostgreSQL (port $(SIDECAR_PORT))"
	@echo "    make sidecar-health  Vérifie l'état du sidecar"
	@echo ""
	@echo "  Serveur de test"
	@echo "    make server          Lance le serveur de test Flask (port $(TEST_SERVER_PORT))"
	@echo "    make server-health   Vérifie l'état du serveur"
	@echo "    make server-tests    Liste les tests disponibles"
	@echo ""
	@echo "  Benchmark"
	@echo "    make run             Lance tous les tests (source=$(SOURCE))"
	@echo "    make run TESTS=W1,R1,R4  Lance des tests spécifiques"
	@echo "    make run SOURCE=afnic    Change la source de données"
	@echo "    make run CONCURRENCE=50  Change le niveau de concurrence"
	@echo ""
	@echo "  Résultats"
	@echo "    make results         Affiche les derniers résultats"
	@echo "    make runs            Liste les sessions de benchmark"
	@echo ""
	@echo "  Nettoyage"
	@echo "    make clean           Supprime les fichiers temporaires"
	@echo "    make clean-results   Supprime la base SQLite des résultats"
	@echo ""

# -----------------------------------------------------------------------------
# Dépendances
# -----------------------------------------------------------------------------
.PHONY: lock install
lock:
	poetry lock

install: lock
	poetry install

# -----------------------------------------------------------------------------
# Infrastructure Docker
# -----------------------------------------------------------------------------
.PHONY: up down down-v ps
up:
	docker compose up -d postgres

down:
	docker compose stop postgres

down-v:
	docker compose down -v postgres

ps:
	docker compose ps

# -----------------------------------------------------------------------------
# Sidecar PostgreSQL
# -----------------------------------------------------------------------------
.PHONY: sidecar sidecar-health
sidecar:
	poetry run uvicorn sidecar.postgres.main:app --port $(SIDECAR_PORT) --reload

sidecar-health:
	curl -s http://localhost:$(SIDECAR_PORT)/health | python3 -m json.tool

# -----------------------------------------------------------------------------
# Serveur de test
# -----------------------------------------------------------------------------
.PHONY: server server-health server-tests
server:
	poetry run python -m test_server.main

server-health:
	curl -s $(TEST_SERVER_URL)/health | python3 -m json.tool

server-tests:
	curl -s $(TEST_SERVER_URL)/tests | python3 -m json.tool

# -----------------------------------------------------------------------------
# Benchmark
# -----------------------------------------------------------------------------
.PHONY: run
run:
	poetry run python -m cli.run \
		--db postgres \
		--sidecar $(SIDECAR_POSTGRES_URL) \
		--source $(SOURCE) \
		$(if $(TESTS),--tests $(TESTS),) \
		--concurrence $(CONCURRENCE)

# -----------------------------------------------------------------------------
# Résultats
# -----------------------------------------------------------------------------
.PHONY: results runs
results:
	curl -s $(TEST_SERVER_URL)/results | python3 -m json.tool

runs:
	curl -s $(TEST_SERVER_URL)/runs | python3 -m json.tool

# -----------------------------------------------------------------------------
# Nettoyage
# -----------------------------------------------------------------------------
.PHONY: clean clean-results
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true

clean-results:
	rm -f data/results.db
