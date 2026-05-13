"""
Serveur de test — API Flask

Endpoints :
  GET  /health       état du serveur
  GET  /tests        liste des tests disponibles
  POST /run          lance un benchmark
  GET  /results      résultats (optionnel : ?run_id=...)
  GET  /runs         sessions de benchmark
"""
from __future__ import annotations

import os

from flask import Flask, jsonify, request

from .runner import available_tests, run_benchmark
from .storage import get_results, get_runs, init_db

app = Flask(__name__)
init_db()


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/tests")
def list_tests():
    return jsonify({"tests": available_tests()})


@app.post("/run")
def run():
    body = request.get_json(force=True)
    db_name = body.get("db_name")
    sidecar_url = body.get("sidecar_url")
    if not db_name or not sidecar_url:
        return jsonify({"error": "db_name et sidecar_url sont requis"}), 400

    summary = run_benchmark(
        db_name=db_name,
        sidecar_url=sidecar_url,
        test_codes=body.get("tests", available_tests()),
        source=body.get("source", "insee"),
        test_kwargs=body.get("kwargs", {}),
    )
    return jsonify(summary)


@app.get("/results")
def results():
    return jsonify(get_results(request.args.get("run_id")))


@app.get("/runs")
def runs():
    return jsonify(get_runs())


if __name__ == "__main__":
    port = int(os.getenv("TEST_SERVER_PORT", "5400"))
    app.run(host="0.0.0.0", port=port, debug=True)
