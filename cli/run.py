"""
CLI — DB Benchmarker

Usage :
    python -m cli.run --db postgres --sidecar http://localhost:8001/graphql
    python -m cli.run --db postgres --sidecar http://localhost:8001/graphql --tests R1,R4,C1
    python -m cli.run --db postgres --sidecar http://localhost:8001/graphql --source afnic --tests W1
"""
from __future__ import annotations

import json
import os
import sys

import click

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_server.runner import available_tests, geometric_steps, run_benchmark
from test_server.storage import init_db


@click.command()
@click.option("--db",          required=True,                       help="Nom de la base (ex. postgres)")
@click.option("--sidecar",     required=True,                       help="URL GraphQL du sidecar")
@click.option("--source",      default="insee", show_default=True,  help="Source : insee | afnic | bodacc")
@click.option("--tests",       default=None,                        help="Tests séparés par virgules (défaut : tous)")
@click.option("--concurrence", default=10,      show_default=True,  help="Niveau de concurrence C1/REL4")
@click.option("--output",      default=None,                        help="Fichier JSON pour exporter les résultats")
def run(db, sidecar, source, tests, concurrence, output):
    """Lance un benchmark multi-paliers sur une base via son sidecar GraphQL."""
    init_db()

    test_codes = [t.strip().upper() for t in tests.split(",")] if tests else available_tests()
    test_kwargs = {
        "C1":   {"concurrence": concurrence},
        "REL4": {"concurrence": concurrence},
    }

    max_rows = int(os.getenv("MAX_ROWS", "10000").replace("_", ""))
    steps = geometric_steps(max_rows)

    click.echo(f"\ndb={db}  sidecar={sidecar}  source={source}")
    click.echo(f"tests    : {', '.join(test_codes)}")
    click.echo(f"paliers  : {steps}")
    click.echo(f"itérations par test : 3 (warm-up écarté)")
    click.echo("─" * 60)

    summary = run_benchmark(
        db_name=db,
        sidecar_url=sidecar,
        test_codes=test_codes,
        source=source,
        test_kwargs=test_kwargs,
    )

    click.echo(f"\nrun_id : {summary['run_id']}")
    click.echo(f"{len(summary['results'])} mesures persistées dans SQLite\n")

    if output:
        with open(output, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        click.echo(f"exporté → {output}")


if __name__ == "__main__":
    run()
