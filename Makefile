.PHONY: help install up down download normalise load pipeline dbt-deps dbt-run dbt-test dbt-docs export all clean

PYTHON ?= python

help:
	@echo "Targets:"
	@echo "  install    — install Python requirements into the active venv"
	@echo "  up/down    — start/stop the local Postgres container"
	@echo "  download   — download raw HPI/PIPR/ASHE sources"
	@echo "  normalise  — produce London-only normalised CSVs"
	@echo "  load       — load normalised CSVs into raw.* Postgres tables"
	@echo "  pipeline   — download + normalise + load"
	@echo "  dbt-deps   — install dbt packages (dbt_utils)"
	@echo "  dbt-run    — run dbt models"
	@echo "  dbt-test   — run dbt tests"
	@echo "  dbt-docs   — generate and serve dbt docs on :8080"
	@echo "  export     — export marts to data/exports as CSV"
	@echo "  all        — pipeline + dbt-deps + dbt-run + dbt-test + export"
	@echo "  clean      — remove data/ and dbt/target build outputs"

install:
	$(PYTHON) -m pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down

download:
	$(PYTHON) src/extract/download_sources.py

normalise:
	$(PYTHON) src/transform/normalise_sources.py

load:
	$(PYTHON) src/load/load_to_postgres.py

pipeline: download normalise load

dbt-deps:
	cd dbt && dbt deps

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve --port 8080

export:
	$(PYTHON) src/load/export_marts.py

all: pipeline dbt-deps dbt-run dbt-test export

clean:
	rm -rf data/raw/* data/normalised/* data/exports/* dbt/target dbt/logs
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
