.PHONY: help install up down download normalise load pipeline dbt-deps dbt-run dbt-test dbt-build dbt-docs export charts dashboard lint test all clean

# Prefer the project venv when it exists so `make all` works without activating it.
VENV_BIN := $(wildcard .venv/bin)
PYTHON ?= $(if $(VENV_BIN),.venv/bin/python,python)
DBT ?= $(if $(VENV_BIN),$(CURDIR)/.venv/bin/dbt,dbt)
RUFF ?= $(if $(VENV_BIN),.venv/bin/ruff,ruff)

# Share .env credentials with dbt (profiles.yml reads PG* env vars).
-include .env
export PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD
export DBT_PROFILES_DIR := $(CURDIR)/dbt

help:
	@echo "Targets:"
	@echo "  install    — install Python requirements"
	@echo "  up/down    — start/stop the local Postgres container"
	@echo "  download   — download raw HPI/PIPR/ASHE sources"
	@echo "  normalise  — produce London-only normalised CSVs"
	@echo "  load       — load normalised CSVs into raw.* Postgres tables"
	@echo "  pipeline   — download + normalise + load"
	@echo "  dbt-deps   — install dbt packages (dbt_utils)"
	@echo "  dbt-run    — run dbt models"
	@echo "  dbt-test   — run dbt tests"
	@echo "  dbt-build  — run + test dbt models in DAG order"
	@echo "  dbt-docs   — generate and serve dbt docs on :8080"
	@echo "  export     — export marts to data/exports as CSV"
	@echo "  charts     — render README charts from exported marts into docs/images"
	@echo "  dashboard  — rebuild housing-analytics/ (tables, dashboard data, PNG images via Chrome)"
	@echo "  lint       — ruff check"
	@echo "  test       — run Python unit tests"
	@echo "  all        — lint + test + pipeline + dbt-deps + dbt-build + export + charts"
	@echo "  clean      — remove data/ and dbt/target build outputs"

install:
	$(PYTHON) -m pip install -r requirements.txt

up:
	docker compose up -d --wait

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
	cd dbt && $(DBT) deps

dbt-run:
	cd dbt && $(DBT) run

dbt-test:
	cd dbt && $(DBT) test

dbt-build:
	cd dbt && $(DBT) build

dbt-docs:
	cd dbt && $(DBT) docs generate && $(DBT) docs serve --port 8080

export:
	$(PYTHON) src/load/export_marts.py

charts:
	$(PYTHON) src/report/make_charts.py

dashboard:
	$(PYTHON) housing-analytics/build_analytics.py
	$(PYTHON) housing-analytics/export_images.py

lint:
	$(RUFF) check src tests housing-analytics

test:
	$(PYTHON) -m unittest discover -s tests

all: lint test pipeline dbt-deps dbt-build export charts

clean:
	rm -rf data/raw/* data/normalised/* data/exports/* dbt/target dbt/logs
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
