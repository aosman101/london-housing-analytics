# london-housing-analytics

![Status](https://img.shields.io/badge/status-in%20progress-orange)
![Python](https://img.shields.io/badge/python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/postgresql-16-336791)
![dbt](https://img.shields.io/badge/dbt-postgres-FC6D26)
![Docker](https://img.shields.io/badge/docker-compose-2496ED)

London housing affordability and rental pressure analytics project, currently at the setup and early ingestion stage.

## Current Progress

This repository is not finished yet. The work completed so far is:

- Project structure created for `data`, `src`, `dbt`, and `tableau`
- Python environment and pinned dependencies added in `requirements.txt`
- Local Postgres configuration added via `docker-compose.yml`
- Example environment variables added in `.env.example`
- Raw source download script added for HM Land Registry HPI, ONS PIPR, and ASHE inputs
- Source inspection script added for Excel and zipped source files
- Initial normalisation script added for London LAD-level datasets

## What Exists Today

### Extraction

- [src/extract/download_sources.py](/Users/adilosman/Downloads/london-housing-analytics/src/extract/download_sources.py) downloads the current source files into `data/raw`

### Inspection

- [src/transform/inspect_sources.py](/Users/adilosman/Downloads/london-housing-analytics/src/transform/inspect_sources.py) previews workbook sheets and extracts ASHE zip contents for inspection

### Normalisation

- [src/transform/normalise_sources.py](/Users/adilosman/Downloads/london-housing-analytics/src/transform/normalise_sources.py) standardises HPI average prices, HPI sales, HPI property type prices, PIPR local rents, and ASHE earnings

## Not Built Yet

The following are planned but not yet implemented in this repo:

- PostgreSQL table creation and load scripts
- dbt staging and mart models
- Tests and validation checks
- Tableau workbook/output files
- Final project documentation

## Repo Structure

```text
london-housing-analytics/
├── data/
│   ├── exports/
│   ├── normalised/
│   ├── raw/
│   └── spatial/
├── dbt/
│   └── models/
│       ├── marts/
│       └── staging/
├── src/
│   ├── extract/
│   │   └── download_sources.py
│   ├── load/
│   └── transform/
│       ├── inspect_sources.py
│       └── normalise_sources.py
├── tableau/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── README.md
└── requirements.txt
```

## Local Setup

Create and activate the virtual environment:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start PostgreSQL when Docker Desktop is running:

```bash
docker compose up -d
```

## Environment Variables

Copy `.env.example` to `.env` and update values if needed:

```env
PGHOST=localhost
PGPORT=5432
PGDATABASE=housing_warehouse
PGUSER=analytics
PGPASSWORD=analytics
```

## Next Steps

Near-term work is:

1. Download the raw source files into `data/raw`
2. Inspect and extract the ASHE source contents
3. Run the normalisation pipeline into `data/normalised`
4. Add database load scripts
5. Build dbt models and reporting outputs
