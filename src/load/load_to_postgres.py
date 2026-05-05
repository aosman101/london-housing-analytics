import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import (
    config,  # noqa: E402
    db,  # noqa: E402
)

NORM = config.NORMALISED_DIR

TABLE_MAP = {
    "hpi_average_prices.csv": "hpi_average_prices",
    "hpi_property_type_prices.csv": "hpi_property_type_prices",
    "hpi_sales.csv": "hpi_sales",
    "pipr_local_rents.csv": "pipr_local_rents",
    "ashe_earnings.csv": "ashe_earnings",
}


def ensure_inputs_exist() -> None:
    missing_files = [NORM / fn for fn in TABLE_MAP if not (NORM / fn).exists()]
    if missing_files:
        names = ", ".join(p.name for p in missing_files)
        raise FileNotFoundError(
            f"Missing normalised files: {names}. "
            "Run src/transform/normalise_sources.py first. "
            "Refusing to replace raw tables without complete replacement data."
        )


def load_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists raw;"))

    for file_name, table_name in TABLE_MAP.items():
        path = NORM / file_name
        df = pd.read_csv(path)
        with engine.begin() as conn:
            conn.execute(text(f'drop table if exists raw."{table_name}" cascade'))
        df.to_sql(
            table_name,
            engine,
            schema="raw",
            if_exists="fail",
            index=False,
            chunksize=5000,
            method="multi",
        )
        print(f"[loaded] raw.{table_name} ({len(df)} rows)")


def main() -> None:
    db.load_environment()
    ensure_inputs_exist()
    load_tables(db.create_db_engine())


if __name__ == "__main__":
    main()
