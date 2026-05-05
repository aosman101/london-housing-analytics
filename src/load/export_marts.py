import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import config  # noqa: E402
from src.common import db  # noqa: E402

EXPORT_DIR = PROJECT_ROOT / "data" / "exports"

TABLES = [
    "mart_london_affordability_monthly",
    "mart_london_property_type_latest",
    "mart_london_borough_snapshot_latest",
]


def export_tables(engine: Engine) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        query = f"select * from analytics.{table}"
        df = pd.read_sql(query, engine)
        df.to_csv(EXPORT_DIR / f"{table}.csv", index=False)
        print(f"[exported] {table} ({len(df)} rows)")


def main() -> None:
    db.load_environment()
    export_tables(db.create_db_engine())


if __name__ == "__main__":
    main()
