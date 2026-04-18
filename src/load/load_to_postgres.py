import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"
NORM = PROJECT_ROOT / "data" / "normalised"
REQUIRED_ENV_VARS = ["PGUSER", "PGPASSWORD", "PGHOST", "PGPORT", "PGDATABASE"]

load_dotenv(ENV_PATH)

missing_env_vars = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
if missing_env_vars:
    missing = ", ".join(missing_env_vars)
    raise RuntimeError(
        f"Missing required environment variables: {missing}. "
        f"Create {ENV_PATH} from .env.example first."
    )

engine = create_engine(
    f"postgresql+psycopg2://{os.environ['PGUSER']}:{os.environ['PGPASSWORD']}"
    f"@{os.environ['PGHOST']}:{os.environ['PGPORT']}/{os.environ['PGDATABASE']}"
)

TABLE_MAP = {
    "hpi_average_prices.csv": "hpi_average_prices",
    "hpi_property_type_prices.csv": "hpi_property_type_prices",
    "hpi_sales.csv": "hpi_sales",
    "pipr_local_rents.csv": "pipr_local_rents",
    "ashe_earnings.csv": "ashe_earnings",
}

with engine.begin() as conn:
    conn.execute(text("create schema if not exists raw;"))

for file_name, table_name in TABLE_MAP.items():
    path = NORM / file_name
    if not path.exists():
        print(f"[skip] missing file: {path}")
        continue
    df = pd.read_csv(path)
    df.to_sql(
        table_name,
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
        chunksize=5000,
        method="multi",
    )
    print(f"[loaded] raw.{table_name}")
