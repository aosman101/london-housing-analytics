import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"
REQUIRED_ENV_VARS = ["PGUSER", "PGPASSWORD", "PGHOST", "PGPORT", "PGDATABASE"]

load_dotenv(ENV_PATH)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

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

TABLES = [
    "mart_london_affordability_monthly",
    "mart_london_property_type_latest",
    "mart_london_borough_snapshot_latest",
]

for table in TABLES:
    query = f"select * from analytics.{table}"
    df = pd.read_sql(query, engine)
    df.to_csv(EXPORT_DIR / f"{table}.csv", index=False)
    print(f"[exported] {table}")
