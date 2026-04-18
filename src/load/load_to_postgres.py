from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.environ['PGUSER']}:{os.environ['PGPASSWORD']}"
    f"@{os.environ['PGHOST']}:{os.environ['PGPORT']}/{os.environ['PGDATABASE']}"
)

NORM = Path("data/normalised")

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
