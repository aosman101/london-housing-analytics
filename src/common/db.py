"""PostgreSQL connection helpers shared by load and export steps."""

import os

from dotenv import load_dotenv
from sqlalchemy import URL, Engine, create_engine

from src.common import config

ENV_PATH = config.PROJECT_ROOT / ".env"
REQUIRED_ENV_VARS = ["PGUSER", "PGPASSWORD", "PGHOST", "PGPORT", "PGDATABASE"]


def load_environment() -> None:
    load_dotenv(ENV_PATH)


def require_env() -> None:
    missing_env_vars = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing_env_vars:
        missing = ", ".join(missing_env_vars)
        raise RuntimeError(
            f"Missing required environment variables: {missing}. "
            f"Create {ENV_PATH} from .env.example first."
        )


def database_url() -> URL:
    require_env()
    return URL.create(
        "postgresql+psycopg2",
        username=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        host=os.environ["PGHOST"],
        port=int(os.environ["PGPORT"]),
        database=os.environ["PGDATABASE"],
    )


def create_db_engine() -> Engine:
    return create_engine(database_url())
