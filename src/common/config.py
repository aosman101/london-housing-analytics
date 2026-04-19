"""Shared access to config/sources.yml so pipeline steps agree on vintages."""

from functools import lru_cache
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yml"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
NORMALISED_DIR = PROJECT_ROOT / "data" / "normalised"


@lru_cache(maxsize=1)
def load() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def _render(template: str, context: dict) -> str:
    context = dict(context)
    hpi_vintage = context.get("hpi_vintage", "")
    context["hpi_vintage_dash"] = hpi_vintage.replace("_", "-")
    return template.format(**context)


def raw_filename(key: str) -> str:
    cfg = load()
    return _render(cfg["sources"][key]["filename_template"], cfg)


def raw_path(key: str) -> Path:
    return RAW_DIR / raw_filename(key)


def ashe_year() -> int:
    return int(load()["ashe_year"])
