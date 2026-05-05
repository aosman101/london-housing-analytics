"""Shared access to config/sources.yml so pipeline steps agree on vintages."""

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "sources.yml"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
NORMALISED_DIR = PROJECT_ROOT / "data" / "normalised"


@lru_cache(maxsize=1)
def load() -> dict:
    with CONFIG_PATH.open() as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict) or not isinstance(cfg.get("sources"), dict):
        raise ValueError(f"{CONFIG_PATH} must define a top-level sources mapping.")
    return cfg


def render(template: str, context: dict[str, Any]) -> str:
    context = dict(context)
    hpi_vintage = context.get("hpi_vintage", "")
    context["hpi_vintage_dash"] = hpi_vintage.replace("_", "-")
    return template.format(**context)


def source_spec(key: str) -> dict:
    cfg = load()
    try:
        spec = cfg["sources"][key]
    except KeyError as exc:
        raise KeyError(f"Unknown source key: {key}") from exc

    required = {"filename_template", "url_template"}
    missing = required - spec.keys()
    if missing:
        raise ValueError(f"Source {key} is missing required fields: {sorted(missing)}")
    return spec


def raw_filename(key: str) -> str:
    cfg = load()
    return render(source_spec(key)["filename_template"], cfg)


def raw_path(key: str) -> Path:
    return RAW_DIR / raw_filename(key)


def ashe_year() -> int:
    return int(load()["ashe_year"])
