"""Render README charts from the exported marts (run `make export` first)."""

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import config  # noqa: E402

EXPORT_DIR = config.PROJECT_ROOT / "data" / "exports"
OUT_DIR = config.PROJECT_ROOT / "docs" / "images"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
GRID = "#e4e3df"
BLUE = "#2a78d6"
RED = "#e34948"

SOURCE_NOTE = "Source: HM Land Registry UK HPI, ONS PIPR, ONS ASHE (full-time, place of residence)."


def _style(ax: plt.Axes) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=INK_SECONDARY, length=0, labelsize=9)
    ax.xaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _finish(fig: plt.Figure, title: str, subtitle: str, path: Path, note: str = "") -> None:
    fig.text(0.015, 0.975, title, fontsize=14, fontweight="bold", color=INK, va="top")
    fig.text(0.015, 0.94, subtitle, fontsize=10, color=INK_SECONDARY, va="top", linespacing=1.5)
    footer = f"{note}\n{SOURCE_NOTE}" if note else SOURCE_NOTE
    fig.text(0.015, 0.012, footer, fontsize=8, color=INK_SECONDARY, linespacing=1.6)
    fig.savefig(path, dpi=160, facecolor=SURFACE)
    plt.close(fig)
    print(f"[chart] {path.relative_to(config.PROJECT_ROOT)}")


def price_to_earnings_chart(snap: pd.DataFrame, month: str) -> None:
    df = snap.sort_values("price_to_earnings_ratio")
    labels = [
        f"{n} *" if fb else n for n, fb in zip(df["area_name"], df["earnings_fallback_used"], strict=True)
    ]
    fig, ax = plt.subplots(figsize=(9, 10), facecolor=SURFACE)
    fig.subplots_adjust(left=0.25, right=0.95, top=0.88, bottom=0.085)
    ax.barh(labels, df["price_to_earnings_ratio"], color=BLUE, height=0.72)
    _style(ax)
    ax.set_ylim(-0.6, len(df) - 0.4)
    for i, value in enumerate(df["price_to_earnings_ratio"]):
        if i < 3 or i >= len(df) - 3:
            ax.text(value + 0.2, i, f"{value:.1f}x", va="center", fontsize=9, color=INK)
    ax.set_xlabel("Average house price ÷ median full-time annual pay", color=INK_SECONDARY, fontsize=9)
    _finish(fig, "How many years of pay does a home cost?",
            f"London boroughs, price-to-earnings ratio, {month}", OUT_DIR / "price_to_earnings_by_borough.png",
            note="* City of London pay is suppressed by ONS (sample too small); London-wide pay is used.")


def rental_pressure_chart(snap: pd.DataFrame, month: str) -> None:
    df = snap.dropna(subset=["rent_growth_minus_income_growth_pct"]).sort_values(
        "rent_growth_minus_income_growth_pct"
    )
    values = df["rent_growth_minus_income_growth_pct"]
    fig, ax = plt.subplots(figsize=(9, 10), facecolor=SURFACE)
    fig.subplots_adjust(left=0.25, right=0.95, top=0.88, bottom=0.085)
    ax.barh(df["area_name"], values, color=[RED if v > 0 else BLUE for v in values], height=0.72)
    _style(ax)
    ax.set_ylim(-0.6, len(df) - 0.4)
    ax.axvline(0, color=INK_SECONDARY, linewidth=1)
    ax.set_xlabel("Rent growth minus pay growth, percentage points (year on year)",
                  color=INK_SECONDARY, fontsize=9)
    _finish(fig, "Where are rents outpacing pay?",
            f"London boroughs, private rent growth ({month}) minus median pay growth.\n"
            "Red = rents rising faster than pay  ·  Blue = pay rising faster than rents",
            OUT_DIR / "rental_pressure_by_borough.png",
            note="Borough pay growth is a single-year ASHE estimate and can be noisy (e.g. Camden +19.1%). "
            "City of London has no rent series.")


def main() -> None:
    path = EXPORT_DIR / "mart_london_borough_snapshot_latest.csv"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run `make export` first.")
    snap = pd.read_csv(path)
    month = pd.to_datetime(snap["date_month"].max()).strftime("%B %Y")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    price_to_earnings_chart(snap, month)
    rental_pressure_chart(snap, month)


if __name__ == "__main__":
    main()
