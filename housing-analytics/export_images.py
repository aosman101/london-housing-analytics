"""Export each dashboard visual, and the full dashboard, as PNG images via headless Chrome.

Usage:  python housing-analytics/export_images.py [visual_id ...]
Chrome is found automatically on macOS/Linux; override with the CHROME environment variable.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
DASHBOARD = HERE / "dashboard" / "index.html"
IMAGES = HERE / "images"
SCALE = 2  # device pixel ratio -> crisp images for LinkedIn

# file name, visual id (or a full query string for app-page screenshots), CSS width, CSS height
VISUALS = [
    ("00_full_dashboard", "full", 1920, 2560),
    ("01_map_price_to_earnings", "map", 1200, 1000),
    ("02_price_to_earnings_ranking", "pe", 1000, 1150),
    ("03_housing_stress_index", "stress", 1000, 1150),
    ("04_deposit_months", "deposit", 1000, 1150),
    ("05_rent_share_of_pay", "rentshare", 1000, 1150),
    ("06_rents_vs_pay_growth", "rentgap", 1000, 1150),
    ("07_rent_vs_price_growth_since_2015", "rentvsprice", 1200, 900),
    ("08_rent_inflation_trend", "renttrend", 1200, 750),
    ("09_house_price_trend_1995_2026", "pricetrend", 1200, 750),
    ("10_sales_volume_history", "sales", 1200, 750),
    ("11_sales_lead_prices", "salesprice", 1200, 850),
    ("12_sales_2025_vs_2021", "salesdrop", 1000, 1150),
    ("13_drawdown_from_peak", "drawdown", 1000, 1150),
    ("14_property_type_heatmap", "heatmap", 1000, 1250),
    ("15_flat_vs_detached_gap", "dumbbell", 1000, 1150),
    ("16_property_type_medians", "ptypemed", 1200, 700),
    ("17_gross_rental_yield", "yield", 1000, 1150),
    ("18_yield_vs_price", "yieldscatter", 1200, 850),
    ("19_price_volatility", "volatility", 1000, 1150),
    ("20_app_overview_page", "page=overview", 1600, 1300),
    ("21_app_borough_profile_page", "page=profile&borough=E09000020", 1600, 1150),
    ("22_app_rental_pressure_page", "page=rent&group=Outer%20London&borough=E09000004", 1600, 1250),
]


def find_chrome() -> str:
    candidates = [
        os.environ.get("CHROME"),
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        shutil.which("google-chrome"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
    ]
    for c in candidates:
        if c and Path(c).exists():
            return c
    raise FileNotFoundError("Chrome not found. Set CHROME=/path/to/chrome.")


# The README's pipeline map (docs/architecture.html), in GitHub's light and dark themes.
ARCHITECTURE_PAGE = HERE.parent / "docs" / "architecture.html"
ARCHITECTURE_IMAGES = [
    ("architecture_light", "export=1&theme=light", 1600, 900),
    ("architecture_dark", "export=1&theme=dark", 1600, 900),
]


def export(chrome: str, name: str, visual: str, width: int, height: int,
           page: Path = DASHBOARD, out_dir: Path = IMAGES) -> None:
    out = out_dir / f"{name}.png"
    query = visual if "=" in visual else f"export={visual}"
    url = f"{page.as_uri()}?{query}"
    with tempfile.TemporaryDirectory() as profile:
        cmd = [
            chrome, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
            f"--user-data-dir={profile}", f"--force-device-scale-factor={SCALE}",
            f"--window-size={width},{height}", "--virtual-time-budget=15000",
            "--run-all-compositor-stages-before-draw", f"--screenshot={out}", url,
        ]
        # Headless Chrome occasionally exits non-zero on macOS display-link errors; retry.
        for attempt in range(1, 4):
            out.unlink(missing_ok=True)  # never mistake a stale image for success
            result = subprocess.run(cmd, capture_output=True, timeout=180)
            if result.returncode == 0 and out.exists():
                break
            if attempt == 3:
                raise RuntimeError(f"Chrome failed to export {visual}: {result.stderr.decode()[-500:]}")
    print(f"[image] {out.relative_to(HERE.parent)}  ({width * SCALE}x{height * SCALE})")


def main() -> None:
    if not (HERE / "dashboard" / "data.js").exists():
        raise FileNotFoundError("dashboard/data.js missing. Run build_analytics.py first.")
    IMAGES.mkdir(exist_ok=True)
    wanted = set(sys.argv[1:])
    chrome = find_chrome()
    for name, visual, w, h in VISUALS:
        if not wanted or visual in wanted or name in wanted:
            export(chrome, name, visual, w, h)
    if not wanted or "architecture" in wanted:
        for name, query, w, h in ARCHITECTURE_IMAGES:
            export(chrome, name, query, w, h, page=ARCHITECTURE_PAGE, out_dir=ARCHITECTURE_PAGE.parent / "images")


if __name__ == "__main__":
    main()
