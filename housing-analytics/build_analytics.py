"""Compute every analysis for the London housing dashboard.

Reads data/normalised/*.csv (run `make normalise` first) and data/spatial, so no database is
needed. Re-implements the dbt mart logic in pandas: borough earnings with a London-wide
fallback, attached only to months within one year of the ASHE reference year.

Outputs (all inside housing-analytics/):
    dashboard/data.js     data payload for the interactive dashboard (index.html)
    tables/*.csv          the numbers behind every visual, for Excel / Tableau / Power BI
    key_metrics.json      headline figures
"""

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.common import config  # noqa: E402

OUT = Path(__file__).resolve().parent
TABLES = OUT / "tables"
DASH = OUT / "dashboard"
NORM = config.NORMALISED_DIR
SPATIAL = ROOT / "data" / "spatial"

LONDON_REGION = "E12000007"
EARNINGS_MAX_LAG_YEARS = 1
# ONS statistical definition of Inner London.
INNER_LONDON = {
    "E09000001", "E09000007", "E09000012", "E09000013", "E09000014", "E09000019", "E09000020",
    "E09000022", "E09000023", "E09000025", "E09000028", "E09000030", "E09000032", "E09000033",
}
PROPERTY_TYPES = ["Flat/Maisonette", "Terraced", "Semi-detached", "Detached"]


# --------------------------------------------------------------------------- load + model


def load() -> dict[str, pd.DataFrame]:
    def read(name: str) -> pd.DataFrame:
        path = NORM / f"{name}.csv"
        if not path.exists():
            raise FileNotFoundError(f"{path} missing. Run `make download normalise` first.")
        df = pd.read_csv(path)
        if "date_month" in df:
            df["date_month"] = pd.to_datetime(df["date_month"])
        return df

    return {
        "prices": read("hpi_average_prices").rename(columns={"pct_change_12m": "house_price_yoy_pct"}),
        "sales": read("hpi_sales"),
        "rents": read("pipr_local_rents"),
        "ptype": read("hpi_property_type_prices"),
        "ashe": read("ashe_earnings"),
    }


def attach_earnings(df: pd.DataFrame, ashe: pd.DataFrame) -> pd.DataFrame:
    """Borough earnings (London fallback), only for months inside the ASHE coverage window."""
    ref_year = int(ashe["reference_year"].max())
    cols = ["median_gross_annual_pay", "earnings_yoy_pct", "earnings_quality", "median_pay_cv"]
    latest = ashe[ashe["reference_year"] == ref_year]
    london = latest[latest["area_code"] == LONDON_REGION][cols].iloc[0]
    df = df.merge(latest[latest["area_code"] != LONDON_REGION][["area_code", *cols]], on="area_code", how="left")

    year = df["date_month"].dt.year
    in_window = ((year >= ref_year) & (year - ref_year <= EARNINGS_MAX_LAG_YEARS)).to_numpy()
    # Only flag a fallback where earnings are actually attached (dbt leaves other months null).
    df["earnings_fallback_used"] = df["median_gross_annual_pay"].isna().to_numpy() & in_window
    for c in cols:
        df[c] = df[c].where(df[c].notna(), london[c])
    df.loc[~in_window, cols] = np.nan
    return df


def build_monthly(d: dict) -> pd.DataFrame:
    m = (
        d["prices"]
        .merge(d["sales"][["date_month", "area_code", "sales_volume"]], on=["date_month", "area_code"], how="left")
        .merge(d["rents"][["date_month", "area_code", "avg_monthly_rent", "rent_yoy_pct"]],
               on=["date_month", "area_code"], how="left")
    )
    m = attach_earnings(m, d["ashe"])
    pay = m["median_gross_annual_pay"]
    m["price_to_earnings_ratio"] = m["average_price"] / pay
    m["annual_rent_to_earnings_ratio"] = m["avg_monthly_rent"] * 12 / pay
    m["months_to_save_10pct_deposit"] = m["average_price"] * 0.10 / (pay / 12)
    m["rent_growth_minus_income_growth_pct"] = m["rent_yoy_pct"] - m["earnings_yoy_pct"]
    m["gross_rental_yield_pct"] = m["avg_monthly_rent"] * 12 / m["average_price"] * 100
    m["group"] = np.where(m["area_code"].isin(INNER_LONDON), "Inner London", "Outer London")
    m["year"] = m["date_month"].dt.year
    return m.sort_values(["area_code", "date_month"])


# --------------------------------------------------------------------------- analyses


def borough_metrics(m: pd.DataFrame, latest: pd.Timestamp) -> pd.DataFrame:
    """One row per borough with every derived metric used by the dashboard."""
    snap = m[m.date_month == latest].set_index("area_code")
    b = snap[["area_name", "group", "average_price", "house_price_yoy_pct", "avg_monthly_rent", "rent_yoy_pct",
              "median_gross_annual_pay", "earnings_yoy_pct", "earnings_quality", "median_pay_cv",
              "earnings_fallback_used", "price_to_earnings_ratio", "annual_rent_to_earnings_ratio",
              "months_to_save_10pct_deposit", "rent_growth_minus_income_growth_pct",
              "gross_rental_yield_pct"]].copy()

    def at(month: str, col: str) -> pd.Series:
        return m[m.date_month == pd.Timestamp(month)].set_index("area_code")[col]

    rent_latest = m.dropna(subset=["avg_monthly_rent"]).groupby("area_code").last()
    b["rent_growth_since_2015_pct"] = (rent_latest["avg_monthly_rent"] / at("2015-01-01", "avg_monthly_rent") - 1) * 100
    b["price_growth_since_2015_pct"] = (b["average_price"] / at("2015-01-01", "average_price") - 1) * 100
    b["rent_growth_5y_pct"] = (b["avg_monthly_rent"] / at(f"{latest.year - 5}-{latest.month:02d}-01",
                                                          "avg_monthly_rent") - 1) * 100

    ann = m[m.year <= 2025].groupby(["year", "area_code"])["average_price"].mean().unstack()
    b["cagr_1995_2025_pct"] = ((ann.loc[2025] / ann.loc[1995]) ** (1 / 30) - 1) * 100
    b["cagr_2015_2025_pct"] = ((ann.loc[2025] / ann.loc[2015]) ** (1 / 10) - 1) * 100
    b["cagr_2020_2025_pct"] = ((ann.loc[2025] / ann.loc[2020]) ** (1 / 5) - 1) * 100

    pv = m.pivot(index="date_month", columns="area_code", values="average_price")
    b["peak_month"] = pv.idxmax().dt.strftime("%Y-%m")
    b["peak_price"] = pv.max()
    b["pct_below_peak"] = (b["average_price"] / b["peak_price"] - 1) * 100

    recent = m[m.date_month >= "2015-01-01"]
    b["price_volatility_sd"] = recent.groupby("area_code")["pct_change_1m"].std()

    sales = m.dropna(subset=["sales_volume"]).groupby("area_code").last()
    b["latest_sales_month"] = sales["date_month"].dt.strftime("%Y-%m")
    b["latest_sales_volume"] = sales["sales_volume"]
    annual_sales = m[m.year <= 2025].groupby(["year", "area_code"])["sales_volume"].sum().unstack()
    b["sales_2025"] = annual_sales.loc[2025]
    b["sales_2025_vs_2021_pct"] = (annual_sales.loc[2025] / annual_sales.loc[2021] - 1) * 100

    corr_same, corr_lead = {}, {}
    for code in b.index:
        s = annual_sales[code].pct_change()
        p = ann[code].pct_change()
        corr_same[code], corr_lead[code] = s.corr(p), s.shift(1).corr(p)
    b["sales_price_corr_same_year"] = pd.Series(corr_same)
    b["sales_price_corr_lead_1y"] = pd.Series(corr_lead)

    # Composite housing stress index: equal-weighted z-scores of three independent pressures.
    comps = ["price_to_earnings_ratio", "annual_rent_to_earnings_ratio", "rent_growth_5y_pct"]
    z = (b[comps] - b[comps].mean()) / b[comps].std()
    b["stress_index"] = z.mean(axis=1, skipna=False)

    for col, asc in [("price_to_earnings_ratio", False), ("annual_rent_to_earnings_ratio", False),
                     ("rent_growth_minus_income_growth_pct", False), ("stress_index", False),
                     ("gross_rental_yield_pct", False)]:
        b[f"{col}_rank"] = b[col].rank(ascending=asc, method="min")
    return b.reset_index()


def group_trends(m: pd.DataFrame) -> pd.DataFrame:
    g = m.groupby(["date_month", "group"]).agg(
        median_price=("average_price", "median"),
        median_rent=("avg_monthly_rent", "median"),
        median_rent_yoy_pct=("rent_yoy_pct", "median"),
        median_price_yoy_pct=("house_price_yoy_pct", "median"),
        # min_count=1: months Land Registry has not yet published stay null instead of summing to 0.
        sales=("sales_volume", lambda s: s.sum(min_count=1)),
    ).reset_index()
    return g


def annual_market(m: pd.DataFrame) -> pd.DataFrame:
    full = m[m.year <= 2025]
    a = full.groupby("year").agg(sales=("sales_volume", "sum"), avg_price=("average_price", "mean")).reset_index()
    a["sales_change_pct"] = a["sales"].pct_change() * 100
    a["price_change_pct"] = a["avg_price"].pct_change() * 100
    return a


def property_types(d: dict) -> pd.DataFrame:
    pt = d["ptype"]
    latest = pt.date_month.max()
    pt = attach_earnings(pt[pt.date_month == latest], d["ashe"])
    pt["price_to_earnings_ratio"] = pt["average_price"] / pt["median_gross_annual_pay"]
    pt["group"] = np.where(pt["area_code"].isin(INNER_LONDON), "Inner London", "Outer London")
    return pt[["date_month", "area_code", "area_name", "group", "property_type", "average_price",
               "house_price_yoy_pct" if "house_price_yoy_pct" in pt else "pct_change_12m",
               "median_gross_annual_pay", "earnings_fallback_used", "price_to_earnings_ratio"]]


def coverage(d: dict) -> pd.DataFrame:
    labels = {"prices": "HPI average prices", "sales": "HPI sales volumes", "rents": "PIPR private rents",
              "ptype": "HPI prices by property type", "ashe": "ASHE earnings (+ London row)"}
    rows = []
    for key, label in labels.items():
        df = d[key]
        has_date = "date_month" in df
        rows.append({
            "dataset": label,
            "rows": len(df),
            "boroughs": df.loc[df.area_code.str.startswith("E09"), "area_code"].nunique(),
            "first_period": str(df.date_month.min().date())[:7] if has_date else str(df.reference_year.min()),
            "last_period": str(df.date_month.max().date())[:7] if has_date else str(df.reference_year.max()),
            "null_cells_pct": round(df.isna().mean().mean() * 100, 2),
        })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- output


def jsonable(o):
    if isinstance(o, dict):
        return {str(k): jsonable(v) for k, v in o.items()}
    if isinstance(o, list | tuple):
        return [jsonable(v) for v in o]
    if isinstance(o, float | np.floating):
        return None if np.isnan(o) else round(float(o), 2)
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, pd.Timestamp):
        return o.strftime("%Y-%m")
    return o


def records(df: pd.DataFrame) -> list[dict]:
    return jsonable(df.replace({np.nan: None}).to_dict(orient="records"))


def main() -> None:
    TABLES.mkdir(parents=True, exist_ok=True)
    DASH.mkdir(parents=True, exist_ok=True)

    d = load()
    m = build_monthly(d)
    latest = m.dropna(subset=["price_to_earnings_ratio", "avg_monthly_rent"]).date_month.max()

    b = borough_metrics(m, latest)
    gt = group_trends(m)
    ann = annual_market(m)
    pt = property_types(d)
    cov = coverage(d)
    london = d["ashe"][d["ashe"].area_code == LONDON_REGION].iloc[0]

    # Tables for analysts / BI tools.
    def save(df: pd.DataFrame, name: str) -> None:
        df.to_csv(TABLES / f"{name}.csv", index=False, float_format="%.2f")

    save(b.sort_values("stress_index", ascending=False), "borough_scorecard")
    save(gt, "inner_outer_monthly_trends")
    save(ann, "london_annual_sales_and_prices")
    save(pt, "property_type_affordability")
    save(cov, "data_coverage")
    m.drop(columns="year").to_csv(TABLES / "borough_monthly_panel.csv.gz", index=False, float_format="%.4f")

    rent_ok = b.dropna(subset=["annual_rent_to_earnings_ratio"])
    kpis = {
        "latest_month": latest.strftime("%B %Y"),
        "latest_month_iso": latest.strftime("%Y-%m"),
        "ashe_year": int(d["ashe"].reference_year.max()),
        "london_pay": float(london.median_gross_annual_pay),
        "london_pay_growth": float(london.earnings_yoy_pct),
        "median_price": b.average_price.median(),
        "median_rent": b.avg_monthly_rent.median(),
        "median_pe": b.price_to_earnings_ratio.median(),
        "pe_max": [b.loc[b.price_to_earnings_ratio.idxmax(), "area_name"], b.price_to_earnings_ratio.max()],
        "pe_min": [b.loc[b.price_to_earnings_ratio.idxmin(), "area_name"], b.price_to_earnings_ratio.min()],
        "median_deposit_months": b.months_to_save_10pct_deposit.median(),
        "median_rent_share": rent_ok.annual_rent_to_earnings_ratio.median(),
        "boroughs_above_30pct_rent": int((rent_ok.annual_rent_to_earnings_ratio > 0.3).sum()),
        "boroughs_with_rent": int(len(rent_ok)),
        "rent_outpacing_pay": int((b.rent_growth_minus_income_growth_pct > 0).sum()),
        "median_rent_yoy": b.rent_yoy_pct.median(),
        "median_price_yoy": b.house_price_yoy_pct.median(),
        "rent_outgrew_price_since_2015": int((b.rent_growth_since_2015_pct > b.price_growth_since_2015_pct).sum()),
        "median_rent_growth_since_2015": b.rent_growth_since_2015_pct.median(),
        "median_price_growth_since_2015": b.price_growth_since_2015_pct.median(),
        "sales_2025": int(ann.set_index("year").sales[2025]),
        "sales_peak": [int(ann.loc[ann.sales.idxmax(), "year"]), int(ann.sales.max())],
        "sales_corr_same_year": ann.sales_change_pct.corr(ann.price_change_pct),
        "sales_corr_lead_1y": ann.sales_change_pct.shift(1).corr(ann.price_change_pct),
        "boroughs_5pct_below_peak": int((b.pct_below_peak < -5).sum()),
        "ptype_median_pe": pt.groupby("property_type").price_to_earnings_ratio.median().to_dict(),
        "yield_price_corr": b[["gross_rental_yield_pct", "average_price"]].corr().iloc[0, 1],
        "built": date.today().isoformat(),
    }
    (OUT / "key_metrics.json").write_text(json.dumps(jsonable(kpis), indent=2))

    # Dashboard payload: compact per-borough monthly arrays aligned to one month index.
    months = sorted(m.date_month.unique())
    month_idx = pd.DatetimeIndex(months)
    series = {}
    for code, g in m.groupby("area_code"):
        g = g.set_index("date_month").reindex(month_idx)
        series[code] = {
            "price": g.average_price.round(0).tolist(),
            "price_yoy": g.house_price_yoy_pct.round(1).tolist(),
            "sales": g.sales_volume.tolist(),
            "rent": g.avg_monthly_rent.round(0).tolist(),
            "rent_yoy": g.rent_yoy_pct.round(1).tolist(),
        }
    geo = json.loads((SPATIAL / "london_borough_boundaries_only.geojson").read_text())
    for f in geo["features"]:
        f["properties"] = {"name": f["properties"]["LAD24CD"]}
        f["geometry"]["coordinates"] = json.loads(
            json.dumps(f["geometry"]["coordinates"]), parse_float=lambda x: round(float(x), 4)
        )

    payload = {
        "kpis": kpis,
        "boroughs": records(b),
        "months": [pd.Timestamp(x).strftime("%Y-%m") for x in months],
        "series": jsonable(series),
        "groupTrends": records(gt.assign(date_month=gt.date_month.dt.strftime("%Y-%m"))),
        "annual": records(ann),
        "propertyTypes": records(pt.drop(columns="date_month")),
        "coverage": records(cov),
        "geo": geo,
    }
    js = "// Generated by build_analytics.py — do not edit by hand.\nwindow.LHA_DATA = "
    (DASH / "data.js").write_text(js + json.dumps(jsonable(payload), separators=(",", ":")) + ";\n")
    print(f"[done] dashboard data for {len(b)} boroughs, {len(months)} months -> {DASH / 'data.js'}")


if __name__ == "__main__":
    main()
