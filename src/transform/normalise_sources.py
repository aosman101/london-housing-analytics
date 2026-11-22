import re
import sys
import zipfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import config  # noqa: E402

RAW = config.RAW_DIR
NORM = config.NORMALISED_DIR
NORM.mkdir(parents=True, exist_ok=True)

# ASHE Table 8.7a is "Annual pay - Gross". Glob by pattern so next year's file works.
ASHE_EXTRACT_DIR = RAW / "ashe_extracted"
ASHE_WORKBOOK_PATTERN = "*Table 8.7a*Annual pay*Gross*.xlsx"
ASHE_CV_WORKBOOK_PATTERN = "*Table 8.7b*Annual pay*Gross*CV.xlsx"
# Full-time employees, by place of residence.
ASHE_SHEET = "Full-Time"

LONDON_LAD_REGEX = r"^E09"
LONDON_REGION_CODE = "E12000007"

PROPERTY_TYPE_LABELS = {
    "D": "Detached",
    "Detached": "Detached",
    "S": "Semi-detached",
    "Semi-detached": "Semi-detached",
    "T": "Terraced",
    "Terraced": "Terraced",
    "F": "Flat/Maisonette",
    "Flat or maisonette": "Flat/Maisonette",
    "Flat/Maisonette": "Flat/Maisonette",
}


def _extract_ashe_zip() -> None:
    zip_path = config.raw_path("ashe_table8")
    if not zip_path.exists():
        raise FileNotFoundError(f"ASHE zip not found at {zip_path}. Run download_sources.py first.")
    ASHE_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        for member in z.namelist():
            if member.lower().endswith((".csv", ".xlsx")):
                z.extract(member, path=ASHE_EXTRACT_DIR)


def _find_ashe_workbook(pattern: str) -> Path:
    """Return the single extracted ASHE workbook matching ``pattern``, extracting the zip if needed."""
    matches = list(ASHE_EXTRACT_DIR.rglob(pattern)) if ASHE_EXTRACT_DIR.exists() else []
    if not matches:
        _extract_ashe_zip()
        matches = list(ASHE_EXTRACT_DIR.rglob(pattern))
    if not matches:
        raise FileNotFoundError(f"No ASHE workbook matched {pattern} in {ASHE_EXTRACT_DIR}.")
    if len(matches) > 1:
        raise RuntimeError(f"Multiple ASHE workbooks matched {pattern}: {matches}")
    return matches[0]


def ensure_ashe_extracted() -> Path:
    """Extract the ASHE zip on first use and return the 8.7a workbook path."""
    return _find_ashe_workbook(ASHE_WORKBOOK_PATTERN)


def ashe_quality_band(cv: float | None) -> str | None:
    """Map an ASHE coefficient of variation (%) to the ONS precision bands."""
    if cv is None or pd.isna(cv):
        return None
    if cv <= 5:
        return "Precise"
    if cv <= 10:
        return "Reasonably precise"
    if cv <= 20:
        return "Acceptable"
    return "Unreliable"


def clean_col(x: str) -> str:
    return (
        str(x)
        .strip()
        .lower()
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("  ", " ")
    )


def to_numeric(df, cols):
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def normalise_property_type_label(value: str) -> str:
    return PROPERTY_TYPE_LABELS.get(str(value).strip(), str(value).strip())


def normalise_hpi_average():
    path = config.raw_path("hpi_average_prices")
    df = pd.read_csv(path, dtype=str)

    if df.shape[1] != 7:
        raise ValueError(f"HPI average prices expected 7 columns, got {df.shape[1]}")

    df.columns = [
        "date_month",
        "area_name",
        "area_code",
        "average_price",
        "pct_change_1m",
        "pct_change_12m",
        "average_price_sa",
    ]
    df["date_month"] = pd.to_datetime(df["date_month"], format="%Y-%m-%d", errors="coerce")
    df["area_name"] = df["area_name"].astype(str).str.strip()
    df["area_code"] = df["area_code"].astype(str).str.strip()
    df = to_numeric(df, ["average_price", "pct_change_1m", "pct_change_12m", "average_price_sa"])
    df = df[df["area_code"].str.match(LONDON_LAD_REGEX, na=False)]
    df.to_csv(NORM / "hpi_average_prices.csv", index=False)


def normalise_hpi_sales():
    path = config.raw_path("hpi_sales")
    df = pd.read_csv(path, dtype=str)

    if df.shape[1] != 4:
        raise ValueError(f"HPI sales expected 4 columns, got {df.shape[1]}")

    df.columns = ["date_month", "area_name", "area_code", "sales_volume"]
    df["date_month"] = pd.to_datetime(df["date_month"], format="%Y-%m-%d", errors="coerce")
    df["area_name"] = df["area_name"].astype(str).str.strip()
    df["area_code"] = df["area_code"].astype(str).str.strip()
    df = to_numeric(df, ["sales_volume"])
    df = df[df["area_code"].str.match(LONDON_LAD_REGEX, na=False)]
    df.to_csv(NORM / "hpi_sales.csv", index=False)


def normalise_hpi_property_type():
    """Defensive parser for both long-format and 4-metric grouped wide-format HPI property-type files."""
    path = config.raw_path("hpi_property_type_prices")
    df = pd.read_csv(path, dtype=str)

    property_types = ["Detached", "Semi-detached", "Terraced", "Flat/Maisonette"]

    if df.shape[1] == 8:
        df.columns = [
            "date_month",
            "area_name",
            "area_code",
            "property_type",
            "average_price",
            "hpi_index",
            "pct_change_1m",
            "pct_change_12m",
        ]
        df["property_type"] = (
            df["property_type"]
            .astype(str)
            .str.strip()
            .map(normalise_property_type_label)
        )
        out = df.copy()

    else:
        if (df.shape[1] - 3) % 4 != 0:
            raise ValueError(
                f"Unexpected property-type file shape: {df.shape[1]} columns. "
                "Inspect the raw file once and update the parser."
            )

        group_count = (df.shape[1] - 3) // 4
        headers = ["date_month", "area_name", "area_code"]
        for i in range(1, group_count + 1):
            headers += [
                f"group_{i}_price",
                f"group_{i}_index",
                f"group_{i}_pct_change_1m",
                f"group_{i}_pct_change_12m",
            ]
        df.columns = headers

        df["date_month"] = pd.to_datetime(df["date_month"], format="%Y-%m-%d", errors="coerce")
        df["area_name"] = df["area_name"].astype(str).str.strip()
        df["area_code"] = df["area_code"].astype(str).str.strip()

        metric_cols = [c for c in df.columns if c not in ["date_month", "area_name", "area_code"]]
        df = to_numeric(df, metric_cols)

        groups_to_use = list(range(1, group_count + 1))

        # If 5 groups exist, identify which one is "All" by comparing to the average-price table.
        if group_count == 5:
            avg = pd.read_csv(NORM / "hpi_average_prices.csv")
            avg["date_month"] = pd.to_datetime(avg["date_month"], errors="coerce")
            avg["area_code"] = avg["area_code"].astype(str).str.strip()
            cmp_df = df.merge(
                avg[["date_month", "area_code", "average_price"]],
                on=["date_month", "area_code"],
                how="left",
            )

            diffs = {
                i: (cmp_df[f"group_{i}_price"] - cmp_df["average_price"]).abs().mean()
                for i in groups_to_use
            }
            all_group = min(diffs, key=diffs.get)
            groups_to_use = [i for i in groups_to_use if i != all_group]

        if len(groups_to_use) != 4:
            raise ValueError(
                f"Could not resolve property-type groups cleanly. Groups found: {groups_to_use}"
            )

        frames = []
        for i, prop in zip(groups_to_use, property_types, strict=False):
            sub = df[
                [
                    "date_month",
                    "area_name",
                    "area_code",
                    f"group_{i}_price",
                    f"group_{i}_index",
                    f"group_{i}_pct_change_1m",
                    f"group_{i}_pct_change_12m",
                ]
            ].copy()
            sub.columns = [
                "date_month",
                "area_name",
                "area_code",
                "average_price",
                "hpi_index",
                "pct_change_1m",
                "pct_change_12m",
            ]
            sub["property_type"] = prop
            frames.append(sub)

        out = pd.concat(frames, ignore_index=True)

    out["date_month"] = pd.to_datetime(out["date_month"], format="%Y-%m-%d", errors="coerce")
    out["area_name"] = out["area_name"].astype(str).str.strip()
    out["area_code"] = out["area_code"].astype(str).str.strip()
    out = to_numeric(out, ["average_price", "hpi_index", "pct_change_1m", "pct_change_12m"])
    out = out[out["area_code"].str.match(LONDON_LAD_REGEX, na=False)]
    out = out.dropna(subset=["date_month", "area_code", "property_type"])
    out.to_csv(NORM / "hpi_property_type_prices.csv", index=False)


def normalise_pipr():
    pipr_file = config.raw_path("pipr_monthly_price_statistics")
    df = pd.read_excel(pipr_file, sheet_name="Table 1", header=2)
    df.columns = [clean_col(c) for c in df.columns]

    out = df[["time period", "area name", "area code", "rental price", "annual change"]].copy()
    out.columns = ["date_month", "area_name", "area_code", "avg_monthly_rent", "rent_yoy_pct"]

    out["date_month"] = pd.to_datetime(out["date_month"], errors="coerce")
    out["area_name"] = out["area_name"].astype(str).str.strip()
    out["area_code"] = out["area_code"].astype(str).str.strip()
    out = to_numeric(out, ["avg_monthly_rent", "rent_yoy_pct"])
    out["rent_yoy_pct"] = out["rent_yoy_pct"].round(2)

    out = out[out["area_code"].str.match(LONDON_LAD_REGEX, na=False)]
    out = out.dropna(subset=["date_month", "area_code"])
    out.to_csv(NORM / "pipr_local_rents.csv", index=False)


def _read_ashe_full_time(workbook: Path) -> pd.DataFrame:
    df = pd.read_excel(workbook, sheet_name=ASHE_SHEET, header=4)
    df.columns = [clean_col(c) for c in df.columns]
    df["code"] = df["code"].astype(str).str.strip()
    df["description"] = df["description"].astype(str).str.strip()
    return df


def normalise_ashe():
    """London borough + London region median full-time gross annual pay, with ONS CV quality."""
    workbook = ensure_ashe_extracted()
    match = re.search(r"(20\d{2})", workbook.name)
    if not match:
        raise ValueError(f"Could not extract year from {workbook.name}")

    pay = _read_ashe_full_time(workbook)
    out = pay[["description", "code", "median", "change"]].copy()
    out.columns = ["area_name", "area_code", "median_gross_annual_pay", "earnings_yoy_pct"]

    cv = _read_ashe_full_time(_find_ashe_workbook(ASHE_CV_WORKBOOK_PATTERN))
    cv = cv[["code", "median"]].rename(columns={"code": "area_code", "median": "median_pay_cv"})
    out = out.merge(cv, on="area_code", how="left")

    out = out[
        out["area_code"].str.match(LONDON_LAD_REGEX, na=False) | out["area_code"].eq(LONDON_REGION_CODE)
    ]
    out = to_numeric(out, ["median_gross_annual_pay", "earnings_yoy_pct", "median_pay_cv"])
    out.insert(0, "reference_year", int(match.group(1)))
    out["earnings_quality"] = out["median_pay_cv"].map(ashe_quality_band)

    # ONS suppresses unreliable estimates (e.g. City of London) with "x"; those rows fall back to London.
    out = out.dropna(subset=["median_gross_annual_pay"])
    out.to_csv(NORM / "ashe_earnings.csv", index=False)


if __name__ == "__main__":
    normalise_hpi_average()
    normalise_hpi_sales()
    normalise_hpi_property_type()
    normalise_pipr()
    normalise_ashe()
    print("[done] normalised London files written to data/normalised")
