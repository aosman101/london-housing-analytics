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
ASHE_SLICE = RAW / "ashe_table8_median_gross_annual_pay.csv"

LONDON_LAD_REGEX = r"^E09"
LONDON_REGION_CODE = "E12000007"


def ensure_ashe_extracted() -> Path:
    """Extract the ASHE zip on first use and return the 8.7a workbook path."""
    zip_path = config.raw_path("ashe_table8")
    if not zip_path.exists():
        raise FileNotFoundError(
            f"ASHE zip not found at {zip_path}. Run download_sources.py first."
        )

    ASHE_EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    matches = list(ASHE_EXTRACT_DIR.rglob(ASHE_WORKBOOK_PATTERN))
    if not matches:
        with zipfile.ZipFile(zip_path, "r") as z:
            for m in z.namelist():
                if m.lower().endswith((".csv", ".xlsx")):
                    z.extract(m, path=ASHE_EXTRACT_DIR)
        matches = list(ASHE_EXTRACT_DIR.rglob(ASHE_WORKBOOK_PATTERN))

    if not matches:
        raise FileNotFoundError(
            f"No ASHE workbook matched {ASHE_WORKBOOK_PATTERN} after extracting {zip_path.name}."
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple ASHE workbooks matched {ASHE_WORKBOOK_PATTERN}: {matches}"
        )
    return matches[0]


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


def normalise_hpi_average():
    path = config.raw_path("hpi_average_prices")
    df = pd.read_csv(path, header=None)

    if df.shape[1] != 7:
        raise ValueError(f"HPI average prices expected 7 columns, got {df.shape[1]}")

    df.columns = [
        "date_month",
        "area_name",
        "area_code",
        "average_price",
        "hpi_index",
        "pct_change_1m",
        "pct_change_12m",
    ]
    df["date_month"] = pd.to_datetime(df["date_month"], errors="coerce")
    df["area_name"] = df["area_name"].astype(str).str.strip()
    df["area_code"] = df["area_code"].astype(str).str.strip()
    df = to_numeric(df, ["average_price", "hpi_index", "pct_change_1m", "pct_change_12m"])
    df = df[df["area_code"].str.match(LONDON_LAD_REGEX, na=False)]
    df.to_csv(NORM / "hpi_average_prices.csv", index=False)


def normalise_hpi_sales():
    path = config.raw_path("hpi_sales")
    df = pd.read_csv(path, header=None)

    if df.shape[1] != 4:
        raise ValueError(f"HPI sales expected 4 columns, got {df.shape[1]}")

    df.columns = ["date_month", "area_name", "area_code", "sales_volume"]
    df["date_month"] = pd.to_datetime(df["date_month"], errors="coerce")
    df["area_name"] = df["area_name"].astype(str).str.strip()
    df["area_code"] = df["area_code"].astype(str).str.strip()
    df = to_numeric(df, ["sales_volume"])
    df = df[df["area_code"].str.match(LONDON_LAD_REGEX, na=False)]
    df.to_csv(NORM / "hpi_sales.csv", index=False)


def normalise_hpi_property_type():
    """Defensive parser for both long-format and 4-metric grouped wide-format HPI property-type files."""
    path = config.raw_path("hpi_property_type_prices")
    df = pd.read_csv(path, header=None)

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
            .replace({
                "D": "Detached",
                "S": "Semi-detached",
                "T": "Terraced",
                "F": "Flat/Maisonette",
                "Flat or maisonette": "Flat/Maisonette",
                "Flat/Maisonette": "Flat/Maisonette",
            })
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

        df["date_month"] = pd.to_datetime(df["date_month"], errors="coerce")
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
        for i, prop in zip(groups_to_use, property_types):
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

    out["date_month"] = pd.to_datetime(out["date_month"], errors="coerce")
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

    out = out[out["area_code"].str.match(LONDON_LAD_REGEX, na=False)]
    out = out.dropna(subset=["date_month", "area_code"])
    out.to_csv(NORM / "pipr_local_rents.csv", index=False)


def extract_ashe_slice():
    workbook = ensure_ashe_extracted()
    df = pd.read_excel(workbook, sheet_name="Full-Time", header=4)
    df.columns = [clean_col(c) for c in df.columns]

    match = re.search(r"(20\d{2})", workbook.name)
    if not match:
        raise ValueError(f"Could not extract year from {workbook.name}")

    out = df[["description", "code", "median"]].copy()
    out.columns = ["area name", "area code", "median gross annual pay"]
    out["year"] = int(match.group(1))
    out = out[["year", "area name", "area code", "median gross annual pay"]]

    out["area name"] = out["area name"].astype(str).str.strip()
    out["area code"] = out["area code"].astype(str).str.strip()
    out["median gross annual pay"] = pd.to_numeric(
        out["median gross annual pay"], errors="coerce"
    )

    out = out[out["area code"].str.match(LONDON_LAD_REGEX, na=False)]
    out.to_csv(ASHE_SLICE, index=False)


def normalise_ashe():
    if not ASHE_SLICE.exists():
        extract_ashe_slice()

    df = pd.read_csv(ASHE_SLICE)
    df.columns = [clean_col(c) for c in df.columns]

    out = df[["year", "area name", "area code", "median gross annual pay"]].copy()
    out.columns = ["reference_year", "area_name", "area_code", "median_gross_annual_pay"]

    out["reference_year"] = pd.to_numeric(out["reference_year"], errors="coerce").astype("Int64")
    out["area_name"] = out["area_name"].astype(str).str.strip()
    out["area_code"] = out["area_code"].astype(str).str.strip()
    out["median_gross_annual_pay"] = pd.to_numeric(
        out["median_gross_annual_pay"], errors="coerce"
    )

    workbook = ensure_ashe_extracted()
    london_region = pd.read_excel(workbook, sheet_name="Full-Time", header=4)
    london_region.columns = [clean_col(c) for c in london_region.columns]
    london_region = london_region[
        london_region["code"].astype(str).str.strip().eq(LONDON_REGION_CODE)
    ][["description", "code", "median"]].copy()
    london_region.columns = ["area_name", "area_code", "median_gross_annual_pay"]
    london_region["reference_year"] = out["reference_year"].dropna().max()
    london_region = london_region[
        ["reference_year", "area_name", "area_code", "median_gross_annual_pay"]
    ]
    london_region["area_name"] = london_region["area_name"].astype(str).str.strip()
    london_region["area_code"] = london_region["area_code"].astype(str).str.strip()
    london_region["median_gross_annual_pay"] = pd.to_numeric(
        london_region["median_gross_annual_pay"], errors="coerce"
    )

    out = pd.concat([out, london_region], ignore_index=True)
    out = out[
        out["area_code"].str.match(LONDON_LAD_REGEX, na=False)
        | out["area_code"].eq(LONDON_REGION_CODE)
    ]
    out = out.dropna(subset=["reference_year", "area_code", "median_gross_annual_pay"])
    out.to_csv(NORM / "ashe_earnings.csv", index=False)


if __name__ == "__main__":
    ensure_ashe_extracted()
    normalise_hpi_average()
    normalise_hpi_sales()
    normalise_hpi_property_type()
    normalise_pipr()
    extract_ashe_slice()
    normalise_ashe()
    print("[done] normalised London files written to data/normalised")
