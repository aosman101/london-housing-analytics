from pathlib import Path
import zipfile
import pandas as pd
from openpyxl import load_workbook

RAW = Path("data/raw")


def preview_excel(path: Path, max_rows: int = 8) -> None:
    print(f"\n===== {path.name} =====")
    wb = load_workbook(path, read_only=True, data_only=True)
    print("Sheets:")
    for s in wb.sheetnames:
        print(f"  - {s}")
    wb.close()

    for s in load_workbook(path, read_only=True).sheetnames[:8]:
        print(f"\n--- Preview: {s} ---")
        try:
            sample = pd.read_excel(path, sheet_name=s, header=None, nrows=max_rows)
            print(sample.to_string(index=False, header=False))
        except Exception as e:
            print(f"Preview failed for {s}: {e}")


def inspect_ashe_zip(path: Path) -> None:
    print(f"\n===== {path.name} =====")
    with zipfile.ZipFile(path, "r") as z:
        members = z.namelist()
        print("ZIP contents:")
        for m in members:
            print(f"  - {m}")

        extract_dir = RAW / "ashe_extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)

        for m in members:
            if m.lower().endswith(".csv") or m.lower().endswith(".xlsx"):
                z.extract(m, path=extract_dir)
                print(f"[extracted] {m}")

        for p in extract_dir.rglob("*"):
            if p.suffix.lower() in [".xlsx", ".xlsm"]:
                preview_excel(p)


if __name__ == "__main__":
    pipr = RAW / "pipr_monthly_price_statistics_2026_03.xlsx"
    ashe_zip = RAW / "ashe_table8_2025_provisional.zip"

    if pipr.exists():
        preview_excel(pipr)

    if ashe_zip.exists():
        inspect_ashe_zip(ashe_zip)
