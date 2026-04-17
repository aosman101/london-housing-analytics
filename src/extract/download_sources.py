from pathlib import Path
import requests

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Current editions visible on 17 Apr 2026.
# Update these month-stamped URLs after future HPI/PIPR releases.
SOURCES = {
    "hpi_average_prices_2026_01.csv": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Average-prices-2026-01.csv?utm_campaign=average_price&utm_medium=GOV.UK&utm_source=datadownload&utm_term=9.30_25_03_26",
    "hpi_property_type_prices_2026_01.csv": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Average-prices-Property-Type-2026-01.csv?utm_campaign=average_price_property_price&utm_medium=GOV.UK&utm_source=datadownload&utm_term=9.30_25_03_26",
    "hpi_sales_2026_01.csv": "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/Sales-2026-01.csv?utm_campaign=sales&utm_medium=GOV.UK&utm_source=datadownload&utm_term=9.30_25_03_26",
    "pipr_monthly_price_statistics_2026_03.xlsx": "https://www.ons.gov.uk/file?uri=%2Feconomy%2Finflationandpriceindices%2Fdatasets%2Fpriceindexofprivaterentsukmonthlypricestatistics%2F25march2026%2Fpriceindexofprivaterentsukmonthlypricestatistics.xlsx",
    "ashe_table8_2025_provisional.zip": "https://www.ons.gov.uk/file?uri=%2Femploymentandlabourmarket%2Fpeopleinwork%2Fearningsandworkinghours%2Fdatasets%2Fplaceofresidencebylocalauthorityashetable8%2F2025provisional%2Fashetable82025provisional.zip",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def download_file(name: str, url: str) -> None:
    dest = RAW_DIR / name
    if dest.exists():
        print(f"[skip] {name} already exists")
        return

    print(f"[download] {name}")
    with requests.get(url, headers=HEADERS, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


if __name__ == "__main__":
    for name, url in SOURCES.items():
        download_file(name, url)
    print("[done] raw source download complete")
