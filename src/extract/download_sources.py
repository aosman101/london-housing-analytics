import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import config  # noqa: E402

RAW_DIR = config.RAW_DIR
RAW_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}


def download_file(name: str, url: str) -> None:
    dest: Path = RAW_DIR / name
    tmp_dest = dest.with_suffix(f"{dest.suffix}.part")
    if dest.exists():
        print(f"[skip] {name} already exists")
        return

    print(f"[download] {name}")
    with requests.get(url, headers=HEADERS, stream=True, timeout=120) as r:
        r.raise_for_status()
        with tmp_dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    tmp_dest.replace(dest)


def main() -> None:
    cfg = config.load()
    for spec in cfg["sources"].values():
        name = config.render(spec["filename_template"], cfg)
        url = config.render(spec["url_template"], cfg)
        download_file(name, url)
    print("[done] raw source download complete")


if __name__ == "__main__":
    main()
