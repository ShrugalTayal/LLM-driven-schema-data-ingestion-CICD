# src/scraper/extract.py
import os, argparse, requests
from urllib.parse import urlparse
from pathlib import Path

OUT = Path("data/raw")
OUT.mkdir(parents=True, exist_ok=True)

def to_raw_github(url: str) -> str:
    # Convert blob URLs to raw where possible
    if "github.com" in url and "/blob/" in url:
        return url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
    return url

def download(url: str):
    url = to_raw_github(url)
    parsed = urlparse(url)
    name = Path(parsed.path).name or "download"
    local = OUT / name
    try:
        r = requests.get(url, stream=True, timeout=30)
        r.raise_for_status()
        with open(local, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        print("Saved", local)
        return str(local)
    except Exception as e:
        print("Failed to download", url, e)
        return None

def main(urls_file):
    with open(urls_file) as f:
        for u in [x.strip() for x in f if x.strip()]:
            if "kaggle.com" in u:
                print("Kaggle link (manual or Kaggle CLI required):", u)
                continue
            download(u)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--urls_file", required=True)
    args = p.parse_args()
    main(args.urls_file)