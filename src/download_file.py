from pathlib import Path
import urllib.request
import gzip
import shutil
import os

def download_if_missing(url: str, save_path: Path):
    save_path.parent.mkdir(parents=True, exist_ok=True)
    if save_path.exists():
        print(f"Exists, skipping download: {save_path}")
        return
    print(f"Downloading {url} to {save_path}")
    urllib.request.urlretrieve(url, save_path)
    print(f"Downloaded {url} to {save_path}")

def gunzip_file(gzip_path: Path, keep_gz:bool = True) -> Path:
    out_path = gzip_path.with_suffix('')  # remove .gz and makes .jsonl
    if out_path.exists():
        print(f"Exists, skipping gunzip: {out_path}")
        return out_path
    if not gzip_path.exists():
        raise FileNotFoundError(f"Gzip file not found: {gzip_path}")

    print(f"Gunzipping {gzip_path} to {out_path}")
    with gzip.open(gzip_path, 'rb') as f_in:
        with open(out_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if not keep_gz:
        os.remove(gzip_path)
    print(f"Gunzipped {gzip_path} to {out_path}")
    return out_path