from pathlib import Path
import pandas as pd
from .config import RAW_DIR, PROCESSED_DIR, URLS
from .download_file import download_if_missing, gunzip_file


def ensure_raw_files() -> dict[str, Path]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    unzipped = {}
    for gz_name, url in URLS.items():
        gz_path = RAW_DIR / gz_name
        jsonl_path = gz_path.with_suffix("")  # remove .gz

        # If jsonl already exists, skip download+unzip
        if jsonl_path.exists():
            print(f"Raw exists: {jsonl_path}")
            unzipped[jsonl_path.name] = jsonl_path
            continue

        # Otherwise download gz if missing, then unzip
        download_if_missing(url, gz_path)
        out = gunzip_file(gz_path, keep_gz=True)
        unzipped[out.name] = out

    return unzipped

def load_reviews_df(force_reload: bool = False) -> pd.DataFrame:
    ensure_raw_files()
    path = RAW_DIR / "All_Beauty.jsonl"
    df = pd.read_json(path, lines=True,  dtype={"timestamp": "int64"})
    print(df["timestamp"].head())
    print(df["timestamp"].dtype)
    return df

def load_meta_df(force_reload: bool = False) -> pd.DataFrame:
    ensure_raw_files()
    path = RAW_DIR / "meta_All_Beauty.jsonl"
    df = pd.read_json(path, lines=True)
    return df

def save_processed_parquet():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    reviews = load_reviews_df()
    meta = load_meta_df()

    print(reviews["timestamp"].head())
    print(reviews["timestamp"].dtype)
    
    # Force timestamp to stay INT64
    if "timestamp" in reviews.columns:
        reviews["timestamp"] = reviews["timestamp"].astype("int64")

    # Write Parquet safely
    reviews.to_parquet(
        PROCESSED_DIR / "reviews_raw.parquet",
        index=False,
        engine="pyarrow"
    )
    
    meta.to_parquet(
        PROCESSED_DIR / "meta_raw.parquet",
        index=False,
        engine="pyarrow"
    )
    
    print("Saved Parquet files cleanly with original timestamp preserved.")
'''def save_processed_parquet() -> None:'
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    reviews = load_reviews_df()
    meta = load_meta_df()

    reviews.to_parquet(PROCESSED_DIR / "reviews_raw.parquet", index=False, engine="pyarrow", coerce_timestamps="us")
    meta.to_parquet(PROCESSED_DIR / "meta_raw.parquet", index=False, engine="pyarrow", coerce_timestamps="us")
    print(f"Saved parquet to: {PROCESSED_DIR}")'''

