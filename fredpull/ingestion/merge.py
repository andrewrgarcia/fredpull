import pandas as pd
import glob
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

CHUNK_DIR = os.path.join(BASE_DIR, "data/raw_chunks")
OUTPUT_PATH = os.path.join(BASE_DIR, "data/raw/fred_monthly_master.parquet")


def load_chunk_files():
    files = sorted(
        glob.glob(f"{CHUNK_DIR}/chunk_*.parquet"),
        key=lambda x: int(x.split("_")[-1].split(".")[0])
    )

    print("Merging", len(files), "chunks...")
    return files


def merge_chunks(files):
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, axis=1)

    print("Merged shape:", df.shape)
    return df


def enforce_monthly_grid(df):
    full_index = pd.date_range(df.index.min(), df.index.max(), freq="MS")
    df = df.reindex(full_index)

    print("After grid:", df.shape)
    return df


def main():
    files = load_chunk_files()

    if not files:
        print("No chunks found.")
        return

    df = merge_chunks(files)
    df = enforce_monthly_grid(df)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH)

    print("Saved:", OUTPUT_PATH)


if __name__ == "__main__":
    main()