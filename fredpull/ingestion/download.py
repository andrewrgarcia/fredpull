import pandas as pd
from tqdm import tqdm
import os
import glob
import time

from fredpull.clients.fred_client import get_fred
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------------------------------
# CONFIG (env override supported)
# -------------------------------------------------

START_DATE = os.getenv("FRED_START_DATE", "1950-01-01")
MIN_OBS = int(os.getenv("FRED_MIN_OBS", "180"))
CHUNK_SIZE = int(os.getenv("FRED_CHUNK_SIZE", "400"))

RATE_SLEEP = 0.6
RATE_LIMIT_SLEEP = 30
NETWORK_SLEEP = 60
MAX_RETRIES = 5

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

META_PATH = os.path.join(BASE_DIR, "data/metadata/fred_full_metadata.parquet")
CHUNK_DIR = os.path.join(BASE_DIR, "data/raw_chunks")


# -------------------------------------------------
# METADATA FILTER
# -------------------------------------------------

def load_filtered_metadata(meta_path: str, start_date: str, chunk_size: int):
    print("Loading metadata...")

    meta = pd.read_parquet(meta_path)

    meta = meta[
        (meta["frequency"] == "Monthly") &
        (meta["observation_start"] < start_date) &
        (meta["observation_end"] > "2022-01-01")
    ]

    ids = meta["id"].tolist()

    total = len(ids)
    expected_chunks = (total + chunk_size - 1) // chunk_size

    print("Total candidate series:", total)
    print("Expected chunks:", expected_chunks)

    return ids, total, expected_chunks


# -------------------------------------------------
# CHUNK HANDLING
# -------------------------------------------------

def detect_existing_chunks(chunk_dir: str):
    existing_files = glob.glob(f"{chunk_dir}/chunk_*.parquet")

    existing_chunks = set(
        int(f.split("_")[-1].split(".")[0])
        for f in existing_files
    )

    print("Existing chunks:", sorted(existing_chunks))
    return existing_chunks


def save_chunk_atomic(data: dict, chunk_idx: int, chunk_dir: str):
    df = pd.DataFrame(data)

    tmp_file = f"{chunk_dir}/chunk_{chunk_idx}.parquet.tmp"
    final_file = f"{chunk_dir}/chunk_{chunk_idx}.parquet"

    df.to_parquet(tmp_file)
    os.replace(tmp_file, final_file)

    print("Saved:", final_file, df.shape)


# -------------------------------------------------
# FETCH
# -------------------------------------------------

def fetch_one_series(sid, fred, start_date, min_obs):
    retries = 0

    while retries < MAX_RETRIES:
        try:
            s = fred.get_series(sid)

            if s is None or len(s) == 0:
                return None

            s = s[s.index >= start_date]

            if len(s) == 0:
                return None

            s.index = s.index.to_period("M").to_timestamp()
            s = s.groupby(s.index).last()

            if s.count() < min_obs:
                return None

            return sid, s

        except Exception as e:
            msg = str(e)

            if "Too Many Requests" in msg:
                time.sleep(RATE_LIMIT_SLEEP)
            elif "Temporary failure" in msg or "Name or service not known" in msg:
                time.sleep(NETWORK_SLEEP)
            else:
                return None

            retries += 1

    return None


# -------------------------------------------------
# DOWNLOAD LOOP
# -------------------------------------------------

def run_download_loop(ids, total, expected_chunks, existing_chunks, fred):
    MAX_WORKERS = 6

    for chunk_idx in range(expected_chunks):
        if chunk_idx in existing_chunks:
            continue

        start = chunk_idx * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, total)

        batch_ids = ids[start:end]

        if not batch_ids:
            break

        print(f"\nDownloading chunk {chunk_idx} ({start}:{end})")

        data = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(fetch_one_series, sid, fred, START_DATE, MIN_OBS)
                for sid in batch_ids
            ]

            for future in tqdm(as_completed(futures), total=len(futures)):
                result = future.result()
                if result:
                    sid, series = result
                    data[sid] = series

        save_chunk_atomic(data, chunk_idx, CHUNK_DIR)

    print("\nDownload complete.")


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    os.makedirs(CHUNK_DIR, exist_ok=True)

    fred = get_fred()

    ids, total, expected_chunks = load_filtered_metadata(
        META_PATH,
        START_DATE,
        CHUNK_SIZE
    )

    existing_chunks = detect_existing_chunks(CHUNK_DIR)

    run_download_loop(
        ids,
        total,
        expected_chunks,
        existing_chunks,
        fred
    )


if __name__ == "__main__":
    main()