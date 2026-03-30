import os
import pandas as pd

from fredpull.metadata.crawl_categories import crawl_categories
from fredpull.metadata.crawl_releases import crawl_all_releases

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
META_DIR = os.path.join(BASE_DIR, "data/metadata")

CAT_PATH = os.path.join(META_DIR, "fred_series_metadata.parquet")
REL_PATH = os.path.join(META_DIR, "fred_release_series.parquet")
FULL_PATH = os.path.join(META_DIR, "fred_full_metadata.parquet")


def ensure_category_metadata():
    if os.path.exists(CAT_PATH):
        return pd.read_parquet(CAT_PATH)

    crawl_categories()
    return pd.read_parquet(CAT_PATH)


def ensure_release_metadata():
    if os.path.exists(REL_PATH):
        return pd.read_parquet(REL_PATH)

    crawl_all_releases()
    return pd.read_parquet(REL_PATH)


def main():
    os.makedirs(META_DIR, exist_ok=True)

    cat = ensure_category_metadata()
    rel = ensure_release_metadata()

    meta = pd.concat([cat, rel]).drop_duplicates("id")
    meta.to_parquet(FULL_PATH)

    print("Saved full metadata:", len(meta))


if __name__ == "__main__":
    main()