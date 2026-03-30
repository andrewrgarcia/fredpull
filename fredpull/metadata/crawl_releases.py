import pandas as pd
import os
from fredpull.clients.fred_http import fred_get

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
META_DIR = os.path.join(BASE_DIR, "data/metadata")


def get_releases():
    data = fred_get("releases", {"limit": 1000})
    return data["releases"]


def get_release_series(release_id):
    all_series = []
    offset = 0
    limit = 1000

    while True:
        data = fred_get("release/series", {
            "release_id": release_id,
            "limit": limit,
            "offset": offset
        })

        series_data = data["seriess"]

        if not series_data:
            break

        all_series.extend(series_data)

        if len(series_data) < limit:
            break

        offset += limit

    return all_series


def crawl_all_releases():
    releases = get_releases()
    all_series = []

    for r in releases:
        rid = r["id"]

        try:
            series = get_release_series(rid)
            all_series.extend(series)
        except Exception:
            pass

    df = pd.DataFrame(all_series).drop_duplicates("id")

    os.makedirs(META_DIR, exist_ok=True)
    df.to_parquet(os.path.join(META_DIR, "fred_release_series.parquet"))

    print("Saved release metadata:", len(df))


if __name__ == "__main__":
    crawl_all_releases()