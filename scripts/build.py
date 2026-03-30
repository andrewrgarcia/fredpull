import argparse
import os

from fredpull.metadata.merge_metadata import main as run_metadata
from fredpull.ingestion.download import main as run_download
from fredpull.ingestion.merge import main as run_merge


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="1950-01-01")
    args = parser.parse_args()

    os.environ["FRED_START_DATE"] = args.start

    print("\n[1/3] metadata")
    run_metadata()

    print("\n[2/3] download")
    run_download()

    print("\n[3/3] merge")
    run_merge()

    print("\nDONE")


if __name__ == "__main__":
    main()