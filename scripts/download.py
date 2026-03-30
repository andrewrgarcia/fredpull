import argparse
import os

from fredpull.ingestion.download import main as run_download


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="1950-01-01")
    args = parser.parse_args()

    os.environ["FRED_START_DATE"] = args.start
    run_download()


if __name__ == "__main__":
    main()