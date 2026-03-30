PYTHON := uv run

START ?= 1950-01-01

.PHONY: help sync metadata download merge build clean dirs

help:
	@echo "Targets:"
	@echo "  make sync                 Install dependencies with uv"
	@echo "  make metadata             Build metadata parquet"
	@echo "  make download START=...   Download chunks from FRED"
	@echo "  make merge                Merge chunk parquet files"
	@echo "  make build START=...      metadata + download + merge"
	@echo "  make dirs                 Create data directories"
	@echo "  make clean                Remove generated chunk/tmp files"

dirs:
	mkdir -p data/metadata data/raw_chunks data/raw data/processed

metadata: dirs
	$(PYTHON) scripts/metadata.py

download: dirs
	FRED_START_DATE=$(START) $(PYTHON) scripts/download.py --start $(START)

merge: dirs
	$(PYTHON) scripts/merge.py

build: metadata download merge
	@echo "DONE"

clean:
	rm -f data/raw_chunks/*.parquet.tmp