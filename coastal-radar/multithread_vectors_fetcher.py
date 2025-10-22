"""
Multithreaded Vector Data Fetcher
=================================

Fetches hourly CODAR surface-current vector files from Ocean Networks Canada (ONC)
for a given date range, downloading them in parallel and maintaining a persistent
manifest of progress.

Overview
--------
This script downloads .tuv vector files for a fixed location (SOGCS) between
user-specified start and end times. It creates or updates a manifest CSV that
records each filename attempted and whether the download succeeded or failed.
Failed or missing files are retried automatically on the next run. A provenance
YAML file logs API parameters and metadata about the manifest.

Directory layout (after successful run)
--------------------------------------
surface-currents/
    vectors/
        <ONC_filename>.tuv
        ...
surface-currents-metadata/
    vectors/
        manifest.csv      # ["timestamp","locationCode","deviceCategoryCode",
                          #  "deviceCode","filename","path","status"]
        provenance.yaml   # challenge name, API call details, manifest summary

Key features
------------
- Multithreaded downloads using Python threads
- Periodic autosave of manifest during long runs
- Safe to interrupt and resume later
- Provenance tracking for reproducibility

Environment
-----------
- Python 3.11+
- Environment variable: ``ONC_TOKEN`` must be set to a valid ONC API key
- Dependencies: ``onc``, ``pandas``, ``pyyaml``, ``python-dotenv``

Usage
-----
Run from the repository root or from within ``surface-currents/``:

    export ONC_TOKEN=<your_api_key>
    python multithread_vector_fetcher.py \
        --start 2023-01-01T00:00:00.000Z \
        --end   2023-01-02T00:00:00.000Z \
        --workers 10

Arguments
---------
--start     ISO-8601 start timestamp (required)
--end       ISO-8601 end timestamp (required)
--workers   Number of parallel download threads (default: 10)

Outputs
-------
- ``surface-currents-metadata/manifest.csv``  – records every attempted file and status
- ``surface-currents-metadata/provenance.yaml`` – documents API parameters and manifest summary
- Downloaded .tuv files under ``surface-currents/vectors``

Notes
-----
If the script stops or fails midway, rerun it with the same date range. The
manifest ensures that previously completed downloads are skipped, and failed
ones are retried. To restart from scratch, delete the manifest file before
rerunning.
"""


from __future__ import annotations

import os
import queue
import threading
import time
from pathlib import Path
from datetime import UTC, datetime

import onc
import pandas as pd
import yaml
from dotenv import load_dotenv

import logging
import sys
import argparse
from dataclasses import dataclass


# ==============================
# Setup
# ==============================


@dataclass(frozen=True)
class Config:
    start: str
    end: str
    workers: int

def parse_args() -> Config:
    """Parse command-line arguments for date range and worker count."""
    parser = argparse.ArgumentParser(
        description="Fetch surface current vector data from ONC."
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (ISO 8601), e.g. 2023-01-01T00:00:00.000Z"
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date (ISO 8601), e.g. 2023-01-02T00:00:00.000Z"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=15,
        help="Number of concurrent download threads (default: 15)"
    )

    args = parser.parse_args()
    return Config(start=args.start, end=args.end, workers=args.workers)


load_dotenv()

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,  # can change to DEBUG for more detail
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],  # ensure console
        force=True,  # <- override prior logging config from other libs
    )

log = logging.getLogger(__name__)

setup_logging()
log.info("Starting surface current vectors downloader...")

DOWNLOADS_PATH = Path.home() / "Downloads"
DATA_ROOT = DOWNLOADS_PATH / "surface-currents/vectors"
METADATA_ROOT = DOWNLOADS_PATH / "surface-currents-metadata/vectors"

MANIFEST_PATH = METADATA_ROOT / "manifest.csv"
PROVENANCE_PATH = METADATA_ROOT / "provenance.yaml"

# Ensure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# ONC client
TOKEN = os.getenv("ONC_TOKEN")
if not TOKEN:
    raise RuntimeError("ONC_TOKEN is not set. Please export your ONC API key before running.")

VECTOR_CLIENT = onc.ONC(TOKEN, outPath=str(DATA_ROOT))

# Globals
API_PARAMS: dict = {}  # For provenance

DOWNLOAD_QUEUE: "queue.Queue[pd.Series | dict]" = queue.Queue()
MANIFEST_DF: pd.DataFrame
FILES_SUCCESS: int = 0
FILES_FAILED: int = 0
DEFAULT_LOCATION = "SOGCS"

manifest_lock = threading.Lock()

MANIFEST_COLUMNS = [
    "timestamp",
    "locationCode",
    "deviceCategoryCode",
    "deviceCode",
    "filename",
    "path",
    "status",
]

CODAR_PARAMS = {
    "locationCode": DEFAULT_LOCATION,
    "deviceCategoryCode": "OCEANOGRAPHICRADAR",
    "dataProductCode": "CODARCD",
    "fileExtension": "tuv", # NOTE: for vectors
}


# ==============================
# Functions
# ==============================

def utc_now_isoz() -> str:
    """Return current UTC time as ISO 8601 string with 'Z' suffix."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")

def load_or_init_manifest() -> None:
    """Load global MANIFEST_DF from CSV or initialize a new empty DataFrame.

    Schema:
        timestamp (datetime64[ns, UTC]),
        locationCode (str),
        deviceCategoryCode (str),
        deviceCode (str),
        filename (str),
        path (str, local path from data root),
        status (str | NaN)
    """
    global MANIFEST_DF

    if MANIFEST_PATH.exists():
        MANIFEST_DF = pd.read_csv(MANIFEST_PATH, parse_dates=["timestamp"])
        # Ensure column order and presence
        for col in MANIFEST_COLUMNS:
            if col not in MANIFEST_DF.columns:
                MANIFEST_DF[col] = pd.NA
        MANIFEST_DF = MANIFEST_DF[MANIFEST_COLUMNS]
    else:
        MANIFEST_DF = pd.DataFrame(columns=MANIFEST_COLUMNS)


def get_filenames(dateFrom: str, dateTo: str) -> list[str]:
    """Return list of filenames via ONC archive listing.

    Example filename:
        "RDLm12345_20231123T234501.000Z.tuv"
    """
    global DEFAULT_LOCATION

    params = CODAR_PARAMS | {"dateFrom": dateFrom, "dateTo": dateTo} # Append with global

    response = VECTOR_CLIENT.getArchivefileByLocation(params)
    filenames = response.get("files", [])

    log.info(
        f"[get_filenames] Requesting files for {DEFAULT_LOCATION} "
        f"{dateFrom} → {dateTo}."
    )
    log.info(f"[get_filenames] Files returned: {len(filenames)}\n")

    # Update dict for provenance (do not include token)
    global API_PARAMS
    API_PARAMS = params.copy()

    return filenames


def filenames_to_file_info(filenames: list[str]) -> pd.DataFrame:
    """Parse filenames into a metadata DataFrame (per-call manifest).

    Input filename format:
        "<deviceCode>_<YYYYMMDDTHHMMSS.mmmZ>.tuv"

    Output schema:
        ["timestamp","locationCode","deviceCategoryCode",
         "deviceCode","filename","path"]
    """
    global DEFAULT_LOCATION

    log.info("[filenames_to_file_info] Building file info table.\n")

    rows: list[dict] = []

    for fname in filenames:
        # Extract timestamp between first underscore and extension
        # Example: "DEV123_20231123T234501.000Z.tuv"
        parts = fname.split("_", 1)
        device_code = parts[0]
        ts_str = parts[1].removesuffix(".tuv")
        ts = pd.to_datetime(ts_str, utc=True)

        path = Path(DEFAULT_LOCATION) / fname

        rows.append(
            {
                "timestamp": ts,
                "locationCode": DEFAULT_LOCATION,
                "deviceCategoryCode": "OCEANOGRAPHICRADAR",
                "deviceCode": device_code,
                "filename": fname,
                "path": str(path),
            }
        )

    return pd.DataFrame(rows)


def write_provenance() -> None:
    """Write provenance YAML, including API parameters and manifest summary."""
    safe_params = {k: v for k, v in API_PARAMS.items() if k not in {"token", "method"}}

    provenance = {
        "challenge": "surface-currents",
        "data-type": "vector",
        "doi": "10.34943/d32df30d-7644-446a-b0ed-9daf63377041",
        "api": {
            "base_url": "https://data.oceannetworks.ca/api",
            "getListByLocation": {
                "endpoint": "/archivefile/location",
                "parameters": safe_params,
            },
            "getFile": {
                "endpoint": "/archivefile/download",
                "parameters": {"filename": "<filename from manifest>"},
            },
        },
        "manifest": {
            "path": str(MANIFEST_PATH),
            "last_updated": utc_now_isoz(),
            "file_count": int(len(MANIFEST_DF)),
        },
    }

    with open(PROVENANCE_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(provenance, f, sort_keys=False)

    log.info(f"[write_provenance] Wrote provenance → {PROVENANCE_PATH}")



def write_manifest() -> None:
    """Save the global manifest DataFrame to CSV."""
    with manifest_lock:
        MANIFEST_DF.to_csv(MANIFEST_PATH, index=False)
    log.info(f"[write_manifest] Saved manifest → {MANIFEST_PATH} "
             f"({len(MANIFEST_DF)} rows)")


# ==============================
# Thread workers
# ==============================

def worker() -> None:
    """Worker thread that consumes DOWNLOAD_QUEUE and updates MANIFEST_DF."""
    global FILES_SUCCESS, FILES_FAILED

    while True:
        try:
            file_info = DOWNLOAD_QUEUE.get(timeout=5)
        except queue.Empty:
            break

        success = download_file(file_info)

        with manifest_lock:
            update_manifest_df(file_info, success)
            if success:
                FILES_SUCCESS += 1
            else:
                FILES_FAILED += 1

        DOWNLOAD_QUEUE.task_done()


def update_manifest_df(file_info: dict | pd.Series, success: bool) -> None:
    """Upsert a row in MANIFEST_DF for the file with updated status."""
    global MANIFEST_DF

    filename = str(file_info["filename"])
    status = "success" if success else "failed"

    # Locate existing row by filename
    matches = MANIFEST_DF.index[MANIFEST_DF["filename"] == filename].tolist()

    if matches:
        MANIFEST_DF.at[matches[0], "status"] = status
    else:
        new_row = dict(file_info)
        new_row["status"] = status
        # Ensure column order
        new_row_ordered = {col: new_row.get(col, pd.NA) for col in MANIFEST_COLUMNS}
        if MANIFEST_DF.empty:
            MANIFEST_DF = pd.DataFrame([new_row_ordered], columns=MANIFEST_COLUMNS)
        else:
            MANIFEST_DF = pd.concat(
                [MANIFEST_DF, pd.DataFrame([new_row_ordered])],
                ignore_index=True,
            )


def periodic_manifest_save(interval: int = 90) -> None:
    """Periodically save MANIFEST_DF to CSV every `interval` seconds."""
    while True:
        time.sleep(interval)
        with manifest_lock:
            MANIFEST_DF.to_csv(MANIFEST_PATH, index=False)
            log.info(
                f"[periodic_manifest_save] {datetime.now(UTC).isoformat().replace("+00:00", "Z")}"
                f"Processed: {FILES_SUCCESS + FILES_FAILED}, "
                f"Success: {FILES_SUCCESS}, Failed: {FILES_FAILED}"
            )


def download_file(file_info: dict | pd.Series) -> bool:
    """Download a single file. Return True on success, False on failure."""
    try:
        VECTOR_CLIENT.getFile(str(file_info["filename"]))
        time.sleep(1)  # small pacing to be gentle to API/filesystem
        return True
    except Exception as exc:  # noqa: BLE001
        log.error(f"[download_file] Failed for {file_info['filename']}: {exc}\n")
        return False


# ==============================
# Main
# ==============================

def main() -> None:
    global DEFAULT_LOCATION

    # A: Initial logging
    start_time = time.time()
    log.info(f"[Main] ===== Start vector fetcher @ {start_time:.2f} =====")
    log.info(f"[Main] Data root: {DATA_ROOT}\n")

    # 1) Get list of filenames
    cfg = parse_args()
    req_start = cfg.start
    req_end = cfg.end

    filenames = get_filenames(
        # locationCode=DEFAULT_LOCATION,
        dateFrom=req_start,
        dateTo=req_end,
    )
    file_info_df = filenames_to_file_info(
        filenames=filenames,
        # locationCode=DEFAULT_LOCATION,
    )

    # 2) Load or initialize global manifest
    load_or_init_manifest()

    # 3) Find files that are new or previously failed
    merged = file_info_df.merge(
        MANIFEST_DF[["filename", "status"]],
        on="filename",
        how="left",
    )

    # 4) Queue missing or failed files
    to_queue = merged[
        merged["status"].isna() | (merged["status"] == "failed")
    ]
    for _, row in to_queue.iterrows():
        DOWNLOAD_QUEUE.put(row)

    # 5) Start threads
    threads_start_time = time.time()
    threading.Thread(
        target=periodic_manifest_save,
        args=(90,),
        daemon=True,
    ).start()

    num_workers = cfg.workers
    threads: list[threading.Thread] = []
    for _ in range(num_workers):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    DOWNLOAD_QUEUE.join()

    for t in threads:
        t.join()

    # 6) Final manifest and provenance save
    write_manifest()
    write_provenance()

    # Z: Summary
    end_time = time.time()
    setup_time = (threads_start_time - start_time) / 60
    thread_alive_time = (end_time - threads_start_time) / 60
    total_minutes = (end_time - start_time) / 60
    log.info(
        f"[Main] ===== Processed {FILES_SUCCESS + FILES_FAILED} files "
        f"in {total_minutes:.2f} min with {num_workers} threads. ====="
    )
    log.info(
        f"[Main] Sequential estimate: {setup_time + (thread_alive_time * num_workers):.2f} min."
    )


if __name__ == "__main__":
    main()
