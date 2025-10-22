"""
Multithreaded CODAR Plot Fetcher
================================

Fetches CODAR surface-current plot images (PNG) from
Ocean Networks Canada (ONC) for a specified year, downloading each
batch in parallel and maintaining a persistent batch manifest.

Overview
--------
This script requests, runs, and downloads CODAR plot images for a
fixed location (SOGCS) and device category (OCEANOGRAPHICRADAR). Data
are fetched in quarter-month batches across the selected year.
Each batch is tracked in a manifest CSV, which records its request ID,
run ID, time range, file count, and download status. A provenance YAML
summarizes the ONC API calls and metadata for reproducibility.

Directory layout (after successful run)
--------------------------------------
surface-currents/
    plots/
        <ONC_filename>.png
        ...
surface-currents-metadata/
    plots/
        batch-manifest.csv   # ["dpRequestId","runIds","batch_id","start","end",
                             #  "last_call_ran","call_status","file_count","downloaded"]
        provenance.yaml      # challenge name, API call details, manifest summary

Key features
------------
- Multithreaded downloads using Python threads
- Batch-based data requests and resumable workflow
- Automatic manifest updates with locking for thread safety
- Provenance tracking for transparency and reproducibility

Environment
-----------
- Python 3.11+
- Environment variable: ``ONC_TOKEN`` must be set to a valid ONC API key
- Dependencies: ``onc``, ``pandas``, ``pyyaml``, ``python-dotenv``

Usage
-----
Run from any directory (downloads go to ~/Downloads/surface-currents):

    export ONC_TOKEN=<your_api_key>
    python multithread_plot_fetcher.py \
        --year 2025 \
        --workers 12

Arguments
---------
--year       Year to fetch (required)
--workers    Number of parallel download threads (default: 15)

Outputs
-------
- ``surface-currents-metadata/plots/batch-manifest.csv``  
  Records batch status, IDs, and file counts  
- ``surface-currents-metadata/plots/provenance.yaml``  
  Documents API parameters and manifest summary  
- Downloaded .png files under ``surface-currents/plots``

Notes
-----
If the script is interrupted, rerun it with the same year.
The manifest ensures that completed batches are skipped and failed
ones are retried. To restart from scratch, delete the manifest file
before rerunning.
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Optional

import onc
import pandas as pd
import yaml
from dotenv import load_dotenv

# ==============================
# Setup
# ==============================

load_dotenv() # Environmental variables

# Command line arguments
@dataclass(frozen=True)
class Config:
    year: int
    workers: int

def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Fetch CODAR plot images in quarter-month batches.")
    p.add_argument("--year", type=int, required=True, help="Year to fetch (e.g., 2025)")
    p.add_argument("--workers", type=int, default=15, help="Concurrent threads (default: 15)")
    a = p.parse_args()
    return Config(year=a.year, workers=a.workers)

# Logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,  # change to DEBUG for detailed tracing
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],  # always print to console
        force=True,  # override logging from other libraries
    )

log = logging.getLogger(__name__)

setup_logging()
log.info("Starting surface current plots downloader...")

DOWNLOADS_PATH = Path.home() / "Downloads"
DATA_ROOT = DOWNLOADS_PATH / "surface-currents" / "plots"
METADATA_ROOT = DOWNLOADS_PATH / "surface-currents-metadata" / "plots"

BATCH_MANIFEST_PATH = METADATA_ROOT / "batch-manifest.csv"
PROVENANCE_PATH = METADATA_ROOT / "provenance.yaml"

# Ensure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# ONC client
TOKEN = os.getenv("ONC_TOKEN")
if not TOKEN:
    raise RuntimeError("ONC_TOKEN is not set. Export your ONC API key before running.")
PLOT_CLIENT = onc.ONC(TOKEN, outPath=str(DATA_ROOT))


# Globals
BATCH_MANIFEST: Optional[pd.DataFrame] = None
DOWNLOAD_QUEUE: "queue.Queue[dict]" = queue.Queue()
MAX_RETRIES: int = 3
DEFAULT_LOCATION = "SOGCS"
DATE_ISOZ = "%Y-%m-%dT%H:%M:%S.000Z"
HUMAN = "%Y-%m-%d %H:%M:%S"

batch_lock = threading.Lock()

PLOT_PARAMS = {
    "locationCode": DEFAULT_LOCATION,
    "deviceCategoryCode": "OCEANOGRAPHICRADAR",
    "dataProductCode": "CODARCD", # manufacturer not quality controlled
    "extension": "png", # NOTE: for plots
    "dpo_includeRadials": 0,
}

# ==============================
# Functions
# ==============================

def ts_to_isoz(ts: datetime) -> str:
    return ts.strftime(DATE_ISOZ)

def utc_now_isoz() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")

def manifest_to_prov():
    """ Writes a provenance to yaml according to the final BATCH_MANIFEST. """
    global BATCH_MANIFEST

    downloaded_files = int(BATCH_MANIFEST.loc[BATCH_MANIFEST["downloaded"] == True, "file_count"].sum())
    
    prov = {
        "challenge": "surface-currents",
        "data-type": "plots",
        "doi": "10.34943/d32df30d-7644-446a-b0ed-9daf63377041",
        "base_url": "https://data.oceannetworks.ca/api",
        "api_calls": {},
        "file_count": downloaded_files 
    }

    for _, row in BATCH_MANIFEST.iterrows():  # loop properly over DataFrame rows
        batch = row.to_dict()
        cur_req_id = batch['dpRequestId']
        cur_run_id = batch['runIds']
        cur_batch_id = batch['batch_id']
        cur_start = pd.Timestamp(batch['start'])
        cur_end = pd.Timestamp(batch['end'])

        prov["api_calls"][cur_batch_id] = {

            "requestDataProduct": {
                "endpoint": "/dataProductDelivery/request", 
                "parameters": {
                    "filters": {
                        **PLOT_PARAMS,  # unpack all the base parameters
                        "dateFrom": ts_to_isoz(cur_start),
                        "dateTo": ts_to_isoz(cur_end),
                    },
                },
                "Request ID": int(cur_req_id),
            },
            "runDataProduct": {
                "endpoint": "/dataProductDelivery/run", 
                "parameters": {
                    "Request ID": cur_req_id,
                    "waitComplete": True,
                },
                "Run IDs": int(cur_run_id),
            },
            "downloadDataProduct": {
                "endpoint": "/dataProductDelivery/download", 
                "parameters": {
                    "Run IDs": int(cur_run_id),
                    "maxRetries": 0,
                    "downloadResultsOnly": False,
                    "includeMetadataFile": False,
                    "overwrite": False,
                },
            }
        }

    # write all the batches to a YAML file
    prov["last_updated"] = utc_now_isoz()
    with open(PROVENANCE_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(prov, f, default_flow_style=False, sort_keys=False)


def build_quartermonth_batches(year: int) -> pd.DataFrame:
    """
    Makes quarter-of-month batch manifest and uses requestDataProduct method to request data products for each batch.

    Schema: [dpRequestId: int, runIds: int, batch_id: str, start: iso, end: iso str, last_call_ran: str, call_status: str, file_count: int, downloaded: bool]
    """

    rows: list[dict] = []

    # Define quarter ranges as (start_day, end_day)
    quarter_days = [
        (1, 7),    # Q1
        (8, 15),   # Q2
        (16, 23),  # Q3
        (24, None) # Q4 handled specially
    ] 
    
    for month in range(1,13):
        for q, (start_day, end_day) in enumerate(quarter_days, start=1):
            start = pd.Timestamp(year=year, month=month, day=start_day, tz="UTC")

            if q < 4:  # Q1–Q3 fixed end day
                end = pd.Timestamp(year=year, month=month, day=end_day, hour=23, minute=59, second=59, tz="UTC")
            else:  # Q4 goes until the day before the next month starts
                if month == 12:
                    next_month_start = pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC")
                else:
                    next_month_start = pd.Timestamp(year=year, month=month + 1, day=1, tz="UTC")
                end = next_month_start - timedelta(seconds=1)

            filters = {
                **PLOT_PARAMS,  # unpack base configuration
                "dateFrom": ts_to_isoz(start),
                "dateTo": ts_to_isoz(end),
            }

            dp_id = make_dp_request(filters=filters)

            rows.append({
                "dpRequestId": dp_id,
                "runIds": None,
                "batch_id": f"{year}-{month:02d}-{q}Q",
                "start": ts_to_isoz(start),
                "end": ts_to_isoz(end),
                "last_call_ran": "requestDataProduct",
                "call_status": "complete",
                "file_count": 0,
                "downloaded": False,
            })

    return pd.DataFrame(rows)

def load_or_init_batch_manifest(year: int) -> None:
    """ 
    Points global BATCH_MANIFEST to the exisiting csv file df or a new df. 
    Schema: [dpRequestId: int, runIds: int or None, batch_id: str, start: iso str , end: iso str, last_call_ran: str, call_status: str, file_count: int, downloaded: bool]
    """
    global BATCH_MANIFEST

    if BATCH_MANIFEST_PATH.exists():
        BATCH_MANIFEST = pd.read_csv(BATCH_MANIFEST_PATH)

        # Re make requestIds for all rows wehere downloaded == False -- also reset last_call_ran, call_status, runIds
        for idx, row in BATCH_MANIFEST.iterrows():
            if row["downloaded"] != True: 
                re_start = row["start"]
                re_end = row["end"]

                params = {
                    **PLOT_PARAMS,  # unpack base configuration
                    "dateFrom": re_start,
                    "dateTo": re_end,
                }

                req_id = make_dp_request(filters=params)

                BATCH_MANIFEST.loc[idx, ["dpRequestId", "runIds", "last_call_ran", "call_status"]] = [req_id, None, "make_dp_request", "complete"]

    else:
        BATCH_MANIFEST = build_quartermonth_batches(year=year)

def make_dp_request(filters: dict) -> int:
    """Make data product request. Returns data product request id."""
    response = PLOT_CLIENT.requestDataProduct(filters=filters)
    dpRequestId = response.get("dpRequestId")
    log.info(f"[requestDataProduct] dpRequestId: {dpRequestId}")
    return dpRequestId

# ==============================
# Thread workers
# ==============================

def save_batch_manifest():
    """Write batch manifest to disk (call after finishing a batch)."""
    global BATCH_MANIFEST
    with batch_lock:
        BATCH_MANIFEST.to_csv(BATCH_MANIFEST_PATH, index=False)

def cart_complete(dp_request_id: int) -> bool:
    """Returns True if cart is closed (0 = closed, 1 = open)."""
    response = PLOT_CLIENT.checkDataProduct(dpRequestId=dp_request_id)
    cart_status = response.get("cartStatus")
    return cart_status == 0


def run_dp(dp_request_id: int) -> list[int]:
    """ 
    Takes dp_request_id from requestDataProduct and returns run_ids. 
    Updates BATCH_MANIFEST with runIds, last_call_ran, call_status.

    NOTE: No error handling? either returns the Id or an error
    """
 
    log.info(f"[run_dp] trying to get runIds for requestId {dp_request_id} at {datetime.now().strftime(HUMAN)}.")

    global BATCH_MANIFEST

    response = PLOT_CLIENT.runDataProduct(dpRequestId=dp_request_id, waitComplete=True)
    raw = response.get("runIds")
    if raw is None:
        raise ValueError(f"Missing runIds in response: {response}")
    run_ids = raw if isinstance(raw, list) else [raw]

    log.info(f"[run_dp] got runIds {run_ids} for requestId {dp_request_id} at {datetime.now().strftime(HUMAN)}.")

    call_status = response.get("status") # Get status of runDataProduct -- waitComplete should wait for status == 'complete'
    num_files = response.get("fileCount")

    with batch_lock: # update batch manifest last_call_ran
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dp_request_id, "runIds"] = run_ids
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dp_request_id, "last_call_ran"] = "runDataProduct"
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dp_request_id, "call_status"] = call_status
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dp_request_id, "file_count"] = num_files
    
    save_batch_manifest()
    return run_ids

def download_dp(run_ids: int, dp_request_id: int):
    """Takes dp_request_id from requestDataProduct and run_ids from runDataProduct and returns True unless error is raised by API."""
    
    log.info(f"[download_dp] trying to download for requestId {dp_request_id} runIds {run_ids} at {datetime.now().strftime(HUMAN)}.")

    global BATCH_MANIFEST

    response = PLOT_CLIENT.downloadDataProduct(runId=run_ids, 
                                            maxRetries=3, 
                                            downloadResultsOnly=False, 
                                            includeMetadataFile=True, 
                                            overwrite=False)

    # Poll until cart closes
    while cart_complete(dp_request_id=dp_request_id) is False:
        time.sleep(3)
        log.info(f"[download_dp] requestId {dp_request_id} downloading in progess.")
        
    log.info(f"[download_dp] downloaded runIds {run_ids} at {datetime.now().strftime(HUMAN)}.")

    with batch_lock:
        # update batch manifest last_call_ran
        BATCH_MANIFEST.loc[BATCH_MANIFEST["runIds"] == run_ids, "last_call_ran"] = "downloadDataProduct"
        BATCH_MANIFEST.loc[BATCH_MANIFEST["runIds"] == run_ids, "call_status"] = "complete"
        BATCH_MANIFEST.loc[BATCH_MANIFEST["runIds"] == run_ids, "downloaded"] = True
    
    save_batch_manifest()

    return True

def worker() -> None:
    """
    Each worker:
    - Pulls a batch dict from DOWNLOAD_QUEUE
    - Tries runDataProduct (up to MAX_RETRIES)
    - Tries downloadDataProduct (up to MAX_RETRIES)
    - Always calls task_done() for each dequeued item
    """
    while True:
        # 1) Pull from queue (or exit when empty)
        try:
            batch_info = DOWNLOAD_QUEUE.get(timeout=5)  # -> dict with keys incl. 'dpRequestId'
        except queue.Empty:
            break

        try:
            req_id = int(batch_info["dpRequestId"])

            # 2) Run data product — obtain runIds (retry)
            run_ids: list[int] | None = None
            cur_run_id: int | None = None

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    run_ids = run_dp(dp_request_id=req_id)  # expected list[int]
                    cur_run_id = int(run_ids[0])
                    log.info(f"[worker] runDataProduct OK for requestId={req_id} (runId={cur_run_id})")
                    break
                except Exception as exc:
                    log.error(f"[worker] runDataProduct failed (attempt {attempt}/{MAX_RETRIES}) "
                              f"for requestId={req_id}: {exc}")
                    log.exception(exc)
                    time.sleep(5)

            if cur_run_id is None:
                log.error(f"[worker] runDataProduct failed permanently for requestId={req_id}")
                continue  # go to finally -> task_done()

            # Always log manifest snapshot
            # log.info(f"[worker] manifest after run_dp for requestId={req_id}\n{BATCH_MANIFEST}")

            # 3) Download data product (retry)
            success = False
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    download_dp(run_ids=cur_run_id, dp_request_id=req_id)
                    success = True
                    log.info(f"[worker] downloadDataProduct OK for requestId={req_id} (runId={cur_run_id})")
                    break
                except Exception as exc:
                    log.error(f"[worker] download_dp failed (attempt {attempt}/{MAX_RETRIES}) "
                              f"for requestId={req_id}: {exc}")
                    log.exception(exc)
                    time.sleep(5)

            if not success:
                log.error(f"[worker] download_dp failed permanently for requestId={req_id}")

            # Always log manifest snapshot
            log.info(f"[worker] manifest after download_dp for requestId={req_id}\n{BATCH_MANIFEST}")


        finally:
            # Always mark the dequeued item as processed to avoid deadlocks
            DOWNLOAD_QUEUE.task_done()

def main():
    # A. Initial Logging
    start_time = datetime.now()  # start timer
    log.info(f"[main] ===== BATCH START ===== at {ts_to_isoz(start_time)}.")

    global BATCH_MANIFEST, DOWNLOAD_QUEUE
    cfg = parse_args()

    # 1. Init batch manifest
    load_or_init_batch_manifest(year=cfg.year)
    log.info(BATCH_MANIFEST)

    # 2. Build download queue -- do we need to check current request vs historical batch?
    for _, row in BATCH_MANIFEST.iterrows():
        if row["downloaded"] != True: # Enqueue all batches not yet downloaded
            DOWNLOAD_QUEUE.put(row.to_dict())
            log.info(f"[main] queueing dpRequestId: {row['dpRequestId']}, start: {row['start']}, end: {row['end']}")
    
    # 3. Start threads
    num_workers = cfg.workers
    threads = []

    log.info(f"[main] Starting {num_workers} threads.")
    for _ in range(num_workers):
        t = threading.Thread(target=worker)
        t.start() # Initialize and start thread object 't'
        threads.append(t)

    # 4. Finish threads
    DOWNLOAD_QUEUE.join() # Wait for all tasks in the queue to be processed - signaled by task_done() in worker

    # Main thread waits for all threads to finish
    for t in threads:
        t.join()

    # 5. Logging / Summary
    manifest_to_prov()

    end_time = datetime.now()
    elapsed_seconds = (end_time - start_time).total_seconds()

    total_batches = len(BATCH_MANIFEST)
    success_batches = len(BATCH_MANIFEST[BATCH_MANIFEST["downloaded"] == True])
    failed_batches = len(BATCH_MANIFEST[BATCH_MANIFEST["downloaded"] == False])
    files_downloaded = BATCH_MANIFEST.loc[BATCH_MANIFEST["downloaded"] == True, "file_count"].sum()

    log.info("\n===== BATCH DOWNLOAD SUMMARY =====")
    log.info(f"Total batches processed : {total_batches}")
    log.info(f"Total files downloaded  : {files_downloaded}")
    log.info(f"Successful batches      : {success_batches}")
    log.info(f"Failed batches          : {failed_batches}")
    log.info(f"Elapsed time            : {elapsed_seconds/60:.2f} mins")

if __name__ == "__main__":
    main()
