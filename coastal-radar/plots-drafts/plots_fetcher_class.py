"""
To make oop:

consider:
- summary statment with total time on all runs
- summary statemnet with number of batches/files proccessed in this run
- option to have logging for debugging vs normal running
- option to download monthly? or scaleable-ly
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
    """Class for command line input config info."""
    year: int
    # output_dir: str
    # num_threads: int = 15


def parse_args() -> Config:
    """Parse command-line arguments for year."""
    parser = argparse.ArgumentParser(description="Fetch CODAR plot images in quarter-month batches.")
    parser.add_argument("--year", type=int, required=True, help="Year to fetch (e.g., 2025)")
    args = parser.parse_args()
    return Config(year=args.year)

# Logging
def setup_logging() -> None:
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

# BATCH_MANIFEST_PATH = METADATA_ROOT / "batch-manifest.csv"
# PROVENANCE_PATH = METADATA_ROOT / "provenance.yaml"

# Ensure folders exist
# DATA_ROOT.mkdir(parents=True, exist_ok=True)
# METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# ONC client
TOKEN: str  = os.getenv("ONC_TOKEN")
if not TOKEN:
    raise RuntimeError("ONC_TOKEN is not set. Export your ONC API key before running.")
PLOT_CLIENT: onc.ONC = onc.ONC(TOKEN, outPath=str(DATA_ROOT))

# Globals
# batch_manifest: Optional[pd.DataFrame] = None
# download_queue: "queue.Queue[dict]" = queue.Queue()
MAX_RETRIES: int = 3
DEFAULT_LOCATION: str = "SOGCS"
DATE_ISOZ: str = "%Y-%m-%dT%H:%M:%S.000Z"
HUMAN: str = "%Y-%m-%d %H:%M:%S"

# batch_lock: threading.Lock = threading.Lock()

PLOT_PARAMS: dict[str, int | str] = {
    "locationCode": DEFAULT_LOCATION,
    "deviceCategoryCode": "OCEANOGRAPHICRADAR",
    "dataProductCode": "CODARCD", # manufacturer not quality controlled
    "extension": "png", # for plots
    "dpo_includeRadials": 0, # NOTE: change to 1 to include radials from all stations
}

MANIFEST_COLUMNS: list = [
    "dpRequestId",
    "runIds",
    "batch_id",
    "start",
    "end",
    "last_call_ran",
    "call_status",
    "file_count",
    "downloaded"
]

def ts_to_isoz(ts: datetime) -> str:
    """Transforms datetime object into ISO string. """
    return ts.strftime(DATE_ISOZ)

def utc_now_isoz() -> str:
    """Returns ISO string for the current time."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")

class SurfaceCurrentFetcher:
    """
    Encapsulates all mutable state (manifest, queue, counters, locks) and the workflow.
    This improves testability and makes concurrency safer by scoping shared state.
    """
    

    def __init__(
        self,
        plot_client: onc.ONC,
        data_root: Path,
        metadata_root: Path,
        default_location: str = DEFAULT_LOCATION,
    ) -> None:
        # External dependencies and paths
        self.plot_client = plot_client
        self.data_root = Path(data_root)
        self.metadata_root = Path(metadata_root)
        self.batch_manifest_path = self.metadata_root / "manifest.csv"
        self.provenance_path = self.metadata_root / "provenance.yaml"
        self.default_location = default_location

        # Make sure directories exist
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.metadata_root.mkdir(parents=True, exist_ok=True)

        # Mutable state held by this instance
        self.download_queue: "queue.Queue[pd.Series | dict]" = queue.Queue()
        self.batch_manifest: pd.DataFrame = pd.DataFrame(columns=MANIFEST_COLUMNS)
        self.files_success: int = 0
        self.files_failed: int = 0

        # One lock for all manifest mutations and counter updates
        self.batch_lock = threading.Lock()

        # Store last API params used for file listing. Sanitized before writing provenance.
        self.api_params: dict = {}

    def manifest_to_prov(self):
        """ Writes a provenance to yaml according to the final batch_manifest. """

        downloaded_files = int(self.batch_manifest.loc[self.batch_manifest["downloaded"], "file_count"].sum())
        
        prov = {
            "challenge": "surface-currents",
            "data-type": "plots",
            "doi": "10.34943/d32df30d-7644-446a-b0ed-9daf63377041",
            "base_url": "https://data.oceannetworks.ca/api",
            "api_calls": {},
            "file_count": downloaded_files 
        }

        for _, row in self.batch_manifest.iterrows():  # loop properly over DataFrame rows
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
        with open(self.provenance_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(prov, f, default_flow_style=False, sort_keys=False)

    def build_quartermonth_batches(self, year: int) -> pd.DataFrame:
        """
        Makes quarter-of-month batch manifest and uses requestDataProduct method to request data products for each batch.

        Schema: [dpRequestId: int, runIds: int, batch_id: str, start: iso, end: iso str, last_call_ran: str, call_status: str, file_count: int, downloaded: bool]
        """
        # TODO: change this to init list of batch objs


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

                dp_id = self.make_dp_request(filters=filters)

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

    def load_or_init_batch_manifest(self, year: int) -> None:
        """ 
        Points class batch_manifest to the exisiting csv file df or a new df. 
        Schema: [dpRequestId: int, runIds: int or None, batch_id: str, start: iso str , end: iso str, last_call_ran: str, call_status: str, file_count: int, downloaded: bool]
        """

        if self.batch_manifest_path.exists():
            self.batch_manifest = pd.read_csv(self.batch_manifest_path)

            # Re make requestIds for all rows where downloaded == False; also update last_call_ran, call_status, runIds
            for idx, row in self.batch_manifest.iterrows():
                if not row["downloaded"]: 
                    re_start = row["start"]
                    re_end = row["end"]

                    params = {
                        **PLOT_PARAMS,  # unpack base configuration
                        "dateFrom": re_start,
                        "dateTo": re_end,
                    }

                    req_id = self.make_dp_request(filters=params)

                    self.batch_manifest.loc[idx, ["dpRequestId", "runIds", "last_call_ran", "call_status"]] = [req_id, None, "make_dp_request", "complete"]

        else:
            self.batch_manifest = self.build_quartermonth_batches(year=year)

    def make_dp_request(self, filters: dict) -> int:
        """Make data product request. Returns data product request id."""
        # TODO: change this input from a dict to an on Obj
        response = self.plot_client.requestDataProduct(filters=filters)
        dp_request_id = response.get("dpRequestId")
        log.info("[requestDataProduct] dpRequestId: %s", dp_request_id)

        return dp_request_id

    # ==============================
    # Thread workers
    # ==============================

    def save_batch_manifest(self):
        """Convert list of bacth objc to dataframe then save to disk (call after finishing a batch)."""
        # TODO: convert list to dataframe

        with self.batch_lock:
            self.batch_manifest.to_csv(self.batch_manifest_path, index=False)

    def cart_complete(self, dp_request_id: int) -> bool:
        """Returns True if cart is closed (0 = closed, 1 = open)."""
        # TODO: shoudl this take the entire batch obj instead of just the id?
        response = self.plot_client.checkDataProduct(dpRequestId=dp_request_id)
        cart_status = response.get("cartStatus")
        return cart_status == 0

    def run_dp(self, dp_request_id: int) -> list[int]:
        """ 
        Takes dp_request_id from requestDataProduct and returns run_ids. 
        Updates batch_manifest with runIds, last_call_ran, call_status.

        NOTE: No error handling- either returns the Id or an errors
        """

        # TODO: should this take the entire batch obj instead of just the id?

        log.info("[run_dp] trying to get runIds for requestId %s at %s.",
                dp_request_id,
                datetime.now().strftime(HUMAN))

        response = self.plot_client.runDataProduct(dpRequestId=dp_request_id, waitComplete=True)
        raw = response.get("runIds")
        if raw is None:
            raise ValueError(f"Missing runIds in response: {response}")
        run_ids = raw if isinstance(raw, list) else [raw]

        log.info("[run_dp] got runIds %s for requestId %s at %s.",
                run_ids,
                dp_request_id,
                datetime.now().strftime(HUMAN))

        call_status = response.get("status") # Get status of runDataProduct -- waitComplete should wait for status == 'complete'
        num_files = response.get("fileCount")

        # TODO: plot batch methods for updating itself?
        with self.batch_lock: # update batch manifest last_call_ran
            self.batch_manifest.loc[self.batch_manifest["dpRequestId"] == dp_request_id, "runIds"] = run_ids
            self.batch_manifest.loc[self.batch_manifest["dpRequestId"] == dp_request_id, "last_call_ran"] = "runDataProduct"
            self.batch_manifest.loc[self.batch_manifest["dpRequestId"] == dp_request_id, "call_status"] = call_status
            self.batch_manifest.loc[self.batch_manifest["dpRequestId"] == dp_request_id, "file_count"] = num_files

        self.save_batch_manifest()
        return run_ids

    def download_dp(self, run_ids: int, dp_request_id: int) -> bool:
        """Takes dp_request_id from requestDataProduct and run_ids from runDataProduct and returns True unless error is raised by API."""
        # TODO: should this take the entire batch obj instead of just the id?
        log.info("[download_dp] trying to download for requestId %s runIds %s at %s.",
                dp_request_id,
                run_ids,
                datetime.now().strftime(HUMAN))

        self.plot_client.downloadDataProduct(runId=run_ids,
                                        maxRetries=3,
                                        downloadResultsOnly=False,
                                        includeMetadataFile=True,
                                        overwrite=False)

        # Poll until cart closes
        while not self.cart_complete(dp_request_id=dp_request_id):
            time.sleep(3)
            log.info("[download_dp] requestId %s downloading in progess.", dp_request_id)
            
        log.info("[download_dp] downloaded runIds %s at %s}.",
                run_ids,
                datetime.now().strftime(HUMAN),
                )

        with self.batch_lock:
            # update batch manifest last_call_ran
            self.batch_manifest.loc[self.batch_manifest["runIds"] == run_ids, "last_call_ran"] = "downloadDataProduct"
            self.batch_manifest.loc[self.batch_manifest["runIds"] == run_ids, "call_status"] = "complete"
            self.batch_manifest.loc[self.batch_manifest["runIds"] == run_ids, "downloaded"] = True
        
        self.save_batch_manifest()

        return True

    def worker(self) -> None:
        """
        Each worker:
        - Pulls a batch dict from download_queue
        - Tries runDataProduct (up to MAX_RETRIES)
        - Tries downloadDataProduct (up to MAX_RETRIES)
        - Always calls task_done() for each dequeued item
        """
        while True:
            # 1) Pull from queue (or exit when empty)
            try:
                batch_info = self.download_queue.get(timeout=5)  # -> dict with keys incl. 'dpRequestId'
            except queue.Empty:
                break

            try:
                req_id = int(batch_info["dpRequestId"])

                # 2) Run data product — obtain runIds (retry)
                run_ids: list[int] | None = None
                cur_run_id: int | None = None

                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        run_ids = self.run_dp(dp_request_id=req_id)  # expected list[int]
                        cur_run_id = int(run_ids[0])
                        log.info("[worker] runDataProduct OK for requestId=%s (runId=%s)",
                                req_id,
                                cur_run_id)
                        break
                    except (ConnectionError, RuntimeError, OSError) as exc:
                        log.error(
                            "[worker] runDataProduct failed (attempt %d/%d\nfor requestId=%s: %s",
                            attempt,
                            MAX_RETRIES,
                            req_id,
                            exc,
                        )
                        log.exception(exc)
                        time.sleep(5)

                if cur_run_id is None:
                    log.error("[worker] runDataProduct failed permanently for requestId=%s",req_id)
                    continue  # go to finally -> task_done()

                # Always log manifest snapshot
                # log.info(f"[worker] manifest after run_dp for requestId={req_id}\n{batch_manifest}")

                # 3) Download data product (retry)
                success = False
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        self.download_dp(run_ids=cur_run_id, dp_request_id=req_id)
                        success = True
                        log.info("[worker] downloadDataProduct OK for requestId=%s (runId=%s)",
                                req_id,
                                cur_run_id)
                        break
                    except (ConnectionError, RuntimeError, OSError) as exc:
                        log.error("[worker] download_dp failed (attempt %d/%d\nfor requestId=%s: %s",
                                attempt,
                                MAX_RETRIES,
                                req_id,
                                exc,
                        )
                        log.exception(exc)
                        time.sleep(5)

                if not success:
                    log.error("[worker] download_dp failed permanently for requestId=%s", req_id)

                # Always log manifest snapshot
                log.info("[worker] manifest after download_dp for requestId=%s\n%s", req_id, self.batch_manifest)

            finally:
                # Always mark the dequeued item as processed to avoid deadlocks
                self.download_queue.task_done()

    def run(self, year: int) -> None:
        """"""

        # A. Initial Logging
        start_time = datetime.now()  # start timer
        log.info("[main] ===== BATCH START ===== at %s.", ts_to_isoz(start_time))


        # 1. Init batch manifest
        self.load_or_init_batch_manifest(year=year)
        log.info(self.batch_manifest)

        # 2. Build download queue -- do we need to check current request vs historical batch?
        for _, row in self.batch_manifest.iterrows():
            if not row["downloaded"]: # Enqueue all batches not yet downloaded
                self.download_queue.put(row.to_dict())
                log.info("[main] queueing dpRequestId: %s, start: %s, end: %s", row['dpRequestId'], row['start'], row['end'])

        # 3. Start threads
        num_workers = 15
        threads = []

        log.info("[main] Starting %d threads.", num_workers)
        for _ in range(num_workers):
            t = threading.Thread(target=self.worker)
            t.start() # Initialize and start thread object 't'
            threads.append(t)

        # 4. Finish threads
        self.download_queue.join() # Wait for all tasks in the queue to be processed - signaled by task_done() in worker

        # Main thread waits for all threads to finish
        for t in threads:
            t.join()

        # 5. Logging / Summary
        self.manifest_to_prov()

        end_time = datetime.now()
        elapsed_seconds = (end_time - start_time).total_seconds()

        total_batches = len(self.batch_manifest)
        success_batches = len(self.batch_manifest[self.batch_manifest["downloaded"]])
        failed_batches = len(self.batch_manifest[~self.batch_manifest["downloaded"]])
        files_downloaded = int(self.batch_manifest.loc[self.batch_manifest["downloaded"], "file_count"].sum())

        log.info("\n===== BATCH DOWNLOAD SUMMARY =====")
        log.info("Total batches processed : %d", total_batches)
        log.info("Total files downloaded  : %d", files_downloaded)
        log.info("Successful batches      : %d", success_batches)
        log.info("Failed batches          : %d", failed_batches)
        log.info("Elapsed time            : %.2f mins", elapsed_seconds/60)


def main():
    cfg = parse_args()
    fetcher = SurfaceCurrentFetcher(PLOT_CLIENT, DATA_ROOT, METADATA_ROOT)
    fetcher.run(year=cfg.year)

   

if __name__ == "__main__":
    main()
