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

import onc
import pandas as pd
import yaml
from dotenv import load_dotenv

from plot_batch import PlotBatch


load_dotenv() # Environmental variables

# CLI
@dataclass(frozen=True)
class Config:
    """Class for command line input config info."""
    year: int
    output_dir: str
    # num_threads: int = 15


def parse_args() -> Config:
    """Parse command-line arguments for year and output directory."""
    parser = argparse.ArgumentParser(description="Fetch CODAR plot images in quarter-month batches.")
    parser.add_argument("--year", type=int, required=True, help="Year to fetch (e.g., 2025)")
    parser.add_argument(
        "--output_dir", type=str, required=True, help="Directory to save downloaded plots and metadata"
    )
    args = parser.parse_args()
    return Config(year=args.year, output_dir=args.output_dir)

def setup_logging() -> None:
    """Set up console logging."""
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

def ts_to_isoz(ts: datetime) -> str:
    """Transforms datetime object into ISO string. """
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def utc_now_isoz() -> str:
    """Returns ISO string for the current time."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")

class PlotFetcher:
    """
    Coordinates:
    - creating PlotBatch objects
    - handling request/run/download
    - managing queues + threads
    - writing manifest + provenance
    """
    # Constants
    MAX_RETRIES = 3
    DEFAULT_LOCATION = "SOGCS"
    HUMAN = "%Y-%m-%d %H:%M:%S"

    PLOT_PARAMS: dict[str, int | str] = {
        "locationCode": DEFAULT_LOCATION,
        "deviceCategoryCode": "OCEANOGRAPHICRADAR",
        "dataProductCode": "CODARCD",
        "extension": "png",
        "dpo_includeRadials": 0,
    }
    MANIFEST_COLUMNS = [ # TODO: import this from plot batch
        "dpRequestId",
        "runIds",
        "batch_id",
        "start",
        "end",
        "last_call_ran",
        "call_status",
        "file_count",
        "downloaded",
    ]

    def __init__(self, output_dir: str) -> None: # TODO: make lighter

        # Config?
        self.plot_client: onc.ONC
        self.threads: int = 15
        
        # External paths
        self.data_root: Path = Path(output_dir) / "surface-currents/plots"
        self.metadata_root: Path = Path(output_dir) / "surface-currents-metadata/plots"
        self.batch_manifest_path: Path = self.metadata_root / "manifest.csv"
        self.provenance_path: Path = self.metadata_root / "provenance.yaml"
  
        # Make sure directories exist
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.metadata_root.mkdir(parents=True, exist_ok=True)

        # Set up API client
        token: str  = os.getenv("ONC_TOKEN")
        if not token:
            raise RuntimeError("ONC_TOKEN is not set. Export your ONC API key before running.")
        self.plot_client: onc.ONC = onc.ONC(token, outPath=str(self.data_root))

        # Mutable state held by this instance
        self.download_queue: "queue.Queue[pd.Series | dict]" = queue.Queue()
        self.batch_manifest: list[PlotBatch] = []
        self.files_success: int = 0
        self.files_failed: int = 0

        # One lock for all manifest mutations and counter updates
        self.batch_lock = threading.Lock()

        # Store last API params used for file listing. Sanitized before writing provenance.
        self.api_params: dict = {}

    def manifest_to_prov(self):
        """ Writes a provenance to yaml according to the final batch_manifest. """

        downloaded_files = sum(batch.file_count for batch in self.batch_manifest if batch.downloaded)
        
        prov = {
            "challenge": "surface-currents",
            "data-type": "plots",
            "doi": "10.34943/d32df30d-7644-446a-b0ed-9daf63377041",
            "base_url": "https://data.oceannetworks.ca/api",
            "api_calls": {},
            "file_count": downloaded_files 
        }

        for batch in self.batch_manifest:
            cur_req_id = batch.dpRequestId
            cur_run_id = batch.runIds
            cur_batch_id = batch.batch_id
            cur_start = pd.Timestamp(batch.start)
            cur_end = pd.Timestamp(batch.end)

            prov["api_calls"][cur_batch_id] = {

                "requestDataProduct": {
                    "endpoint": "/dataProductDelivery/request", 
                    "parameters": {
                        "filters": {
                            **self.PLOT_PARAMS,  # unpack all the base parameters
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
                        "Run IDs": cur_run_id,
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

    def build_quartermonth_batches(self, year: int) -> list[PlotBatch]: # TODO; should be this updating the historical or present req list of batches?
        """
        Makes quarter-of-month batch manifest and uses requestDataProduct method to request data products for each batch.

        Schema: [dpRequestId: int, runIds: int, batch_id: str, start: iso, end: iso str, last_call_ran: str, call_status: str, file_count: int, downloaded: bool]
        """

        batches: dict[PlotBatch] = []
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
                    **self.PLOT_PARAMS,  # unpack base configuration
                    "dateFrom": ts_to_isoz(start),
                    "dateTo": ts_to_isoz(end),
                }

                dp_id = self.make_dp_request(filters=filters)

                cur_batch = PlotBatch(dpRequestId=dp_id,
                                           batch_id=f"{year}-{month:02d}-{q}Q",
                                           start=ts_to_isoz(start),
                                           end=ts_to_isoz(end))
              
                batches.append(cur_batch)

        return batches

    def load_or_init_batch_manifest(self, year: int) -> None:
        """ 
        Points class batch_manifest to the exisiting csv file as list of batches or a to new list of batches. 
        Schema: [dpRequestId: int, runIds: int or None, batch_id: str, start: iso str , end: iso str, last_call_ran: str, call_status: str, file_count: int, downloaded: bool]
        """

        if self.batch_manifest_path.exists():
            df = pd.read_csv(self.batch_manifest_path)

            # Historical → self.batch_manifest
            self.batch_manifest = [PlotBatch.dict_to_obj(row) for _, row in df.iterrows()]

            # Re reuqest failed batches
            for batch in self.batch_manifest:
                if not batch.downloaded:
                    params = {
                        **self.PLOT_PARAMS,  # unpack base configuration
                        "dateFrom": batch.start,
                        "dateTo": batch.end,
                    }

                    req_id = self.make_dp_request(filters=params)

                    # update req id, last_call_ran, call_status, runIds
                    batch.update_after_new_request(new_req_id=req_id)

        else:
            self.batch_manifest = self.build_quartermonth_batches(year=year)

        return

    def make_dp_request(self, filters: dict) -> int:
        """Make data product request. Returns data product request id."""
        response = self.plot_client.requestDataProduct(filters=filters)
        dp_request_id = response.get("dpRequestId")
        log.info("[requestDataProduct] dpRequestId: %s", dp_request_id)

        return dp_request_id

    # ==============================
    # Thread workers
    # ==============================

    def save_batch_manifest(self):
        """Convert list of batch objects to CSV and save."""
        with self.batch_lock:
            rows = [batch.to_dict() for batch in self.batch_manifest]
            df = pd.DataFrame(rows)
            df.to_csv(self.batch_manifest_path, index=False)

    def cart_complete(self, batch: PlotBatch) -> bool:
        """Returns True if cart is closed (0 = closed, 1 = open)."""
        dp_request_id = batch.dpRequestId

        response = self.plot_client.checkDataProduct(dpRequestId=dp_request_id)
        cart_status = response.get("cartStatus")
        return cart_status == 0

    def run_dp(self, batch: PlotBatch) -> list[int]:
        """ 
        Takes dp_request_id from requestDataProduct and returns run_ids. 
        Updates batch_manifest with runIds, last_call_ran, call_status.

        NOTE: No error handling- either returns the Id or an errors
        """
        dp_request_id = batch.dpRequestId

        log.info("[run_dp] trying to get runIds for requestId %s at %s.",
                dp_request_id,
                datetime.now().strftime(self.HUMAN))

        response = self.plot_client.runDataProduct(dpRequestId=dp_request_id, waitComplete=True)
        raw = response.get("runIds")
        if raw is None:
            raise ValueError(f"Missing runIds in response: {response}")
        run_ids = raw if isinstance(raw, list) else [raw]

        log.info("[run_dp] got runIds %s for requestId %s at %s.",
                run_ids,
                dp_request_id,
                datetime.now().strftime(self.HUMAN))

        call_status = response.get("status") # Get status of runDataProduct -- waitComplete should wait for status == 'complete'
        num_files = response.get("fileCount")


        with self.batch_lock: # update batch manifest last_call_ran
            batch.update_after_run(new_run_id=run_ids, call_status=call_status, file_count=num_files)

        self.save_batch_manifest()
        return run_ids

    def download_dp(self, batch: PlotBatch) -> bool:
        """Takes dp_request_id from requestDataProduct and run_ids from runDataProduct and returns True unless error is raised by API."""
        # TODO: should this take the entire batch obj instead of just the id?
        dp_request_id = batch.dpRequestId
        run_ids = batch.runIds

        log.info("[download_dp] trying to download for requestId %s runIds %s at %s.",
                dp_request_id,
                run_ids,
                datetime.now().strftime(self.HUMAN))

        self.plot_client.downloadDataProduct(runId=run_ids,
                                        maxRetries=3,
                                        downloadResultsOnly=False,
                                        includeMetadataFile=True,
                                        overwrite=False)

        # Poll until cart closes
        while not self.cart_complete(batch=batch):
            time.sleep(3)
            log.info("[download_dp] requestId %s downloading in progess.", dp_request_id)

        log.info("[download_dp] downloaded runIds %s at %s}.",
                run_ids,
                datetime.now().strftime(self.HUMAN),
                )

        with self.batch_lock:
            # update batch manifest last_call_ran
            batch.update_after_download()

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
                batch = self.download_queue.get(timeout=5)  # -> PlotBatch obj
            except queue.Empty:
                break

            try:
                req_id = int(batch.dpRequestId)

                # 2) Run data product — obtain runIds (retry)
                run_ids: list[int] | None = None
                cur_run_id: int | None = None

                for attempt in range(1, self.MAX_RETRIES + 1):
                    try:
                        run_ids = self.run_dp(batch=batch)  # expected list[int]
                        cur_run_id = int(run_ids[0]) # NOTE: ensure int and not list
                        
                        log.info("[worker] run_dp OK for requestId=%s (runId=%s)",
                                req_id,
                                cur_run_id)
                        break
                    except (ConnectionError, RuntimeError, OSError) as exc:
                        log.error(
                            "[worker] run_dp failed (attempt %d/%d\nfor requestId=%s: %s",
                            attempt,
                            self.MAX_RETRIES,
                            req_id,
                            exc,
                        )
                        log.exception(exc)
                        time.sleep(5)

                if cur_run_id is None:
                    log.error("[worker] run_dp failed permanently for requestId=%s",req_id)
                    continue  # go to finally -> task_done()

                # 3) Download data product (retry)
                success = False
                for attempt in range(1, self.MAX_RETRIES + 1):
                    try:
                        self.download_dp(batch=batch)
                        success = True
                        log.info("[worker] download_dp OK for requestId=%s (runId=%s)",
                                req_id,
                                cur_run_id)
                        break
                    except (ConnectionError, RuntimeError, OSError) as exc:
                        log.error("[worker] download_dp failed (attempt %d/%d\nfor requestId=%s: %s",
                                attempt,
                                self.MAX_RETRIES,
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
        for batch in self.batch_manifest:
            if not batch.downloaded:
                self.download_queue.put(batch)
                log.info("[main] queueing dpRequestId: %s, start: %s, end: %s", batch.dpRequestId, batch.start, batch.end)


        # 3. Start threads
        num_workers = self.threads
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

        # # 5. Logging / Summary
        # self.manifest_to_prov()

        end_time = datetime.now()
        elapsed_seconds = (end_time - start_time).total_seconds()

        # TODO: update for list or make it based off of csv
        total_batches = len(self.batch_manifest)
        success_batches = sum(1 for b in self.batch_manifest if b.downloaded)
        failed_batches = sum(1 for b in self.batch_manifest if not b.downloaded)
        files_downloaded = sum(b.file_count for b in self.batch_manifest if b.downloaded)


        log.info("\n===== BATCH DOWNLOAD SUMMARY =====")
        log.info("Total batches processed : %d", total_batches)
        log.info("Total files downloaded  : %d", files_downloaded)
        log.info("Successful batches      : %d", success_batches)
        log.info("Failed batches          : %d", failed_batches)
        log.info("Elapsed time            : %.2f mins", elapsed_seconds/60)


def main():
    cfg = parse_args()
    fetcher = PlotFetcher(output_dir=cfg.output_dir)
    fetcher.run(year=cfg.year)

   

if __name__ == "__main__":
    main()
