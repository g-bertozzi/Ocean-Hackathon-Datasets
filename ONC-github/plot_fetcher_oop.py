"""
CODAR Surface Currents Plot Fetcher

This module provides functionality to download CODAR surface current plots from Ocean Networks Canada
for a specified year. It organizes downloads into quarter-month batches, tracks progress, and provides 
error handling and retry mechanisms.

Features:
- Automatically divides the specified year into quarter-month batches and submits data product requests.
- Tracks each batch with request IDs, run IDs, status, file counts, and download completion.
- Supports retries for failed requests or downloads up to a configurable maximum.
- Downloads batches in parallel using multiple threads for efficiency.
- Maintains a manifest (CSV) and provenance (YAML) for reproducibility and audit purposes.
- Provides detailed logging for progress monitoring and debugging, with optional DEBUG mode for verbose output.

Usage:
    python fetch_surface_current_plots.py --year 2024 --download_dir /path/to/data --threads 10 --debug

Command Line Input Options:
- year: Required. The year of plots to download.
- download_dir: Optional. Base directory to store downloaded plots and metadata (default: Downloads).
- threads: Optional. Number of parallel download threads (default: 15).
- debug: Optional. Provides additional information on API calls, batch processing, and error details.

Output Structure:
    Downloads/
        surface-currents/plots_{year}/
            STRAITOFGEORGIAARRAY_{YYYYMMDDTHHMMSS.sssZ}.png
            ...
        surface-currents-metadata/plots_{year}/
            manifest.csv
            provenance.yaml


Notes:
- Each year's data is stored in a separate directory to prevent conflicts with previously downloaded years.
- Manifest file allow the script to resume incomplete downloads.
- Ensure the ONC_TOKEN environment variable is set, e.g.:
    export ONC_TOKEN="your_token_here"  (Linux/macOS)
    set ONC_TOKEN=your_token_here       (Windows)

Author: Grace Bertozzi
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

# Setup
load_dotenv()

# Logging
def setup_logging(debug: bool = False) -> logging.Logger:
    """
    Configures and returns a logger.
    - debug=True enables DEBUG level logging.
    - All logs are output to stdout with timestamps.
    """
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    return logging.getLogger(__name__)

log = setup_logging()

# Config / CLI
@dataclass(frozen=True)
class Config:
    """
    Simple dataclass to store command-line configuration.
    - year: Year for which plots are fetched
    - download_dir: Base directory for downloads
    - threads: Number of parallel threads
    """
    year: int
    download_dir: Optional[Path] = Path.home() / "Downloads"
    threads: int = 15
    debug: bool = False

def parse_args() -> Config:
    """
    Parse command-line arguments and return a Config object.
    """
    parser = argparse.ArgumentParser(description="Fetch CODAR plot images in batches.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--download_dir", type=str, default=str(Path.home() / "Downloads"))
    parser.add_argument("--threads", type=int, default=15)
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    return Config(year=args.year, download_dir=Path(args.download_dir), threads=args.threads, debug=args.debug)

# Constants
DATE_ISOZ = "%Y-%m-%dT%H:%M:%S.000Z"
HUMAN = "%Y-%m-%d %H:%M:%S"
DEFAULT_LOCATION = "SOGCS"
MAX_RETRIES = 3

PLOT_PARAMS: dict[str, int | str] = {
    "locationCode": DEFAULT_LOCATION,
    "deviceCategoryCode": "OCEANOGRAPHICRADAR",
    "dataProductCode": "CODARCD",
    "extension": "png", # NOTE: change your extension type here
    "dpo_includeRadials": 0, # NOTE: change to 1 to include radials
}

MANIFEST_COLUMNS = [
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

# Utility functions
def ts_to_isoz(ts: datetime) -> str:
    """
    Convert a datetime object to the standard ISO string format.
    """
    return ts.strftime(DATE_ISOZ)

def utc_now_isoz() -> str:
    """
    Return the current UTC time as an ISO string.
    """
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


# PlotBatch Class
class PlotBatch:
    """
    Represents a single CODAR plot batch.
    Stores request/run IDs, timestamps, status, and file count.
    """

    def __init__(self, dpRequestId: int, batch_id: str, start: str, end: str):
        self.dpRequestId: int = dpRequestId
        self.runIds: Optional[list[int]] = None
        self.batch_id: str = batch_id
        self.start: str = start
        self.end: str = end
        self.last_call_ran = "requestDataProduct"
        self.call_status = "pending"
        self.file_count = 0
        self.downloaded = False

    @classmethod
    def from_dict(cls, row: dict):
        """
        Create a PlotBatch object from a dictionary (e.g., from CSV row).
        """
        batch = cls(row["dpRequestId"], row["batch_id"], row["start"], row["end"])
        batch.runIds = row.get("runIds")
        batch.last_call_ran = row.get("last_call_ran", batch.last_call_ran)
        batch.call_status = row.get("call_status", batch.call_status)
        batch.file_count = row.get("file_count", batch.file_count)
        batch.downloaded = row.get("downloaded", batch.downloaded)
        return batch

    def to_dict(self) -> dict:
        """
        Convert PlotBatch to a dictionary for CSV saving.
        """
        return {
            "dpRequestId": self.dpRequestId,
            "runIds": self.runIds,
            "batch_id": self.batch_id,
            "start": self.start,
            "end": self.end,
            "last_call_ran": self.last_call_ran,
            "call_status": self.call_status,
            "file_count": self.file_count,
            "downloaded": self.downloaded,
        }

    # Update methods to keep batch in sync with API calls
    def update_after_request(self, dpRequestId: int):
        """
        """
        self.dpRequestId = dpRequestId
        self.runIds = None
        self.last_call_ran = "requestDataProduct"
        self.call_status = "complete"

    def update_after_run(self, runIds: list[int], file_count: int, call_status: str):
        """
        """
        self.runIds = runIds
        self.file_count = file_count
        self.call_status = call_status
        self.last_call_ran = "runDataProduct"

    def update_after_download(self):
        """
        """       
        self.downloaded = True
        self.last_call_ran = "downloadDataProduct"
        self.call_status = "complete"

# PlotFetcher Class
class PlotFetcher:
    """
    Coordinates fetching CODAR plot batches:
    - Requests, runs, downloads plots
    - Handles threading and manifest
    - Saves provenance
    """

    def __init__(self, plot_client: onc.ONC, data_root: Path, metadata_root: Path, threads: int = 15): # TODO: change output dir var & return type
        """
        """
        self.plot_client = plot_client
        self.data_root = data_root
        self.metadata_root = metadata_root
        self.threads = threads

        # Ensure directories exist
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.metadata_root.mkdir(parents=True, exist_ok=True)
        
        # Paths for manifest and provenance
        self.batch_manifest_path = self.metadata_root / "manifest.csv"
        self.provenance_path = self.metadata_root / "provenance.yaml"
        
        # Threading
        self.batch_lock = threading.Lock()
        self.download_queue: "queue.Queue[PlotBatch]" = queue.Queue()
        self.batch_manifest: list[PlotBatch] = []

    # Provenance
    def manifest_to_prov(self):
        """
        Write a YAML file describing API calls and files downloaded.
        """
        downloaded_files = sum(b.file_count for b in self.batch_manifest if b.downloaded)
        prov = {
            "challenge": "surface-currents",
            "data-type": "plots",
            "doi": "10.34943/d32df30d-7644-446a-b0ed-9daf63377041",
            "base_url": "https://data.oceannetworks.ca/api",
            "api_calls": {},
            "file_count": downloaded_files
        }
        for batch in self.batch_manifest:
            prov["api_calls"][batch.batch_id] = {
                "requestDataProduct": {
                    "endpoint": "/dataProductDelivery/request",
                    "parameters": {**PLOT_PARAMS, "dateFrom": batch.start, "dateTo": batch.end},
                    "Request ID": batch.dpRequestId,
                },
                "runDataProduct": {
                    "endpoint": "/dataProductDelivery/run",
                    "parameters": {"Request ID": batch.dpRequestId, "waitComplete": True},
                    "Run IDs": batch.runIds,
                },
                "downloadDataProduct": {
                    "endpoint": "/dataProductDelivery/download",
                    "parameters": {"Run IDs": batch.runIds, "maxRetries": 0, "downloadResultsOnly": False,
                                   "includeMetadataFile": False, "overwrite": False},
                }
            }
        prov["last_updated"] = utc_now_isoz()
        with open(self.provenance_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(prov, f, default_flow_style=False, sort_keys=False)

    # Manifest
    def load_or_init_manifest(self, year: int):
        """
        Initialize batch_manifest according to existing CSV manifest of historical download attempts.
        Re-request data product request ids for any batches that failed previously. In there is no existing 
        CSV manifest, initialize batch_manifest as fresh list of batches.
        """
        if self.batch_manifest_path.exists():
            df = pd.read_csv(self.batch_manifest_path)
            self.batch_manifest = [PlotBatch.from_dict(r) for _, r in df.iterrows()]
            # Re-request failed batches
            for batch in self.batch_manifest:
                if not batch.downloaded:
                    dp_id = self.make_dp_request(batch.start, batch.end)
                    batch.update_after_request(dp_id)
        else:
            self.batch_manifest = self.build_quartermonth_batches(year)
        self.save_manifest()

    def save_manifest(self):
        """
        Save current batch_manifest to CSV in a thread-safe way.
        """
        with self.batch_lock:
            df = pd.DataFrame([b.to_dict() for b in self.batch_manifest])
            df.to_csv(self.batch_manifest_path, index=False)

    def build_quartermonth_batches(self, year: int) -> list[PlotBatch]:
        """
        Initializes PlotBatch objects for each quarter-month of the specified year, 
        using data product request IDs obtained via requestDataProduct. 
        """
        batches: list[PlotBatch] = []
        quarter_days = [(1,7),(8,15),(16,23),(24,None)]
        for month in range(1,13):
            for q, (start_day, end_day) in enumerate(quarter_days, start=1):
                start = pd.Timestamp(year=year, month=month, day=start_day, tz="UTC")
                if end_day is None:
                    next_month = pd.Timestamp(year=year if month<12 else year+1,
                                              month=month%12+1, day=1, tz="UTC")
                    end = next_month - timedelta(seconds=1)
                else:
                    end = pd.Timestamp(year=year, month=month, day=end_day, hour=23, minute=59, second=59, tz="UTC")
                dp_id = self.make_dp_request(ts_to_isoz(start), ts_to_isoz(end))
                batch_id = f"{year}-{month:02d}-{q}Q"
                batches.append(PlotBatch(dp_id, batch_id, ts_to_isoz(start), ts_to_isoz(end)))
        return batches

    # API calls
    def make_dp_request(self, start_iso: str, end_iso: str) -> int:
        """Return data product request id from requestDataProduct."""
        log.debug("Requesting data product from %s to %s", start_iso, end_iso)
        filters = {**PLOT_PARAMS, "dateFrom": start_iso, "dateTo": end_iso}
        response = self.plot_client.requestDataProduct(filters=filters)
        dp_request_id = response.get("dpRequestId")
        log.info("[requestDataProduct] dpRequestId: %s", dp_request_id)
        return dp_request_id

    def run_dp(self, batch: PlotBatch) -> list[int]:
        """Return data product run ids from runDataProduct. Updates batch accordingly."""
        log.debug("Running data product for batch %s with dpRequestId %s", batch.batch_id, batch.dpRequestId)
        response = self.plot_client.runDataProduct(dpRequestId=batch.dpRequestId, waitComplete=True)
        run_ids = response.get("runIds")
        if not isinstance(run_ids, list):
            run_ids = [run_ids]
        batch.update_after_run(run_ids, response.get("fileCount", 0), response.get("status", "complete"))
        self.save_manifest()
        return run_ids

    def download_dp(self, batch: PlotBatch):
        """Runs downloadDataProduct. Waits for cart to complete and updates batch accordingly."""
        log.debug("Downloading batch %s with runIds %s", batch.batch_id, batch.runIds)
        self.plot_client.downloadDataProduct(
            runId=batch.runIds[0],
            maxRetries=3,
            downloadResultsOnly=False,
            includeMetadataFile=True,
            overwrite=False,
        )
        while not self.cart_complete(batch.dpRequestId):
            log.debug("Waiting for cart to complete for batch %s", batch.batch_id)
            time.sleep(3)
        batch.update_after_download()
        self.save_manifest()
        log.debug("Download complete for batch %s", batch.batch_id)


    def cart_complete(self, dp_request_id: int) -> bool:
        """
        Check if the download cart is complete (all files ready).
        """
        return self.plot_client.checkDataProduct(dpRequestId=dp_request_id).get("cartStatus",1) == 0

    def worker(self):
        """
        Worker thread function:
        - Pull batches from the queue
        - Run data product
        - Download files
        - Retry on failure
        Debug logging only prints if debug mode is enabled.
        """
        while True:
            try:
                batch = self.download_queue.get(timeout=5)
            except queue.Empty:
                log.debug("Queue empty, worker exiting")
                break

            log.debug("Worker picked up batch %s", getattr(batch, "batch_id", "<unknown>"))

            try:
                run_ids = None
                for attempt in range(MAX_RETRIES):
                    try:
                        log.debug("Running data product for batch %s (attempt %d)", batch.batch_id, attempt)
                        run_ids = self.run_dp(batch)
                        break
                    except Exception as e:
                        log.exception("Error running data product for batch %s (attempt %d): %s", batch.batch_id, attempt, e)
                        time.sleep(5)

                if run_ids is None:
                    log.warning("Failed to run data product for batch %s after %d attempts", batch.batch_id, MAX_RETRIES)
                    continue

                for attempt in range(MAX_RETRIES):
                    try:
                        log.debug("Downloading batch %s (attempt %d)", batch.batch_id, attempt)
                        self.download_dp(batch)
                        log.info("Successfully downloaded batch %s (%d files)", batch.batch_id, batch.file_count)
                        break
                    except Exception as e:
                        log.exception("Error downloading batch %s (attempt %d): %s", batch.batch_id, attempt, e)
                        time.sleep(5)

            finally:
                self.download_queue.task_done()
                log.debug("Finished processing batch %s", getattr(batch, "batch_id", "<unknown>"))

    def run(self, year: int):
        """
        Main function to run the fetcher:
        - Load or create batch manifest
        - Queue batches
        - Start worker threads
        - Wait for completion
        - Save provenance and summary
        """
        start_time = datetime.now()
        self.load_or_init_manifest(year)
        for batch in self.batch_manifest:
            if not batch.downloaded:
                self.download_queue.put(batch)
        threads = [threading.Thread(target=self.worker) for _ in range(self.threads)]
        for t in threads:
            t.start()
        self.download_queue.join()
        for t in threads:
            t.join()
        self.manifest_to_prov()
        elapsed = (datetime.now()-start_time).total_seconds()
        total_batches = len(self.batch_manifest)
        success_batches = sum(1 for b in self.batch_manifest if b.downloaded)
        files_downloaded = sum(b.file_count for b in self.batch_manifest if b.downloaded)
        log.info("===== SUMMARY =====")
        log.info("Total batches: %d", total_batches)
        log.info("Successful batches: %d", success_batches)
        log.info("Files downloaded: %d", files_downloaded)
        log.info("Elapsed time: %.2f mins", elapsed/60)

def main() -> None:
    """
    Entry point for script execution:
    - Parse CLI arguments
    - Create ONC client
    - Initialize PlotFetcher
    - Run fetcher
    """
    cfg = parse_args()
    log.setLevel(logging.DEBUG if cfg.debug else logging.INFO)

    token = os.getenv("ONC_TOKEN")
    if not token:
        raise RuntimeError("ONC_TOKEN is not set")
    data_root = cfg.download_dir / "surface-currents" / f"plots_{cfg.year}"
    metadata_root = cfg.download_dir / "surface-currents-metadata" / f"plots_{cfg.year}"

    client = onc.ONC(token, outPath=str(data_root))
    fetcher = PlotFetcher(client, data_root, metadata_root, threads=cfg.threads)
    fetcher.run(cfg.year)

if __name__ == "__main__":
    main()