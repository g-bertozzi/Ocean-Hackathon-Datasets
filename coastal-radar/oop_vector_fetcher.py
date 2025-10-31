"""
Surface current vector downloader for ONC (Oceans 3.0).

This script:
  1) Lists CODAR vector files (.tuv) for a given time range.
  2) Builds a manifest of expected files with metadata.
  3) Downloads missing or previously failed files in parallel.
  4) Periodically checkpoints the manifest to disk.
  5) Writes a provenance YAML describing API calls and artifacts.

Key ideas:
  - Thread pool with a task queue for parallel downloads.
  - A single manifest DataFrame as the source of truth.
  - One lock to protect all manifest mutations and stats counters.
  - Idempotent re-runs. Already successful files are skipped.

Outputs:
  - <Downloads>/surface-currents/vectors/*.tuv
  - <Downloads>/surface-currents-metadata/vectors/manifest.csv
  - <Downloads>/surface-currents-metadata/vectors/provenance.yaml
"""

from __future__ import annotations

import os
import queue
import threading
import time
import logging
import sys
import argparse
from pathlib import Path
from datetime import UTC, datetime
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Optional, List

import onc
import pandas as pd
import yaml

# ==============================
# Setup
# ==============================
load_dotenv()  # Environmental variables

@dataclass(frozen=True)
class Config:
    """Data class to store CLI."""
    start: str
    end: str

def parse_args() -> Config:
    """
    Parse the command line arguments and return a Config.

    Expected format for dates: ISO 8601, e.g. "2023-01-01T00:00:00.000Z".
    The script does not transform these strings. They are passed as-is to the API.
    """
    parser = argparse.ArgumentParser(description="Fetch surface current vector data from ONC.")
    parser.add_argument("--start", required=True, help="Start date (ISO 8601), e.g. 2023-01-01T00:00:00.000Z")
    parser.add_argument("--end", required=True, help="End date (ISO 8601), e.g. 2023-01-02T00:00:00.000Z")
    args = parser.parse_args()
    return Config(start=args.start, end=args.end)

def setup_logging() -> None:
    """
    Configure root logging. Uses INFO by default and prints to stdout.

    Note: Set the LOGLEVEL environment variable or adjust here if you want DEBUG.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

setup_logging()
log = logging.getLogger(__name__)
log.info("Starting surface current vectors downloader...")

# Project paths (defaults)
DOWNLOADS_PATH = Path.home() / "Downloads"
METADATA_ROOT = DOWNLOADS_PATH / "surface-currents-metadata/vectors"
DATA_ROOT = DOWNLOADS_PATH / "surface-currents/vectors"

# Ensure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# ONC client
TOKEN = os.getenv("ONC_TOKEN")
if not TOKEN:
    raise RuntimeError("ONC_TOKEN is not set. Please export your ONC API key before running.")
VECTOR_CLIENT = onc.ONC(TOKEN, outPath=str(DATA_ROOT))

# Constants
DEFAULT_LOCATION = "SOGCS"
MANIFEST_COLUMNS = [
    "timestamp",
    "locationCode",
    "deviceCategoryCode",
    "deviceCode",
    "filename",
    "path",
    "status",
]
VECTOR_PARAMS = {
    "locationCode": DEFAULT_LOCATION,
    "deviceCategoryCode": "OCEANOGRAPHICRADAR",
    "dataProductCode": "CODARCD",
    "fileExtension": "tuv",
}

def utc_now_isoz() -> str:
    """Return current UTC time as ISO 8601 string with Z suffix (format accepted by the ONC API)."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")

class SurfaceCurrentFetcher:
    """
    Encapsulates all mutable state (manifest, queue, counters, locks) and the workflow.
    This improves testability and makes concurrency safer by scoping shared state.
    """

    def __init__(
        self,
        vector_client: onc.ONC,
        data_root: Path,
        metadata_root: Path,
        default_location: str = DEFAULT_LOCATION,
    ) -> None:
        # External dependencies and paths
        self.vector_client = vector_client
        self.data_root = Path(data_root)
        self.metadata_root = Path(metadata_root)
        self.manifest_path = self.metadata_root / "manifest.csv"
        self.provenance_path = self.metadata_root / "provenance.yaml"
        self.default_location = default_location

        # Make sure directories exist
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.metadata_root.mkdir(parents=True, exist_ok=True)

        # Mutable state held by this instance
        self.download_queue: "queue.Queue[pd.Series | dict]" = queue.Queue()
        self.manifest_df: pd.DataFrame = pd.DataFrame(columns=MANIFEST_COLUMNS)
        self.files_success: int = 0
        self.files_failed: int = 0

        # One lock for all manifest mutations and counter updates
        self.manifest_lock = threading.Lock()

        # Store last API params used for file listing. Sanitized before writing provenance.
        self.api_params: dict = {}

    # --- Manifest I/O ---
    def load_or_init_manifest(self) -> None:
        """
        Load manifest.csv if it exists, else start with an empty schema.

        Always ensures MANIFEST_COLUMNS exist and are ordered. This makes downstream
        code simpler and avoids KeyError on missing columns after refactors.
        """

        if self.manifest_path.exists():
            df = pd.read_csv(self.manifest_path, parse_dates=["timestamp"])
            for col in MANIFEST_COLUMNS:
                if col not in df.columns:
                    df[col] = pd.NA
            self.manifest_df = df[MANIFEST_COLUMNS]
            log.info("[load_or_init_manifest] Loaded %d rows from %s", len(self.manifest_df), self.manifest_path)
        else:
            self.manifest_df = pd.DataFrame(columns=MANIFEST_COLUMNS)
            log.info("[load_or_init_manifest] Initialized empty manifest at %s", self.manifest_path)

    def write_manifest(self) -> None:
        """
        Persist the entire manifest DataFrame to CSV.

        Important: Always hold the manifest_lock before writing in hot paths.
        This method acquires the lock locally to keep the contract simple.
        """
        with self.manifest_lock:
            self.manifest_df.to_csv(self.manifest_path, index=False)
        log.info("[write_manifest] Saved manifest → %s (%d rows)", self.manifest_path, len(self.manifest_df))

    def write_provenance(self) -> None:
        """
        Write provenance.yaml with information about the run.

        Includes:
          - challenge and data type labels
          - API base and endpoints
          - sanitized API params for reproducibility
          - manifest path, last update time, and file count
        """

        # Avoid leaking secrets or internal client details
        safe_params = {k: v for k, v in self.api_params.items() if k not in {"token", "method"}}
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
                "path": str(self.manifest_path),
                "last_updated": utc_now_isoz(),
                "file_count": int(len(self.manifest_df)),
            },
        }
        with open(self.provenance_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(provenance, f, sort_keys=False)
        log.info("[write_provenance] Wrote provenance → %s", self.provenance_path)

    # --- ONC / filename helpers ---
    def get_filenames(self, date_from: str, date_to: str) -> List[str]:
        """
        Query ONC for archive file names in the given time window.

        Returns:
          List of filenames. The script later parses each to extract
          deviceCode and a UTC timestamp.

        Note: For large windows with pagination, consider passing allPages=True
        if the client supports it.
        """
        params = VECTOR_PARAMS | {"dateFrom": date_from, "dateTo": date_to}
        resp = self.vector_client.getArchivefileByLocation(params)
        filenames = resp.get("files", [])
        log.info("[get_filenames] Requesting vector files for %s → %s", date_from, date_to)
        log.info("[get_filenames] Files returned: %d", len(filenames))
        self.api_params = params.copy()
        return filenames

    def filenames_to_file_info(self, filenames: List[str]) -> pd.DataFrame:
        """
        Convert a list of filenames into a normalized DataFrame compatible with the manifest.

        Each row includes:
          timestamp (UTC), locationCode, deviceCategoryCode, deviceCode, filename, path
        """

        rows: List[dict] = []
        log.info("[filenames_to_file_info] Building file info table (%d files)", len(filenames))
        for fname in filenames:
            parts = fname.split("_", 1)
            device_code = parts[0]
            ts_str = parts[1].removesuffix(".tuv")
            ts = pd.to_datetime(ts_str, utc=True)
            path = str(self.data_root / fname)
            rows.append(
                {
                    "timestamp": ts,
                    "locationCode": self.default_location,
                    "deviceCategoryCode": "OCEANOGRAPHICRADAR",
                    "deviceCode": device_code,
                    "filename": fname,
                    "path": path,
                }
            )
        return pd.DataFrame(rows)

    # --- Download / manifest update ---
    def download_file(self, file_info: dict | pd.Series) -> bool:
        """
        Download a single file by filename using the ONC client.

        Returns:
          True on success. False on known, handled exceptions.

        Note: The client call here is getFile(filename). If you prefer the higher level
        method name and overwrite semantics, you can switch to:
          self.vector_client.downloadArchivefile(filename, overwrite=False)
        """
        try:
            self.vector_client.getFile(str(file_info["filename"]))
            # small pacing
            time.sleep(1)
            return True
        except (OSError, ConnectionError, RuntimeError) as exc:
            log.error("[download_file] Failed for %s: %s", file_info["filename"], exc)
            return False

    def update_manifest_df(self, file_info: dict | pd.Series, success: bool) -> None:
        """
        Insert or update a single row in the manifest in memory.

        If the filename already exists, only the status is updated.
        Otherwise a new row is appended with the correct column order.
        """
        filename = str(file_info["filename"])
        status = "success" if success else "failed"
        matches = self.manifest_df.index[self.manifest_df["filename"] == filename].tolist()
        if matches:
            self.manifest_df.at[matches[0], "status"] = status
        else:
            new_row = dict(file_info)
            new_row["status"] = status
            new_row_ordered = {col: new_row.get(col, pd.NA) for col in MANIFEST_COLUMNS}
            if self.manifest_df.empty:
                self.manifest_df = pd.DataFrame([new_row_ordered], columns=MANIFEST_COLUMNS)
            else:
                self.manifest_df = pd.concat([self.manifest_df, pd.DataFrame([new_row_ordered])], ignore_index=True)

    # --- Worker / threads ---
    def worker(self) -> None:
        """
        Worker thread loop.

        Pulls items from the queue until it is empty for 5 seconds.
        For each item:
          - attempts the download
          - updates the manifest and counters under the lock
          - signals task completion to the queue
        """

        while True:
            try:
                file_info = self.download_queue.get(timeout=5)
            except queue.Empty:
                break
            try:
                success = self.download_file(file_info)
                with self.manifest_lock:
                    self.update_manifest_df(file_info, success)
                    if success:
                        self.files_success += 1
                    else:
                        self.files_failed += 1
            finally:
                # ensure task_done called for every get()
                self.download_queue.task_done()

    def periodic_manifest_save(self, interval: int = 90) -> None:
        """
        Background thread that checkpoints the manifest to disk every N seconds.

        This limits data loss if the process is killed mid-run and provides
        visibility into progress via the log line it prints each time.
        """
        while True:
            time.sleep(interval)
            with self.manifest_lock:
                self.manifest_df.to_csv(self.manifest_path, index=False)
                log.info(
                    "[periodic_manifest_save] %s Processed: %d, Success: %d, Failed: %d",
                    utc_now_isoz(),
                    self.files_success + self.files_failed,
                    self.files_success,
                    self.files_failed,
                )

    # --- Orchestration ---
    def run(self, date_from: str, date_to: str, num_workers: int = 15) -> None:
        """        
        Main entry point for a full run over a time window.

        Steps:
          1) List filenames for the date range.
          2) Shape them into a DataFrame.
          3) Load or initialize the manifest.
          4) Enqueue only missing or previously failed files.
          5) Start a periodic saver thread for checkpoints.
          6) Launch worker threads and wait for completion.
          7) Write final manifest and provenance.
        """
        start_time = time.time()
        log.info("[Main] ===== Start vector fetcher @ %.2f =====", start_time)
        log.info("[Main] Data root: %s", self.data_root)

        filenames = self.get_filenames(date_from=date_from, date_to=date_to)
        file_info_df = self.filenames_to_file_info(filenames)

        self.load_or_init_manifest()

        merged = file_info_df.merge(self.manifest_df[["filename", "status"]], on="filename", how="left")
        to_queue = merged[merged["status"].isna() | (merged["status"] == "failed")]

        for _, row in to_queue.iterrows():
            self.download_queue.put(row)

        threading.Thread(target=self.periodic_manifest_save, args=(90,), daemon=True).start()

        threads: List[threading.Thread] = []
        for _ in range(num_workers):
            t = threading.Thread(target=self.worker)
            t.start()
            threads.append(t)

        self.download_queue.join()
        for t in threads:
            t.join()

        self.write_manifest()
        self.write_provenance()

        end_time = time.time()
        total_minutes = (end_time - start_time) / 60.0
        log.info(
            "[Main] ===== Processed %d files in %.2f min with %d threads. =====",
            self.files_success + self.files_failed,
            total_minutes,
            num_workers,
        )

def main() -> None:
    """
    Parse CLI, construct the fetcher, and run it.
    """
    cfg = parse_args()
    fetcher = SurfaceCurrentFetcher(VECTOR_CLIENT, DATA_ROOT, METADATA_ROOT)
    fetcher.run(date_from=cfg.start, date_to=cfg.end)

if __name__ == "__main__":
    main()