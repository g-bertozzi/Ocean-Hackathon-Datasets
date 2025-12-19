"""
Multithreaded ONC Still Image Downloader

Downloads still images (.jpg) from Ocean Networks Canada (ONC) for a specified
location and time period. The script builds a manifest, downloads missing/failed
files in parallel, checkpoints the manifest periodically, and writes provenance.

To run (example):
    export ONC_TOKEN="your_token_here"
    python boat-traffic/fetch_shore_station_images.py
"""
from __future__ import annotations

print("[STARTUP] Initializing imports...", flush=True)

import logging
import os
import random
import sys
import threading
import time
import queue
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

print("[STARTUP] Core imports done", flush=True)

import pandas as pd
import yaml
from dotenv import load_dotenv

print("[STARTUP] Third-party imports done", flush=True)

import onc

print("[STARTUP] ONC SDK imported", flush=True)

# --- Logging -----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger(__name__)
print("[STARTUP] Logging configured", flush=True)

# --- Load environment -------------------------------------------------------
print("[STARTUP] Loading .env...", flush=True)
load_dotenv()
print("[STARTUP] .env loaded", flush=True)

# --- Immutable defaults / paths ---------------------------------------------
LOCATION_CODE: str = "CCSS"
DOWNLOADS_PATH: Path = Path.home() / "Downloads"
DATA_ROOT: Path = DOWNLOADS_PATH / "boat-traffic" / LOCATION_CODE
METADATA_ROOT: Path = DOWNLOADS_PATH / "boat-traffic-metadata" / LOCATION_CODE

MANIFEST_PATH: Path = Path(METADATA_ROOT) / "manifest.csv"
PROVENANCE_PATH: Path = Path(METADATA_ROOT) / "provenance.yaml"

print(f"[STARTUP] Creating directories...", flush=True)
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

print(f"[STARTUP] Data root: {DATA_ROOT}", flush=True)
print(f"[STARTUP] Metadata root: {METADATA_ROOT}", flush=True)

MANIFEST_COLUMNS: List[str] = [
    "timestamp",
    "locationCode",
    "deviceCategoryCode",
    "deviceCode",
    "filename",
    "path",
    "status",
]

print("[STARTUP] All module-level setup complete", flush=True)


# --- StillImageFetcher ------------------------------------------------------
class StillImageFetcher:
    """
    Encapsulate mutable state and worker logic for downloading still images.

    Attributes:
        client: ONC SDK client instance.
        data_root: directory where files are saved.
        metadata_root: directory where manifest and provenance are saved.
        location_code: location code for ONC queries.
    """

    def __init__(self, client: onc.ONC, data_root: Path, metadata_root: Path, location_code: str = LOCATION_CODE) -> None:
        self.client: onc.ONC = client
        self.data_root: Path = Path(data_root)
        self.metadata_root: Path = Path(metadata_root)
        self.manifest_path: Path = Path(metadata_root) / "manifest.csv"
        self.provenance_path: Path = Path(metadata_root) / "provenance.yaml"
        self.location_code: str = location_code

        # Mutable state (previously globals)
        self.manifest_df: pd.DataFrame = pd.DataFrame(columns=MANIFEST_COLUMNS)
        self.manifest_lock: threading.Lock = threading.Lock()
        self.download_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.files_success: int = 0
        self.files_processed: int = 0
        self.api_call_count: int = 0
        self.prov_info: Dict[str, Any] = {
            "challenge": "boat-traffic",
            "description": f"1 year of still images (every 5 minutes) from shore station camera at {location_code}",
            "api_calls": {},
            "manifest": {},
        }

        # ensure dirs
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.metadata_root.mkdir(parents=True, exist_ok=True)

    # Manifest / provenance I/O

    def load_or_init_manifest(self) -> None:
        """Load manifest from CSV if present, else initialize empty manifest."""
        if self.manifest_path.exists():
            df = pd.read_csv(self.manifest_path, parse_dates=["timestamp"])
            # ensure columns and order
            for col in MANIFEST_COLUMNS:
                if col not in df.columns:
                    df[col] = pd.NA
            self.manifest_df = df[MANIFEST_COLUMNS]
            print(f"[load_or_init_manifest] Loaded {len(self.manifest_df)} rows from {self.manifest_path}", flush=True)
        else:
            self.manifest_df = pd.DataFrame(columns=MANIFEST_COLUMNS)
            print(f"[load_or_init_manifest] Initialized empty manifest at {self.manifest_path}", flush=True)

    def write_manifest(self) -> None:
        """Persist the manifest to CSV in a thread-safe manner."""
        with self.manifest_lock:
            self.manifest_df.to_csv(self.manifest_path, index=False)
        print(f"[write_manifest] Saved manifest → {self.manifest_path} ({len(self.manifest_df)} rows)", flush=True)

    def write_provenance(self) -> None:
        """Write provenance YAML describing API calls and manifest artifact."""
        self.prov_info["manifest"] = {
            "path": str(self.manifest_path),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "file_count": int(len(self.manifest_df)),
        }
        with open(self.provenance_path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(self.prov_info, fh, sort_keys=False)
        print(f"[write_provenance] Provenance written to {self.provenance_path}", flush=True)

    # ONC listing helpers

    def get_6_month_filenames(self, date_from: str, date_to: str) -> List[str]:
        """Request a list of filenames from ONC for a 6-month-ish window."""
        params = {
            "locationCode": self.location_code,
            "dateFrom": date_from,
            "dateTo": date_to,
            "deviceCategoryCode": "VIDEOCAM",
            "fileExtension": ".jpg",
        }
        print(f"[get_6_month_filenames] Requesting files from {date_from} to {date_to}...", flush=True)
        resp = self.client.getArchivefileByLocation(params)
        files = resp.get("files", []) if isinstance(resp, dict) else []
        citations = resp.get("citations", []) if isinstance(resp, dict) else []

        citation_info = citations[0] if citations else {}
        params_minus_token = params.copy()
        params_minus_token.pop("token", None)

        self.prov_info["api_calls"][f"call_{self.api_call_count + 1}"] = {
            "Python": "getListByLocation",
            "endpoint": "/archivefile/location",
            "parameters": params_minus_token,
            "citation": citation_info.get("citation"),
            "doi": citation_info.get("doi"),
        }
        self.api_call_count += 1

        print(f"[get_6_month_filenames] Retrieved {len(files)} files from {date_from} → {date_to}", flush=True)
        return files

    def get_filenames(self, date_from: str, date_to: str) -> List[str]:
        """Get filenames for the full interval by splitting into two ~6-month ranges."""
        print(f"[get_filenames] Starting to fetch filenames for full range: {date_from} to {date_to}", flush=True)
        start_dt = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
        mid_dt = start_dt + timedelta(days=183)
        files1 = self.get_6_month_filenames(date_from, mid_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
        print(f"[get_filenames] First 6 months: {len(files1)} files", flush=True)
        files2 = self.get_6_month_filenames(mid_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"), date_to)
        print(f"[get_filenames] Second 6 months: {len(files2)} files", flush=True)
        all_files = files1 + files2
        print(f"[get_filenames] ===== TOTAL: {len(all_files)} files for {self.location_code} =====", flush=True)
        return all_files

    def filenames_to_file_info(self, filenames: List[str]) -> pd.DataFrame:
        """Parse filenames into a DataFrame matching the manifest schema."""
        print(f"[filenames_to_file_info] Creating information table for {len(filenames)} files", flush=True)
        print("", flush=True)
        rows: List[Dict[str, Any]] = []
        for fname in filenames:
            parts = fname.split("_", 1)
            if len(parts) < 2:
                print(f"[filenames_to_file_info] Skipping invalid filename: {fname}", flush=True)
                continue
            device_code = parts[0]
            ts_str = parts[1].replace(".jpg", "")
            try:
                ts = pd.to_datetime(ts_str, utc=True)
            except Exception as exc:
                print(f"[filenames_to_file_info] Could not parse timestamp from {fname}: {exc}", flush=True)
                continue
            rows.append(
                {
                    "timestamp": ts,
                    "locationCode": self.location_code,
                    "deviceCategoryCode": "VIDEOCAM",
                    "deviceCode": device_code,
                    "filename": fname,
                    "path": str(Path(self.location_code) / fname),
                }
            )
        df = pd.DataFrame(rows)
        for col in MANIFEST_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[MANIFEST_COLUMNS]
        print(f"[filenames_to_file_info] ===== PREPARED: {len(df)} rows =====", flush=True)
        return df

    # Downloading + manifest updates

    def download_file(self, file_info: Dict[str, Any]) -> bool:
        """Download a single file via ONC client. Returns True on success."""
        filename = str(file_info.get("filename"))
        try:
            self.client.getFile(filename)
            time.sleep(random.uniform(0.05, 0.2))
            return True
        except Exception as exc:
            # Only log errors, not all attempts
            return False

    def update_manifest_df(self, file_info: Dict[str, Any], success: bool) -> None:
        """
        Insert or update a single row in the manifest, ensuring the new-row DataFrame
        uses the same columns as the existing manifest to avoid pandas concat warnings.
        """
        filename = str(file_info.get("filename"))
        status = "success" if success else "failed"
        matches = self.manifest_df.index[self.manifest_df["filename"] == filename].tolist()
        if matches:
            self.manifest_df.at[matches[0], "status"] = status
        else:
            new_row = dict(file_info)
            new_row["status"] = status
            new_row_df = pd.DataFrame([new_row], columns=self.manifest_df.columns)
            if self.manifest_df.empty:
                self.manifest_df = new_row_df.copy()
            else:
                self.manifest_df = pd.concat([self.manifest_df, new_row_df], ignore_index=True)

    # Worker loop

    def worker(self, worker_id: int) -> None:
        """Worker that pulls file-info dicts from the queue and attempts downloads."""
        files_processed_by_worker = 0
        while True:
            try:
                file_info = self.download_queue.get(timeout=5)
            except queue.Empty:
                break
            try:
                success = self.download_file(file_info)
                files_processed_by_worker += 1
                with self.manifest_lock:
                    self.update_manifest_df(file_info, success)
                    if success:
                        self.files_success += 1
                        self.files_processed += 1
                    else:
                        self.files_processed += 1
                        attempts = int(file_info.get("attempts", 0)) + 1
                        file_info["attempts"] = attempts
                        if attempts <= MAX_RETRIES:
                            self.download_queue.put(file_info)
                    # Log progress every 100 files
                    if self.files_processed % 100 == 0:
                        print(f"[worker] Progress: {self.files_processed} processed, {self.files_success} success", flush=True)
            finally:
                self.download_queue.task_done()

    def download_batches(self, file_info: pd.DataFrame, batch_size: int = 2000, num_threads: int = 25) -> None:
        """
        Split `file_info` into batches and process each batch with worker threads.
        Saves manifest after each batch.
        """
        total_files = len(file_info)
        start_idx = 0
        batch_num = 1

        print(f"[download_batches] Starting batch downloads: {total_files} total files, batch size {batch_size}, {num_threads} workers", flush=True)

        while start_idx < total_files:
            end_idx = min(start_idx + batch_size, total_files)
            batch = file_info.iloc[start_idx:end_idx].copy()
            
            queued_count = 0
            for _, row in batch.iterrows():
                if pd.isna(row.get("status")) or row.get("status") == "failed":
                    self.download_queue.put(row.to_dict())
                    queued_count += 1

            batch_start_time = time.time()
            print(f"[download_batches] ===== BATCH {batch_num} ===== Queued {queued_count} files (indices {start_idx}-{end_idx - 1}) for download", flush=True)

            threads: List[threading.Thread] = []
            for worker_id in range(num_threads):
                t = threading.Thread(target=self.worker, args=(worker_id,))
                t.start()
                threads.append(t)

            self.download_queue.join()
            for t in threads:
                t.join()

            batch_elapsed = (time.time() - batch_start_time) / 60.0
            self.write_manifest()
            
            total_attempted = int(self.files_success + (self.manifest_df["status"] == "failed").sum())
            failed = total_attempted - self.files_success
            print(f"[download_batches] ===== BATCH {batch_num} COMPLETE ===== Elapsed: {batch_elapsed:.2f} min | Successes: {self.files_success} | Failed: {failed} | Total attempted: {total_attempted}", flush=True)
            
            time.sleep(1)
            start_idx += batch_size
            batch_num += 1

        print(f"[download_batches] ===== ALL BATCHES DONE ===== Total success: {self.files_success}", flush=True)

    def periodic_manifest_save(self, interval: int = 60) -> None:
        """Background checkpoint thread to save manifest periodically."""
        checkpoint_num = 0
        while True:
            time.sleep(interval)
            with self.manifest_lock:
                self.manifest_df.to_csv(self.manifest_path, index=False)
                total_attempted = int(self.files_success + (self.manifest_df["status"] == "failed").sum())
                failed = total_attempted - self.files_success
                checkpoint_num += 1
                print(f"[periodic_manifest_save] CHECKPOINT #{checkpoint_num}: Processed: {total_attempted} | Success: {self.files_success} | Failed: {failed}", flush=True)

    # Orchestration

    def run(self, date_from: str, date_to: str, batch_size: int = 2000, num_workers: int = 20) -> None:
        """Main orchestration: list, build manifest, download in batches and write provenance."""
        start_time = time.time()
        readable_time = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
        print("", flush=True)
        print(f"[Main] ========== STARTING MULTITHREADED STILL IMAGE FETCHER ==========", flush=True)
        print(f"[Main] Location: {self.location_code} | Time: {readable_time}", flush=True)
        print(f"[Main] Download location: {self.data_root}", flush=True)
        print(f"[Main] Metadata location: {self.metadata_root}", flush=True)

        # PHASE 1: List files
        print(f"[Main] ===== PHASE 1: LISTING FILES =====", flush=True)
        filenames = self.get_filenames(date_from, date_to)
        
        # PHASE 2: Build manifest
        print(f"[Main] ===== PHASE 2: BUILDING MANIFEST =====", flush=True)
        file_info_df = self.filenames_to_file_info(filenames)
        self.load_or_init_manifest()
        merged = file_info_df.merge(self.manifest_df[["filename", "status"]], on="filename", how="left")
        print(f"[Main] ===== MANIFEST READY: {len(merged)} files to process =====", flush=True)

        # PHASE 3: Start downloads
        print(f"[Main] ===== PHASE 3: STARTING DOWNLOADS =====", flush=True)
        saver = threading.Thread(target=self.periodic_manifest_save, args=(60,), daemon=True)
        saver.start()

        self.download_batches(merged, batch_size=batch_size, num_threads=num_workers)

        # PHASE 4: Finalize
        print(f"[Main] ===== PHASE 4: WRITING FINAL ARTIFACTS =====", flush=True)
        self.write_manifest()
        self.write_provenance()

        elapsed_min = (time.time() - start_time) / 60.0
        total_attempted = int(self.files_success + (self.manifest_df["status"] == "failed").sum())
        failed = total_attempted - self.files_success
        
        print(f"[Main] ========== FINISHED ==========", flush=True)
        print(f"[Main] Total files processed: {total_attempted}", flush=True)
        print(f"[Main] Successful downloads: {self.files_success}", flush=True)
        print(f"[Main] Failed downloads: {failed}", flush=True)
        print(f"[Main] Total time: {elapsed_min:.2f} minutes", flush=True)
        print(f"[Main] Sequential time estimate: {elapsed_min * num_workers:.2f} minutes", flush=True)
        print(f"[Main] Threads used: {num_workers}", flush=True)


# --- Module-level constants kept for compatibility --------------------------
MAX_RETRIES: int = 3


# --- Entrypoint -------------------------------------------------------------
def main() -> None:
    """Create ONC client, instantiate fetcher and run (keeps the original default dates)."""
    print("\n" + "="*80, flush=True)
    print("[MAIN] ========== STARTING ==========", flush=True)
    print("="*80, flush=True)
    
    print("[MAIN] Checking for ONC_TOKEN...", flush=True)
    token = os.getenv("ONC_TOKEN")
    if not token:
        print("[ERROR] ONC_TOKEN is not set!", flush=True)
        raise RuntimeError("ONC_TOKEN is not set. Please export your ONC API key before running.")
    
    print("[MAIN] Token found!", flush=True)
    print("[MAIN] ========== ENVIRONMENT ==========", flush=True)
    print(f"[MAIN] ONC_TOKEN set: Yes", flush=True)
    print(f"[MAIN] Downloads will go to: {DATA_ROOT}", flush=True)
    print(f"[MAIN] Metadata will go to: {METADATA_ROOT}", flush=True)
    print("[MAIN] =====================================", flush=True)
    
    print("[MAIN] Creating ONC client...", flush=True)
    client = onc.ONC(token, outPath=str(DATA_ROOT))
    print("[MAIN] ONC client created!", flush=True)
    
    print("[MAIN] Instantiating fetcher...", flush=True)
    fetcher = StillImageFetcher(client, data_root=DATA_ROOT, metadata_root=METADATA_ROOT, location_code=LOCATION_CODE)
    print("[MAIN] Fetcher instantiated!", flush=True)

    print("[MAIN] Starting run method...", flush=True)
    try:
        fetcher.run(date_from="2023-09-01T00:00:00.000Z", date_to="2024-09-01T00:00:00.000Z", batch_size=1000, num_workers=20)
    except KeyboardInterrupt:
        print("[MAIN] Interrupted by user — saving manifest and provenance before exit.", flush=True)
        fetcher.write_manifest()
        fetcher.write_provenance()
        raise


if __name__ == "__main__":
    print("[STARTUP] Script starting...", flush=True)
    main()