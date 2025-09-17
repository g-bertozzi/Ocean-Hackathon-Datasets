"""
Multithreaded ONC Still Image Downloader

This script downloads still images from Ocean Networks Canada (ONC) for a given 
location and time period. It uses the ONC API, handles batching, retries, and 
multithreaded downloads. A manifest (CSV) tracks download status, and a 
provenance file (YAML) records API calls.

Key features:
- Manifest ensures continuity across runs and after interrupts
- Mid-run interrupts are recoverable
- Supports retry logic for failed files
- Metadata and provenance are saved for reproducibility
"""


# Import SDKs
import random
import threading
import yaml
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
from dotenv import load_dotenv
import onc
import time
import queue

load_dotenv()

# --- Project-root based paths ---
LOCATION_CODE = "CRSS"

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data" / LOCATION_CODE
METADATA_ROOT = PROJECT_ROOT / "metadata" / LOCATION_CODE

MANIFEST_PATH = Path(METADATA_ROOT) / "manifest.csv"
PROVENANCE_PATH = Path(METADATA_ROOT) / "provenance.yaml"

# --- ONC API client setup ---
TOKEN = os.getenv("ONC_TOKEN")
LOCATION_CLIENT = onc.ONC(TOKEN, outPath=str(Path(DATA_ROOT)))

# MORE GLOBALS
FILES_SUCCESS = 0
MAX_RETRIES = 3
API_CALL_N = 0 # Count of API calls made    
DOWNLOAD_QUEUE = queue.Queue()
manifest_lock = threading.Lock()

PROV_INFO = {
    "challenge": "boat-traffic",
    "description": f"1 year of still images (every 5 minutes) from shore station camera at {LOCATION_CODE}",
    "api_calls": {
        # "call_1": {
        #     "endpoint": "",
        #     "parameters": {}, # locationCode, deviceCategoryCode, fileExtension, dateFrom, dateTo
        #     "citation": "",
        #     "doi": "",
        # }
        # "call_2": { ...
    },
    "manifest": {
    }
}

# FUNCTIONS
def setup_paths() -> dict:
    """Ensure required directories exist. Call once at runtime, not at import."""
    for path in [DATA_ROOT, METADATA_ROOT, MANIFEST_PATH.parent, PROVENANCE_PATH.parent]:
        path.mkdir(parents=True, exist_ok=True)

    return {
        "project_root": PROJECT_ROOT,
        "data_root": DATA_ROOT,
        "metadata_root": METADATA_ROOT,
        "manifest_path": MANIFEST_PATH,
        "provenance_path": PROVENANCE_PATH,
    }

def load_or_init_manifest() -> None: 
    """ 
    Updates global manifest_df variable, either loading from CSV or creating a new one.
   
    Manifest schema: [timestamp: pd.Datetime, locationCode: str, deviceCategoryCode: str, deviceCode: str, filename: str, path: str -- local path from data root, status: str - 'success' or 'failed' or Nan]
    """

    global MANIFEST_DF
    # either load in manifest from CSV to dataframe
    if MANIFEST_PATH.exists():
        MANIFEST_DF = pd.read_csv(MANIFEST_PATH, parse_dates=["timestamp"])
    # or create a new dataframe
    else:
        MANIFEST_DF = pd.DataFrame(columns=[
            "timestamp","locationCode","deviceCategoryCode","deviceCode","filename","path","status"
        ])

def write_manifest() -> None:
    with manifest_lock:
        MANIFEST_DF.to_csv(MANIFEST_PATH, index=False)
    print(f"[write_manifest] Final manifest save complete. {len(MANIFEST_DF)} total entries.")

def write_prov(method: str) -> None:
    """ Updates with download api call info and saves to YAML. Called once at end of main. """
    global API_CALL_N, PROV_INFO

    # getFile call details
    PROV_INFO["api_calls"][f"call_{API_CALL_N + 1}"] = {
        "Python": "getFile",
        "endpoint": "/archivefile/download",
        "parameters": {"filename": "<filename from manifest>"},
    }

    API_CALL_N += 1

    # Manifest details
    PROV_INFO["manifest"] = {
        "path": str(MANIFEST_PATH),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "file_count": len(MANIFEST_DF),
    }

    with open(PROVENANCE_PATH, "w") as f:
        yaml.dump(PROV_INFO, f, sort_keys=False)

    print(f"[write_prov] Provenance written to {PROVENANCE_PATH}.")


def get_6_month_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list[str]:
    """
    Returns a list of filenames for still images from the video camera at the specified location within a 6 month period. 

    NOTE: Split into 6 months periods to mitigate API max response of 100 000 lines. 
    """
    global API_CALL_N, PROV_INFO

    params = {
    'locationCode': locationCode,
    'dateFrom': dateFrom,
    'dateTo': dateTo,
    'deviceCategoryCode': "VIDEOCAM",
    'fileExtension': ".jpg",
    }

    response = LOCATION_CLIENT.getArchivefileByLocation(params) # Make request 
    files = response.get('files', []) # Isolate list of files

    # Isolate info for provenance and update global dictionary
    citations_info = response.get('citations')[0]

    params_minus_token = params.copy()
    params_minus_token.pop("token", None) # Remove token for printing


    PROV_INFO["api_calls"][f"call_{API_CALL_N + 1}"] = {
        "Python": "getListByLocation",
        "endpoint": "/archivefile/location",
        "parameters": params_minus_token,
        "citation": citations_info['citation'],
        "doi": citations_info['doi'],
    }

    API_CALL_N += 1

    # print(f"[get_6_month_filenames] file 1: {files[0] if files else 'No files found.'}, file {n}: {files[-1] if files else 'No files found.'}")

    return files

def get_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list[str]:
    """
    Returns a list of filenames from the getArchiveListByLocation method for still images ('deviceCategoryCode': "VIDEOCAM", 'fileExtension': ".jpg") at the specified location for a full year.

    Example output filename format: "AXISQ6074EPTZACCC8EACA584_20231123T234501.000Z.jpg"
    """
    all_files = []
    
    start_dt = datetime.fromisoformat(dateFrom.replace("Z", "+00:00")) # Convert iso string to date time to maniupulate time frame
    mid_dt = start_dt + timedelta(days = 183)
    
    # First 6 months
    files1 = get_6_month_filenames(locationCode = locationCode, dateFrom = dateFrom, dateTo = mid_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
    all_files.extend(files1)

    # Second 6 months
    files2 = get_6_month_filenames(locationCode = locationCode, dateFrom = mid_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"), dateTo = dateTo)
    all_files.extend(files2)

    print(f"[get_filenames] {locationCode} YEAR TOTAL: {len(all_files)} files")

    return all_files

def filenames_to_file_info(filenames: list[str], locationCode: str) -> pd.DataFrame:
    """ 
    Parses list of filenames from the getArchiveListByLocation method to create a metadata DataFrame manifest.

    Expected input filename format: "AXISQ6074EPTZACCC8EACA584_20231123T234501.000Z.jpg"
    Output dataframe schema: [timestamp: pd.datetime, locationCode: str, deviceCategoryCode: str, deviceCode: str, filename: str, path: str -- local path from data root]
    """

    print(f"[filenames_to_file_info] Creating information table for files in this call.")
    print()

    # Create list of dictionaries: list = dataframe, each dict = a row
    rows = []
    
    # Parse filenames to extract metadata and build rows
    for fname in filenames:
        parts = fname.split("_")
        if len(parts) < 2:
            raise ValueError(f"Invalid filename format (expected 'deviceCode_TIMESTAMP.jpg'): {fname}")

        deviceCode = parts[0]
        ts_str = ts_str = parts[1].replace(".jpg", "")

        try:
            ts = pd.to_datetime(ts_str, utc=True)
        except Exception as e:
            raise ValueError(f"Could not parse timestamp from filename '{fname}': {e}")

        path = Path(locationCode) / fname # Planned local path in data root

        rows.append({
            "timestamp": ts,
            "locationCode": locationCode,
            "deviceCategoryCode": "VIDEOCAM",
            "deviceCode": deviceCode,
            "filename": fname,
            "path": str(path)
        })
    

    return pd.DataFrame(rows)

# THREAD FUNCTIONS
def download_batches(file_info: pd.DataFrame, batch_size: int = 2000, num_threads: int = 20) -> None:
    """
    Download files using getFile method. Batches input dataframe multithreading:
    - Splits `file_info` into batches of size `batch_size`
    - Starts workers that pull from each batch and attempt to download.

    Input dataframe schema: [timestamp: pd.datetime, locationCode: str, deviceCategoryCode: str, deviceCode: str, filename: str, path: str -- local path from data root]
    """

    total_files = len(file_info)
    start_idx = 0

    while start_idx < total_files:
        # Define current batch
        end_idx = min(start_idx + batch_size, total_files)
        batch = file_info.iloc[start_idx:end_idx].copy() # make a copy

        # Iterate through batch and enqueue; new OR (failed AND under retry lim)
        for _, row in batch.iterrows():
            if pd.isna(row['status']) or row['status'] == 'failed':
                DOWNLOAD_QUEUE.put(row)

        print(f"[download_batches] Queued files {start_idx} to {end_idx-1} for download.")

        # Start worker threads; pull from queue and attempt to download
        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)

        # Wait for all threads to finish this batch
        DOWNLOAD_QUEUE.join()
        for t in threads:
            t.join()

        cur_time = time.time()
        readable_time = datetime.fromtimestamp(cur_time).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[download_batches] Finished downloading batch {start_idx} to {end_idx-1} at {readable_time}.")

        # Save manifest to csv after each batch
        with manifest_lock:
            MANIFEST_DF.to_csv(MANIFEST_PATH, index=False)
            print(f"[download_batches] Manifest saved. Successes: {FILES_SUCCESS}. Total entries: {len(MANIFEST_DF)}")

        # Small sleep
        time.sleep(1)

        start_idx += batch_size

    print(f"[download_batches] All batches done downloading.")
    total_attempted = FILES_SUCCESS + MANIFEST_DF[MANIFEST_DF['status'] == 'failed'].shape[0]
    failed = total_attempted - FILES_SUCCESS
    print(f"[download_batches] Success: {FILES_SUCCESS}, Failed: {failed}")
    
def worker() -> None:
    """ 
    Worker thread function to process the download queue.

    Will pull from queue and attempt to download. Updates manifest dataframe and global success counter. 
    Handles retry logic for failed files from each batch.
    """
    global FILES_SUCCESS

    while True:
        try:
            file_info = DOWNLOAD_QUEUE.get(timeout=5)
        except queue.Empty:
            break  # Exit if queue is empty
        
        try:
            success = download_file(file_info) # Returns bool depending on download success

            # Critical section: update manifest dataframe
            with manifest_lock:
                update_manifest_df(file_info, 'success' if success else 'failed')
                if success:
                    FILES_SUCCESS += 1 # update global counter
                else:
                    # Check how many times file has failed
                    attempts = file_info.get("attempts", 0) + 1 # Check cur attempts val
                    file_info["attempts"] = attempts # Update cur row with incremented val

                    if attempts <= MAX_RETRIES:
                            DOWNLOAD_QUEUE.put(file_info)  # requeue for retry
        finally:
            DOWNLOAD_QUEUE.task_done()

def download_file(file_info: dict) -> bool:
    """Download the file and return True if successful, else False (including exceptions)."""

    try:
        LOCATION_CLIENT.getFile(file_info['filename'])
        time.sleep(random.uniform(0.05, 0.2))

        # If download succeeds:
        return True
    
    except Exception as e:
        print(f"[download_file] Download failed for {file_info['filename']}: {e}")
        time.sleep(random.uniform(0.05, 0.2))
        print()
        return False

def update_manifest_df(file_info: dict, success: bool) -> None:
    """
    Updates the global manifest dataframe with:
     
    - the file info of the file attempted to download
    - the status of the file download: either 'success' or 'failed'
    """

    global MANIFEST_DF

    filename = file_info['filename']
    status = 'success' if success else 'failed'

    # Find the row in MANIFEST_DF with this filename
    idx = MANIFEST_DF.index[MANIFEST_DF['filename'] == filename].tolist()

    if idx:
        # Update only the status column for existing row
        MANIFEST_DF.at[idx[0], 'status'] = status
    else:
        # If not present, add a new row
        new_row = file_info.copy()
        new_row['status'] = status
        if MANIFEST_DF.empty:
            MANIFEST_DF = pd.DataFrame([new_row], columns=MANIFEST_DF.columns)
        else:
            MANIFEST_DF = pd.concat([MANIFEST_DF, pd.DataFrame([new_row])], ignore_index=True)
        
def periodic_manifest_save(interval: int = 60) -> None:
    """Periodically saves the manifest DataFrame to CSV every 'interval' seconds."""

    while True:
        time.sleep(interval)
        with manifest_lock:
            MANIFEST_DF.to_csv(MANIFEST_PATH, index=False)
            total_attempted = FILES_SUCCESS + MANIFEST_DF[MANIFEST_DF['status'] == 'failed'].shape[0]
            failed = total_attempted - FILES_SUCCESS
            print(f"[periodic_manifest_save] Saved at {datetime.now()}: "
                  f"[periodic_manifest_save] Processed: {total_attempted}, Success: {FILES_SUCCESS}, Failed: {failed}")

def main():

    setup_paths()

    # A: logging
    start_time = time.time()
    readable_time = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[Main] ========== Starting multithreaded still image data fetcher for {LOCATION_CODE} at {readable_time}. ========== ")
    print(f"[Main] Data root: {DATA_ROOT}")
    print(f"[Main] Metadata root: {METADATA_ROOT}")
    print()
    
    # 1. Get list of filenames in current request
    start = "2023-09-01T00:00:00.000Z"
    end = "2024-09-01T00:00:00.000Z"

    filenames = get_filenames(locationCode= LOCATION_CODE, dateFrom= start, dateTo= end)
    file_info = filenames_to_file_info(filenames=filenames, locationCode= LOCATION_CODE)

    # 2. Build global df manifest or load from CSV (tracks download history)
    load_or_init_manifest()

    # 3. Build download queue -- Compare list of filenames and the GLOBAL manifest and queue filenames depending on status column
    """
    - Natural join these (new files will get a NaN in 'status' column)
    - Queue file or skip it depending on 'status' column (either NaN or 'failed' means queue it)
    """
    merged_manifest = file_info.merge(MANIFEST_DF[['filename', 'status']], on='filename', how='left')
    # merged_manifest = merged_manifest.iloc[:20000] # NOTE: FOR SUBSAMPLE

    # 4. Download by batches - multithread
    num_workers = 20
    download_batches(file_info=merged_manifest, batch_size=1000, num_threads=num_workers)
    
    # 5. Final manifest and provenance save to CSV
    write_manifest()
    write_prov()

    # Z: Logging summary
    end_time = time.time()
    total_minutes = (end_time - start_time) / 60
    total_attempted = FILES_SUCCESS + MANIFEST_DF[MANIFEST_DF['status'] == 'failed'].shape[0]
    failed = total_attempted - FILES_SUCCESS

    print(f"[Main] ========== Processed {total_attempted} files in {total_minutes:.2f} minutes with {num_workers} threads. Success: {FILES_SUCCESS}. Failed: {failed}. ==========")
    print(f"[Main] Sequential time estimate: {total_minutes * num_workers:.2f} minutes.")
    print(f"[Main] Failed files to retry: {failed}.")
    
if __name__ == "__main__":
    main()