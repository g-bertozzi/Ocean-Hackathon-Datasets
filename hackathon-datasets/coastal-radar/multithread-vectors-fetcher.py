# SDKs
import threading
import yaml
import urllib
import os, shutil
from pathlib import Path
from datetime import datetime, timedelta
import math
import pandas as pd
from dotenv import load_dotenv
import onc
import time
import queue

# --- Setup ---
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data/vectors"
METADATA_ROOT = PROJECT_ROOT / "metadata/vectors"
MANIFEST_PATH = METADATA_ROOT / "manifest.csv"

# Make sure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# ONC client
TOKEN = os.getenv("ONC_TOKEN")
VECTOR_CLIENT = onc.ONC(TOKEN, outPath=str(DATA_ROOT))

# Global vars
SOG_LOCATION = "SOGCS"


DOWNLOAD_QUEUE = queue.Queue()

FILES_SUCCESS = 0
FILES_FAILED = 0

# --- Manifest Management ---
manifest_lock = threading.Lock()
# manifest_df = None

def load_or_init_manifest() -> None: 
    """ Updates global manifest_df variable, either loading from CSV or creating a new one."""
    global manifest_df
    # either load in manifest from CSV to dataframe
    if MANIFEST_PATH.exists():
        manifest_df = pd.read_csv(MANIFEST_PATH, parse_dates=["timestamp"])
    # or create a new dataframe
    else:
        manifest_df = pd.DataFrame(columns=[
            "timestamp", "locationCode", "deviceCategoryCode",
            "deviceCode", "filename", "path", "status"
        ])


def get_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list[str]:
    """ Returns list of filenames via getArchivefileByLocation """

    params = {
    'locationCode': locationCode,
    'dateFrom': dateFrom,
    'dateTo': dateTo,
    'deviceCategoryCode': "OCEANOGRAPHICRADAR",
    'dataProductCode': "CODARCD",
    'fileExtension': ".tuv"
    }

    response = VECTOR_CLIENT.getArchivefileByLocation(params) # Make request 
    filenames = response.get('files', []) # Isolate list of files
    n = len(filenames) # Number of files

    print(f"[{locationCode} total]: {len(filenames)} files")

    return filenames

def filenames_to_file_info(filenames: list[str], locationCode: str) -> pd.DataFrame:
    """ Converts list of filenames to a DataFrame with metadata parsed from filenames."""

    # Parse filenames to extract metadata and build rows
    print(f"Creating information table for {locationCode}.")

    # Create list of dictionaries: list is dataframe, each dict is a row
    manifest_list = []
    
    for fname in filenames:
        # Extract time info: YYYYMMDDHHMMSS.mmmZ
        ts_str = fname.split("_")[1].replace(".tuv", "") # Extract timestamp between the first underscore and the file extension
        ts = pd.to_datetime(ts_str, utc = True)  # Convert to datetime object in UTC

        deviceCode = fname.split("_")[0]
        path = Path(locationCode) / fname # Planned local path

        manifest_list.append({
            "timestamp": ts,
            "locationCode": locationCode,
            "deviceCategoryCode": "VIDEOCAM",
            "deviceCode": deviceCode,
            "filename": fname,
            "path": str(path)
        })
    
    # Convert list of dicts to DataFrame
    file_info_df = pd.DataFrame(manifest_list)

    return file_info_df

# THREAD FUNCTIONS


def update_manifest_df(file_info: dict, success: bool) -> None:
    """
    Updates the global manifest_df with:
     
    - the file info of the file attempted to download
    - the status of the file download
    
    NOTE: Global manifest will only ever show files that have been attempted to download
    If I ever want to erase my download progess, before I re run the script I only have to delete 
    the manfiest csv. The files alrady downloaded will be overwritten by the ONC client.

    """

    global manifest_df

    filename = file_info['filename']
    status = 'success' if success else 'failed'

    # Find the row in manifest_df with this filename
    idx = manifest_df.index[manifest_df['filename'] == filename].tolist()

    if idx:
        # Update only the status column for existing row
        manifest_df.at[idx[0], 'status'] = status
    else:
        # If not present, add a new row
        new_row = file_info.copy()
        new_row['status'] = status
        if manifest_df.empty:
            manifest_df = pd.DataFrame([new_row], columns=manifest_df.columns)
        else:
            manifest_df = pd.concat([manifest_df, pd.DataFrame([new_row])], ignore_index=True)
        

def periodic_manifest_save(interval=180) -> None:
    """Periodically saves the manifest DataFrame to CSV every 'interval' seconds."""
    while True:
        time.sleep(interval)
        with manifest_lock:
            manifest_df.to_csv(MANIFEST_PATH, index=False)
            print(f"[Manifest] Saved at {datetime.now()}: "
                  f"Processed: {FILES_SUCCESS + FILES_FAILED}, Success: {FILES_SUCCESS}, Failed: {FILES_FAILED}")

def download_file(file_info: dict) -> bool:
    """Download the file and return True if successful, False otherwise."""

    try:
        VECTOR_CLIENT.getFile(file_info['filename'])
        time.sleep(1)

        # If download succeeds:
        return True
    
    except Exception as e:
        print(f"Download failed for {file_info['filename']}: {e}")
        return False
    
def worker():
    global FILES_SUCCESS, FILES_SUCCESS
    while True:
        try:
            file_info = DOWNLOAD_QUEUE.get(timeout=5)
        except queue.Empty:
            break  # Exit if queue is empty

        success = download_file(file_info)

        # Critical section: update manifest
        with manifest_lock:
            update_manifest_df(file_info, success)
            if success:
                FILES_SUCCESS += 1
            else:
                FILES_FAILED += 1

        DOWNLOAD_QUEUE.task_done()   
    


def main():
    """"""
    start_time = time.time()  # Record start time

    # 1. Get list of filenames
    yr_start = "2023-01-01T00:00:00.000Z"
    yr_end = "2023-01-04T00:00:00.000Z"

    filenames = get_filenames(locationCode=SOG_LOCATION, dateFrom=yr_start, dateTo=yr_end) # List of filenames from ONC
    file_info = filenames_to_file_info(filenames=filenames, locationCode=SOG_LOCATION) # DataFrame with metadata parsed from filenames

    # 2. Build GLOBAL manifest from any previous script runs
    load_or_init_manifest() # Either from memory if it exists else empty df 

    # 3. Compare list of filenames and the GLOBAL manifest
    """
    - Natural join these (new files will get a NaN in 'status' column)
    - Queue file or skip it depending on 'status' column (either NaN or 'failed' means queue it)
    """
    merged_manifest = file_info.merge(manifest_df[['filename', 'status']], on='filename', how='left')

    # 4. Queue only missing or failed files
    for _, row in merged_manifest.iterrows():
        if pd.isna(row['status']) or row['status'] == 'failed': # If nan or failed
            DOWNLOAD_QUEUE.put(row) # Then enqueue NOTE: THREAD SAFE

    # Start periodic manifest save thread
    threading.Thread(target=periodic_manifest_save, args=(180,), daemon=True).start()

    # 5. Worker Threads
    """
    Pull from queue (syncrhonized automatically by queue library) # CRIT SECTION but THREAD SAFE
    Call downloaded file # not crit section THREAD SAFE
        - 
    Check whether success or failure 

    * Update manifest with this sucess or failure in the 'status' column* (CRIT SECTION needs lock)

    * Periodically save manifest to disk * (CRIT SECTION needs lock)

    loop until queue in empty
    
    """
    num_workers = 4 
    threads = [] # Keep track of threads

    for _ in range(num_workers):
        t = threading.Thread(target=worker)
        t.start() # Initialize and start thread object 't'
        threads.append(t)

    # Wait for all tasks in the queue to be processed - signaled by task_done() in worker
    DOWNLOAD_QUEUE.join()

    # Main thread waits for all threads to finish
    for t in threads:
        t.join()

    # Final manifest save
    with manifest_lock:
        manifest_df.to_csv(MANIFEST_PATH, index=False)
    print("[Manifest] Final save complete.")

    # Print total runtime and number of files requested
    end_time = time.time()
    total_seconds = end_time - start_time
    print(f"Processed {FILES_SUCCESS + FILES_FAILED} files in {total_seconds:.2f} seconds.")


if __name__ == "__main__":
    main()