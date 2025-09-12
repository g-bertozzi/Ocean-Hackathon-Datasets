"""    
NOTE: This script can handle mid run interrupts. A global manifest csv file will be downloaded and update periodically throughout running,
    tracking all filenames it attempts to download, and the status of these downloads. To erase any download progress simply delete the 
    'coastal-radar/metadata/vectors/manifest.csv' file. Any files already downloaded will be overwritten by the ONC client.

    This vector data was already present in the ONC archive at run time.

    Goal layout: 

    coastal-radar/
        data/
            vectors/                # 1 year of hourly vectors (8760 files)
            <ONC_filename>.tuv
            ...
            plots/                 
            <ONC_filename>.png
            ...
        metadata/
            vectors/
                manifest.csv          # 7 columns: ["timestamp", "locationCode", "deviceCategoryCode", "deviceCode", "filename", "path", "status"]
                provenance.yaml       # challenge name, data description, api call descriptions
            plots/
                ...

"""

# Import SDKs
import threading
import yaml
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import onc
import time
import queue

# --- Setup ---
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data/vectors(2024)"
METADATA_ROOT = PROJECT_ROOT / "metadata/vectors(2024)"
MANIFEST_PATH = METADATA_ROOT / "manifest.csv"
PROVENANCE_PATH = METADATA_ROOT / "provenance.yaml"

# Make sure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# ONC client
TOKEN = os.getenv("ONC_TOKEN") # NOTE: Change this to your ONC token
VECTOR_CLIENT = onc.ONC(TOKEN, outPath=str(DATA_ROOT))

# Global vars
SOG_LOCATION = "SOGCS"
API_PARAMS = {} # For provenance
FILES_SUCCESS = 0
FILES_FAILED = 0

DOWNLOAD_QUEUE = queue.Queue() # Queue for files to download
manifest_lock = threading.Lock()

# FUNCTIONS
def load_or_init_manifest() -> None: 
    """ Updates global manifest_df variable, either loading from CSV or creating a new one."""

    global MANIFEST_DF
    # either load in manifest from CSV to dataframe
    if MANIFEST_PATH.exists():
        MANIFEST_DF = pd.read_csv(MANIFEST_PATH, parse_dates=["timestamp"])
    # or create a new dataframe
    else:
        MANIFEST_DF = pd.DataFrame(columns=[
            "timestamp", "locationCode", "deviceCategoryCode",
            "deviceCode", "filename", "path", "status"
        ])

def get_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list[str]:
    """ 
    Returns list of filenames via getArchivefileByLocation.
     
    Example output filename format: "AXISQ6074EPTZACCC8EACA584_20231123T234501.000Z.jpg"
    """

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

    print(f"[get_filenames] Requesting files from ONC for location {locationCode} between {dateFrom} and {dateTo}.")
    print(f"[get_filenames] Total number of files for this call: {len(filenames)} files")
    print()

    # Update dict for provenance
    global API_PARAMS
    API_PARAMS = params

    return filenames

def filenames_to_file_info(filenames: list[str], locationCode: str) -> pd.DataFrame:
    """ 
    Parses list of filenames from the getArchiveListByLocation method to create a metadata DataFrame manifest.
    
    Expected input filename format: "AXISQ6074EPTZACCC8EACA584_20231123T234501.000Z.jpg"
    Output dataframe schema: [timestamp: pd.datetime, locationCode: str, deviceCategoryCode: str, deviceCode: str, filename: str, path: str -- local path from data root]
    """

    # Parse filenames to extract metadata and build rows
    print(f"[filenames_to_file_info] Creating information table for files in this call.")
    print()

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

def write_provenance() -> None:
    """ Writes provenance YAML file."""

    safe_params = API_PARAMS.copy()
    safe_params.pop("token", None)  # remove token if present
    safe_params.pop("method", None) # redundancy
    
    provenance = {
        "challenge": "coastal-radar",
        "data-type": "vector",

        "api": {
            "base_url": "https://data.oceannetworks.ca/api",
            "getListByLocation": {
                "endpoint": "/archivefile/location",
                "parameters": safe_params
            },
            "getFile": {
                "endpoint": "/archivefile/download",
                "parameters": {
                    "filename": "<filename from manifest>"
                },
            }  
        },
        "manifest": {
            "path": str(MANIFEST_PATH),
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "file_count": len(MANIFEST_DF),
        },
    }

    with open(PROVENANCE_PATH, "w") as f:
        yaml.dump(provenance, f, sort_keys=False)
    
    print(f"[write_provenance] Provenance written to {PROVENANCE_PATH}.")

def write_manifest() -> None:
    with manifest_lock:
        MANIFEST_DF.to_csv(MANIFEST_PATH, index=False)
    print(f"[write_manifest] Final manifest save to csv complete. {len(MANIFEST_DF)} total entries.")

# THREAD FUNCTIONS
def worker() -> None:
    """ 
    Worker thread function to process the download queue.
    - Pulls from queue
    - Attempts to download
    - Updates manifest dataframe accordingly
    """

    global FILES_SUCCESS, FILES_FAILED
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

def update_manifest_df(file_info: dict, success: bool) -> None:
    """
    Updates the global manifest dataframe with:
     
    - the file info of the file attempted to download
    - the status of the file download
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
        
def periodic_manifest_save(interval: int = 90) -> None:
    """Periodically saves the manifest DataFrame to CSV every 'interval' seconds."""

    while True:
        time.sleep(interval)
        with manifest_lock:
            MANIFEST_DF.to_csv(MANIFEST_PATH, index=False)
            print(f"[periodic_manifest_save] Saved at {datetime.now()}: "
                  f"[periodic_manifest_save] Processed: {FILES_SUCCESS + FILES_FAILED}, Success: {FILES_SUCCESS}, Failed: {FILES_FAILED}")

def download_file(file_info: dict) -> bool:
    """ Download the file and return True if successful, False otherwise (including exceptions)."""

    try:
        VECTOR_CLIENT.getFile(file_info['filename'])
        time.sleep(1)

        # If download succeeds:
        return True
    
    except Exception as e:
        print(f"[download_file] Download failed for {file_info['filename']}: {e}")
        print()
        return False
    
def main():
    """"""
    # A: Initial Logging
    start_time = time.time()
    print(f"[Main] ====== Starting multithreaded vector data fetcher at {start_time}. ====== ")
    print(f"[Main] Data root: {DATA_ROOT}")
    print()
    
    # 1. Get list of filenames
    req_start = "2024-01-01T00:00:00.000Z"
    req_end = "2025-01-01T00:00:00.000Z"

    filenames = get_filenames(locationCode=SOG_LOCATION, dateFrom=req_start, dateTo=req_end) # List of filenames from ONC
    file_info = filenames_to_file_info(filenames=filenames, locationCode=SOG_LOCATION) # DataFrame with metadata parsed from filenames

    # 2. Build GLOBAL manifest from any previous script runs
    load_or_init_manifest()

    # 3. Compare list of filenames and the GLOBAL manifest -- check for files that are either new in this request or failed in past request
    """
    - Natural join these (new files will get a NaN in 'status' column)
    - Queue file or skip it depending on 'status' column (either NaN or 'failed' means queue it)
    """
    merged_manifest = file_info.merge(MANIFEST_DF[['filename', 'status']], on='filename', how='left')

    # 4. Build download queue -- queue only missing or failed files
    for _, row in merged_manifest.iterrows():
        if pd.isna(row['status']) or row['status'] == 'failed': # If nan or failed
            DOWNLOAD_QUEUE.put(row)

    # 5. Start threads 
    threading.Thread(target=periodic_manifest_save, args=(90,), daemon=True).start() # Periodic manifest save thread

    num_workers = 10
    threads = []

    for _ in range(num_workers):
        t = threading.Thread(target=worker)
        t.start() # Initialize and start thread object 't'
        threads.append(t)

    DOWNLOAD_QUEUE.join() # Wait for all tasks in the queue to be processed - signaled by task_done() in worker

    # Main thread waits for all threads to finish
    for t in threads:
        t.join()

    # 6. Final manifest and provevnance save to file
    write_manifest()
    write_provenance()

    # Z: Summary Logging 
    end_time = time.time()
    total_minutes = (end_time - start_time) / 60
    print(f"[Main] ===== Processed {FILES_SUCCESS + FILES_FAILED} files in {total_minutes:.2f} minutes with {num_workers} threads. =====")
    print(f"[Main] Sequential time estimate: {total_minutes * num_workers:.2f} minutes.")

if __name__ == "__main__":
    main()