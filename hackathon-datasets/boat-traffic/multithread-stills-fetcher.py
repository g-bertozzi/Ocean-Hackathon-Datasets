# Import SDKs
import random
import threading
import urllib
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

PROJECT_ROOT = Path(__file__).resolve().parent # NOTE: for testing
# PROJECT_ROOT = Path("/Volumes/Ocean-Hackathon/boat-traffic") # NOTE: for the actual data downloads
DATA_ROOT = PROJECT_ROOT / "data" / LOCATION_CODE
METADATA_ROOT = PROJECT_ROOT / "metadata" / LOCATION_CODE

MANIFEST_PATH = Path("/Users/catherinebertozzi/hackathon-datasets/boat-traffic/metadata") / LOCATION_CODE / "manifest.csv"
PROVENANCE_PATH = Path("/Users/catherinebertozzi/hackathon-datasets/boat-traffic/metadata") / LOCATION_CODE / "provenance.yaml"

# Make sure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# --- ONC API client setup ---
TOKEN = os.getenv("ONC_TOKEN")
LOCATION_CLIENT = onc.ONC(TOKEN, outPath=str(Path(DATA_ROOT)))

# MORE GLOBALS
FILES_SUCCESS = 0


DOWNLOAD_QUEUE = queue.Queue() # Queue for files to download
RETRY_QUEUE = queue.Queue() # Queue for files that failed that we need to retry
MAX_RETRIES = 3     
manifest_lock = threading.Lock() # Memory management



API_CALL_N = 0 # Count of API calls made
NEW_MANIFEST = True

PROV_INFO = {
    "challenge": "boat-traffic",
    "description": f"1 year of still images (every 5 minutes) from shore station camera at {LOCATION_CODE}",
    "api": {
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
def load_or_init_manifest() -> None: 
    """ Updates global manifest_df variable, either loading from CSV or creating a new one."""

    global MANIFEST_DF
    # either load in manifest from CSV to dataframe
    if MANIFEST_PATH.exists():
        MANIFEST_DF = pd.read_csv(MANIFEST_PATH, parse_dates=["timestamp"])
    # or create a new dataframe
    else:
        MANIFEST_DF = pd.DataFrame(columns=[
            "timestamp","locationCode","deviceCategoryCode","deviceCode","filename","path","status"
        ])

def write_prov() -> None:
    """ Updates with download api call info and saves to YAML. """

    # Update global PROV_INFO with API call details
    global API_CALL_N, PROV_INFO

    PROV_INFO["api"][f"call_{API_CALL_N + 1}"] = {
        "endpoint": "/archivefiles",
        "parameters": {"filename": "<filename from manifest>"},
        "method": "getFile"
    }

    API_CALL_N += 1 # Update global API call count

    PROV_INFO["manifest"] = {
        "path": str(MANIFEST_PATH),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "file_count": len(MANIFEST_DF),
    }

    with open(PROVENANCE_PATH, "w") as f:
        yaml.dump(PROV_INFO, f, sort_keys=False)

    print(f"[write_prov] Provenance written to {PROVENANCE_PATH}.")

def get_6_month_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list:
    """
    Returns a list of filenames for still images from the video camera at the specified location within a 6 month period.

    Inputs:
    Output:
    """

    params = {
    'locationCode': locationCode,
    'dateFrom': dateFrom,
    'dateTo': dateTo,
    'deviceCategoryCode': "VIDEOCAM",
    'fileExtension': ".jpg", # 'jpg' for still images
    # 'returnOptions': "all" # NOTE: debugging to find file size demands
    }

    response = LOCATION_CLIENT.getArchivefileByLocation(params) # Make request 
    files = response.get('files', []) # Isolate list of files
    n = len(files) # Number of files

    # Isolate info for provenance - NOTE: done for each API call
    query_url = response.get('queryUrl')
    citations_info = response.get('citations')[0]
    url_parsed = urllib.parse.urlparse(query_url) # Parse the URL and break into components

    params_minus_token = params.copy()
    # del params_minus_token['token']
    params_minus_token.pop("token", None)
    
    # Update global PROV_INFO with API call details
    global API_CALL_N, PROV_INFO

    PROV_INFO["api"][f"call_{API_CALL_N + 1}"] = {
    "endpoint": url_parsed.path.replace("/api", "", 1),
    "parameters": params_minus_token,
    "citation": citations_info['citation'],
    "doi": citations_info['doi'],
    }

    API_CALL_N += 1 # Update global API call count

    # NOTE: debugging output
    # print("6 months of data")
    # print(f"file 1: {files[0] if files else 'No files found.'}")
    # print(f"file {n}: {files[-1] if files else 'No files found.'}")
    # print(f"num files: {n}")

    return files

def get_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list:
    """
    Returns a list of filenames for still images from the video camera at the specified location for a full year.

    Inputs:
    Output:
    """
    all_files = []
    
    start_dt = datetime.fromisoformat(dateFrom.replace("Z", "+00:00"))
    mid_dt = start_dt + timedelta(days = 183)
    # end_dt = datetime.fromisoformat(dateTo.replace("Z", "+00:00"))
    
    # First 6 months
    files1 = get_6_month_filenames(locationCode = locationCode, dateFrom = dateFrom, dateTo = mid_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
    all_files.extend(files1)

    # Second 6 months
    files2 = get_6_month_filenames(locationCode = locationCode, dateFrom = mid_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"), dateTo = dateTo)
    all_files.extend(files2)

    # NOTE: debugging output
    print(f"[get_filenames] {locationCode} YEAR TOTAL: {len(all_files)} files")

    return all_files

def filenames_to_file_info(filenames: list[str], locationCode: str) -> pd.DataFrame:
    """ Converts list of filenames to a DataFrame with metadata parsed from filenames."""

    # Parse filenames to extract metadata and build rows
    print(f"[filenames_to_file_info] Creating information table for files in this call.")
    print()

    # Create list of dictionaries: list is dataframe, each dict is a row
    manifest_list = []
    
    for fname in filenames:
        # Extract time info: YYYYMMDDHHMMSS.mmmZ
        ts_str = fname.split("_")[1].replace(".jpg", "") # Extract timestamp between the first underscore and the file extension
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
def download_batches(file_info: pd.DataFrame, batch_size: int = 2000, num_threads: int = 20, max_retries: int = 3):
    """
    Download files in batches with multithreading.
    - Splits `file_info` into batches of size `batch_size`
    - Starts workers that pull from each batch and attempt to download.
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

        print(f"[download_batches] Finished downloading batch {start_idx} to {end_idx-1}.")

        # Save manifest after each chunk
        with manifest_lock:
            MANIFEST_DF.to_csv(MANIFEST_PATH, index=False)
            print(f"[download_batches] Manifest saved. Successes: {FILES_SUCCESS}. Total entries: {len(MANIFEST_DF)}")

        # Optional small sleep to avoid hitting API too hard
        time.sleep(1)

        start_idx += batch_size

    print(f"[download_batches] All batches done downloading.")
    total_attempted = FILES_SUCCESS + MANIFEST_DF[MANIFEST_DF['status'] == 'failed'].shape[0]
    failed = total_attempted - FILES_SUCCESS
    print(f"[download_batches] Success: {FILES_SUCCESS}, Failed: {failed}")
    
def worker() -> None:
    """ Worker thread function to process the download queue.

    Will pull from queue and attempt to download. Updates manifest dataframe and global success/fail counters.
    
    """

    global FILES_SUCCESS

    while True:
        try:
            file_info = DOWNLOAD_QUEUE.get(timeout=5)
        except queue.Empty:
            break  # Exit if queue is empty

        success = download_file(file_info) # Returns bool depending on download success

        # Critical section: update manifest
        with manifest_lock:
            update_manifest_df(file_info, success)
            if success:
                FILES_SUCCESS += 1
            else:
                attempts = file_info.get("attempts", 0) + 1 # Check cur attempts val
                file_info["attempts"] = attempts # Update cur row with incremented val

                if attempts <= MAX_RETRIES:
                        DOWNLOAD_QUEUE.put(file_info)  # requeue for retry
                else:
                    update_manifest_df(file_info, False)  # permanent fail      

        DOWNLOAD_QUEUE.task_done()

def download_file(file_info: dict) -> bool:
    """Download the file and return True if successful, False otherwise. Appends fai"""

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

    start_time = time.time()  # Record start time
    print(f"[Main] Starting multithreaded still image data fetcher at {LOCATION_CLIENT}.")
    print(f"[Main] Data root: {DATA_ROOT}")
    print()
    
    # 1. Get list of filenames
    start = "2023-09-01T00:00:00.000Z"
    end = "2024-09-01T00:00:00.000Z"

    filenames = get_filenames(locationCode= LOCATION_CODE, dateFrom= start, dateTo= end)
    file_info = filenames_to_file_info(filenames=filenames, locationCode= LOCATION_CODE)

    # 2. Build global df manifest or load from CSV
    load_or_init_manifest()

    # 3. Compare list of filenames and the GLOBAL manifest
    """
    - Natural join these (new files will get a NaN in 'status' column)
    - Queue file or skip it depending on 'status' column (either NaN or 'failed' means queue it)
    """
    merged_manifest = file_info.merge(MANIFEST_DF[['filename', 'status']], on='filename', how='left')
    merged_manifest = merged_manifest.iloc[:15000] # NOTE: FOR SUBSAMPLE

    # 4. Download by batches - multithread
    num_workers = 20
    download_batches(file_info=merged_manifest, batch_size=1000, num_threads=num_workers)
    
    # 5. Final manifest and provenance save
    with manifest_lock:
        MANIFEST_DF.to_csv(MANIFEST_PATH, index=False)
    print(f"[Main] Final manifest save complete. {len(MANIFEST_DF)} total entries.")

    write_prov()

    # Print total runtime and number of files requested
    end_time = time.time()
    total_minutes = (end_time - start_time) / 60
    total_attempted = FILES_SUCCESS + MANIFEST_DF[MANIFEST_DF['status'] == 'failed'].shape[0]
    failed = total_attempted - FILES_SUCCESS

    print(f"[Main] Processed {total_attempted} files in {total_minutes:.2f} minutes with {num_workers} threads. Success: {FILES_SUCCESS}. Failed: {failed}")
    print(f"[Main] Sequential time estimate: {total_minutes * num_workers:.2f} minutes.")
    print(f"[Main] Failed files to retry: {failed}.")
    
if __name__ == "__main__":
    main()