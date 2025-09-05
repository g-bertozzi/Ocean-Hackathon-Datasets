"""    

Goal layout: 

    coastal-radar/
        data/
            vectors/
            <ONC_filename>.tuv
            ...
            plots/                  # 1 year of hourly codar plots (8760 files)                 
            <ONC_filename>.png
            ...
        metadata/
            vectors/
                ...
            plots/
                manifest.csv         # 
                provenance.yaml      # 

"""

# Import SDKs
import calendar
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
from typing import Optional
import pandas as pd


# --- Setup ---
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent # NOTE: for local testing
# PROJECT_ROOT = Path("/Volumes/Ocean-Hackathon/coastal-radar") # NOTE: for shared drive download
DATA_ROOT = PROJECT_ROOT / "data/plots"
METADATA_ROOT = PROJECT_ROOT / "metadata/plots"
BATCH_MANIFEST_PATH = METADATA_ROOT / "batch-manifest.csv"
FILE_MANIFEST_PATH = METADATA_ROOT / "file-manifest.csv"
PROVENANCE_PATH = METADATA_ROOT / "provenance.yaml"

# Make sure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# ONC client
TOKEN = os.getenv("ONC_TOKEN") # NOTE: Change this to your ONC token
PLOT_CLIENT = onc.ONC(TOKEN, outPath=str(DATA_ROOT))

# --- Globals ---
BATCH_MANIFEST: Optional[pd.DataFrame] = None
FILE_MANIFEST: Optional[pd.DataFrame] = None

batch_lock = threading.Lock()
files_lock = threading.Lock()

DOWNLOAD_QUEUE = queue.Queue() # Queue for files to download
MAX_RETRIES = 3
POLL_INTERVAL = 5

# FUNCTIONS
def build_quartermonth_batches(year: int = 2023) -> pd.DataFrame:
    """
    Make quarter-of-month batches:
      - 1-7
      - 8-15
      - 16-23
      - 24-1 of next month (exclusive)
    
    Each row starts at 00:00:00 and ends at 23:59:59 of the last day.
    Gives deterministic batch_id.
    """
    rows = []
    for month in range(1, 2):
        # 1st–7th
        rows.append({
            "batch_id": f"{year}-{month:02d}-1Q",
            "start": datetime(year, month, 1),
            "end": datetime(year, month, 7, 23, 59, 59),
            "status": "pending",
            "requested_at": None,
            "completed_at": None,
            "attempt_count": 0,
        })

        # 8th–15th
        rows.append({
            "batch_id": f"{year}-{month:02d}-2Q",
            "start": datetime(year, month, 8),
            "end": datetime(year, month, 15, 23, 59, 59),
            "status": "pending",
            "requested_at": None,
            "completed_at": None,
            "attempt_count": 0,
        })

        # 16th–23rd
        rows.append({
            "batch_id": f"{year}-{month:02d}-3Q",
            "start": datetime(year, month, 16),
            "end": datetime(year, month, 23, 23, 59, 59),
            "status": "pending",
            "requested_at": None,
            "completed_at": None,
            "attempt_count": 0,
        })

        # 24th → 1st of next month (exclusive)
        if month == 12:
            # For December, roll into Jan 1 of next year
            next_month_start = datetime(year + 1, 1, 1)
        else:
            next_month_start = datetime(year, month + 1, 1)

        rows.append({
            "batch_id": f"{year}-{month:02d}-4Q",
            "start": datetime(year, month, 24),
            "end": next_month_start - timedelta(seconds=1),  # 23:59:59 of last day
            "status": "pending",
            "requested_at": None,
            "completed_at": None,
            "attempt_count": 0,
        })

    return pd.DataFrame(rows)

def load_or_init_batch_manifest(year: int = 2023) -> None:
    """ Points global BATCH_MANIFEST to the exisiting csv file df or a new df. """
    global BATCH_MANIFEST
    if BATCH_MANIFEST_PATH.exists():
        BATCH_MANIFEST = pd.read_csv(BATCH_MANIFEST_PATH, parse_dates=["requested_at", "completed_at"])
        BATCH_MANIFEST["attempt_count"] = 0 # Reset attempt_count to 0 for all batches
    else:
        BATCH_MANIFEST = build_quartermonth_batches(year)

def load_or_init_file_manifest() -> None:
    """ Points global FILE_MANIFEST to the exisiting csv file df or a new df. """
    global FILE_MANIFEST
    if FILE_MANIFEST_PATH.exists():
        FILE_MANIFEST = pd.read_csv(FILE_MANIFEST_PATH)
    else:
        FILE_MANIFEST = pd.DataFrame(columns=["batch_id", "timestamp", "locationCode",
                                   "deviceCategoryCode", "filename", "path", "status"])

# THREAD FUNCTIONS
def append_file_rows(rows: list[dict]):
    """Append new file rows in memory only (no disk write)."""
    global FILE_MANIFEST
    df_new = pd.DataFrame(rows)
    with files_lock:
        FILE_MANIFEST = pd.concat([FILE_MANIFEST, df_new], ignore_index=True)

def save_file_manifest():
    """Write file manifest to disk (call after finishing a batch)."""
    global FILE_MANIFEST
    with files_lock:
        FILE_MANIFEST.to_csv(FILE_MANIFEST_PATH, index=False)

def save_batch_manifest():
    """Write batch manifest to disk (call after finishing a batch)."""
    global BATCH_MANIFEST
    with batch_lock:
        BATCH_MANIFEST.to_csv(BATCH_MANIFEST_PATH, index=False)

def worker():
    """
    - pull batch from queue
    - attempt to download batch (process_batch())
        - parse reponse for file manifest
    - if failed: requeue
    - write manifests to csv
    - mark task as done
    """
    global BATCH_MANIFEST, FILE_MANIFEST, DOWNLOAD_QUEUE

    while True:
        try:
            # BATCH_MANIFEST SCHEMA: batch_id, start, end, status, requested_at, completed_at
            batch_info = DOWNLOAD_QUEUE.get(timeout=5) # batch_info: DICT
        except queue.Empty:
            break  # Exit if queue is empty

        try:
            # process batch
            batch_id = batch_info.get('batch_id')

            with batch_lock:
                BATCH_MANIFEST.loc[BATCH_MANIFEST["batch_id"] == batch_id, "requested_at"] = datetime.now(timezone.utc)

            batch_success = process_batch(batch_info=batch_info)
            status_str = "done" if batch_success else "failed"

            with batch_lock:
                print(f"[worker] {batch_id} processed with status: {status_str}.")
                # Update BATCH_MANIFEST -- completed_at, status, inc attempt_count
                BATCH_MANIFEST.loc[BATCH_MANIFEST["batch_id"] == batch_id, "completed_at"] = datetime.now(timezone.utc)
                BATCH_MANIFEST.loc[BATCH_MANIFEST["batch_id"] == batch_id, "status"] = status_str

                attempt_no = BATCH_MANIFEST.loc[BATCH_MANIFEST["batch_id"] == batch_id, "attempt_count"].iloc[0] + 1
                BATCH_MANIFEST.loc[BATCH_MANIFEST["batch_id"] == batch_id, "attempt_count"] = attempt_no

                # RETRY LOGIC HERE
                if not batch_success and attempt_no < MAX_RETRIES:
                        DOWNLOAD_QUEUE.put(batch_info.to_dict())
                        # time.sleep(10)  # sleep 10 seconds

                        

            # save manifests to csv (already thread safe)
            save_batch_manifest()
            save_file_manifest()

        finally:
            DOWNLOAD_QUEUE.task_done()

def process_batch(batch_info: dict) -> bool:
    """
    For one batch:
    - builds parameters
    - makes API request
    - Update FILE_MANIFEST
    - Return batch_failed (True if any file failed)
    """
    # Pull batch_id dict from row of BATCH_MANIFEST -- schema: [batch_id, start, end, status, requested_at, completed_at]
    batch_id = batch_info.get('batch_id')
    batch_success = True

    # 1. Build parameters
    batch_start = batch_info.get('start')
    batch_end = batch_info.get('end')
    batch_params = {
        "dateFrom": batch_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "dateTo": batch_end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "locationCode": LOCATION_CODE,
        "dataProductCode": DATA_PRODUCT_CODE,
        "deviceCategoryCode": DEVICE_CAT_CODE,
        "extension": EXTENSION,
        "dpo_includeRadials": 0,
    }


    # POLL UNTIL API RESPONDS?
    # 2. Make orderDataProduct request -- i.e. attempt to download all files in the batch
    files_info = None
    poll_count = 0
    max_polls = 20  # adjust as needed

    while True:
        poll_count += 1
        try:
            batch_response = PLOT_CLIENT.orderDataProduct(
                filters=batch_params,
                maxRetries=3,
                includeMetadataFile=False
            )

            if not batch_response:
                print(f"[worker] batch_response empty for batch {batch_id}, poll {poll_count}/{max_polls}")
            elif 'downloadResults' not in batch_response:
                print(f"[worker] batch_response missing 'downloadResults' for batch {batch_id}, poll {poll_count}/{max_polls}")
            else:
                files_info = batch_response.get("downloadResults")
                if files_info:
                    print(f"[worker] batch {batch_id} ready with {len(files_info)} files.")
                    break
                else:
                    print(f"[worker] batch {batch_id} not ready yet, polling {poll_count}/{max_polls}")

        except Exception as e:
            print(f"[worker] network/API error for batch {batch_id}: {e}. Retrying ({poll_count}/{max_polls})")

        if poll_count >= max_polls:
            print(f"[worker] batch {batch_id} never became ready after {max_polls} polls.")
            break

        time.sleep(POLL_INTERVAL)



    # 3. Update FILE_MANIFEST according to download response
    global FILE_MANIFEST
    
    # Iterate through response -- parse for metadata for manifest and for download status
    files_info = batch_response.get('downloadResults')
    batch_rows = []

    # Create row for FILE MANIFEST -- schema: [batch_id, timestamp, locationCode, devcatcode, filename, path, status]
    for file in files_info:

        # Extract filename
        fname = file.get('file')
        fname_split = fname.split("_")

        # Extract timestamp
        start_tmsp_str = fname_split[3]
        start_tmsp = datetime.strptime(start_tmsp_str, "%Y%m%dT%H%M%S.%fZ")

        # Status logic
        if file.get("status") == "failed": # File download failed
            status = "failed"
            batch_success = False # Set flag for whether or not batch was success
        elif file.get("status") == "success": # File download succeeded
            status = "success"
        else: # File already present
            status = "skipped"

        # Build row
        row = {
            "batch_id": batch_id,
            "start_timestamp": start_tmsp,
            "locationCode": LOCATION_CODE,
            "deviceCategoryCode": DEVICE_CAT_CODE,
            "filename": file.get('file'),
            "path": f"plots/{fname}", # make using filename and data type
            "status": status
        }

        batch_rows.append(row) # Append each row
    with files_lock:
        print(batch_rows.head(10))
    append_file_rows(batch_rows) # Append entire batch to files manifest NOTE: not saving to csv yet

    return batch_success

def main():
    """
    Flow:


    1. load or init batch manifest
        - if csv exists: load into BATCH_MANIFEST global dataframe
        - else build 1/2 month batches df and point BATCH_MANIFEST global dataframe
    
    
    2. load or init file manifest
        - if csv exists: load into FILE_MANIFEST global dataframe
        - else built empty df with set schema and point FILE_MANIFEST global dataframe

    3. iterate through BATCH_MANIFEST and append row to queue if status isnt success 

    4. workers
        - pull batch from queue
        - orderDataProduct
            - {params, maxRetries = 3, downloadResultsOnly = False, includeMetadataFile = False, overwrite = False}

        - at end of each batch:
            -  file records (loop through the API response ?)
                - extract meta for file manifest
                - write row to file manifest
                - batch scope bool to track if any file fails
            
            - retry logic for failed downloads?
                - if ANY file fails we would have to requeue the entire batch
                    - would successful ones just be over written?

            - batch records:
                - mark batch as done in batch manifest
                - write batch manifest to csv
                - write file manifest to csv
    """
    start_time = time.time()  # start timer

    global BATCH_MANIFEST, FILE_MANIFEST, DOWNLOAD_QUEUE

    # set base parameters
    global DATA_PRODUCT_CODE
    global DEVICE_CAT_CODE
    global EXTENSION 
    global LOCATION_CODE

    # PARAM CONSTANTS
    DATA_PRODUCT_CODE = "CODARQCSC"
    DEVICE_CAT_CODE = "OCEANOGRAPHICRADAR"
    EXTENSION = "png"
    LOCATION_CODE = "SOGCS"

    # 1. Init batch and file manifest
    load_or_init_batch_manifest(year=2023) # points BATCH_MANIFEST
    load_or_init_file_manifest() # points FILE_MANIFEST


    # 2. Build queue -- enqueue all batches where status != 'success"

    # BATCH_MANIFEST SCHEMA: batch_id, start, end, status, requested_at, completed_at
    for _, row in BATCH_MANIFEST.iterrows():
        if row["status"] != "success":
            DOWNLOAD_QUEUE.put(row.to_dict()) # append row as a dictionary
            print(f"[main] queueing batch ID: {row['batch_id']}, start: {row['start']}, end: {row['end']}")


    # 3. Worker Threads
    num_workers = 4 # should not be greater than num of batches
    threads = [] # Keep track of threads
    print(f"[main] Starting {num_workers} threads.")
    for _ in range(num_workers):
        t = threading.Thread(target=worker)
        t.start() # Initialize and start thread object 't'
        threads.append(t)

    # Wait for all tasks in the queue to be processed - signaled by task_done() in worker
    DOWNLOAD_QUEUE.join()

    # Main thread waits for all threads to finish
    for t in threads:
        t.join()



    # --- Logging / Summary ---
    end_time = time.time()
    elapsed_sec = end_time - start_time

    total_batches = len(BATCH_MANIFEST)
    success_batches = len(BATCH_MANIFEST[BATCH_MANIFEST["status"] == True])
    failed_batches = len(BATCH_MANIFEST[BATCH_MANIFEST["status"] == False])

    print("\n===== BATCH DOWNLOAD SUMMARY =====")
    print(f"Total batches processed : {total_batches}")
    print(f"Successful batches      : {success_batches}")
    print(f"Failed batches          : {failed_batches}")
    print(f"Elapsed time (seconds)  : {elapsed_sec:.2f} sec")
    print("\nBatch details:")
    for _, row in BATCH_MANIFEST.iterrows():
        print(f"- {row['batch_id']}: status={row['status']}, attempts={row['attempt_count']}, "
              f"requested_at={row['requested_at']}, completed_at={row['completed_at']}")
    print("==================================\n")



if __name__ == "__main__":
    main()
