"""


"""

# Import SDKs
import threading
import traceback
import yaml
import os
from pathlib import Path
from datetime import datetime, timedelta
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
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data/plots-2025"
METADATA_ROOT = PROJECT_ROOT / "metadata/plots-2025"
BATCH_MANIFEST_PATH = METADATA_ROOT / "batch-manifest.csv"
PROVENANCE_PATH = METADATA_ROOT / "provenance.yaml"

# Make sure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# ONC client
TOKEN = os.getenv("ONC_TOKEN") # NOTE: Change this to your ONC token
PLOT_CLIENT = onc.ONC(TOKEN, outPath=str(DATA_ROOT))

# --- Globals ---
BATCH_MANIFEST: Optional[pd.DataFrame] = None

batch_lock = threading.Lock()
log_lock = threading.Lock()

DOWNLOAD_QUEUE = queue.Queue() # Queue for files to download
MAX_RETRIES = 3
POLL_INTERVAL = 5

# Base parameters
DATA_PRODUCT_CODE = "CODARCD"
DEVICE_CATEGORY_CODE = "OCEANOGRAPHICRADAR"
EXT = "png"
LOCATION_CODE = "SOGCS"

# FUNCTIONS
def manifest_to_prov():
    """ Writes a provevnance to yaml according to the final BATCH_MANIFEST. """
    global BATCH_MANIFEST

    downloaded_files = int(BATCH_MANIFEST.loc[BATCH_MANIFEST["downloaded"] == True, "file_count"].sum())
    
    prov = {
        "challenge": "coastal-radar",
        "data-type": "plots",
        "base_url": "https://data.oceannetworks.ca/api",
        "api_calls": {},
        "file_count": downloaded_files 
    }

    for _, row in BATCH_MANIFEST.iterrows():  # loop properly over DataFrame rows
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
                        "locationCode": LOCATION_CODE,
                        "deviceCategoryCode": DEVICE_CATEGORY_CODE,
                        "dataProductCode": DATA_PRODUCT_CODE,
                        "extension": EXT,
                        "dpo_includeRadials": 0,
                        "dateFrom": cur_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                        "dateTo": cur_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
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
    with open(PROVENANCE_PATH, "w") as f:
        yaml.dump(prov, f, default_flow_style=False, sort_keys=False)

def build_quartermonth_batches(year: int) -> pd.DataFrame:
    """
    Makes quarter-of-month batch manifest and uses requestDataProduct method to request data products for each batch.

    Schema: [dpRequestId: int, runIds: int, batch_id: str, start: iso, end: iso str, last_call_ran: str, call_status: str, file_count: int, downloaded: bool]
    """

    rows = []

    # Define quarter ranges as (start_day, end_day)
    quarter_days = [
        (1, 7),    # Q1
        (8, 15),   # Q2
        (16, 23),  # Q3
        (24, None) # Q4 handled specially
    ] 
    
    for month in [11,12]:
        for q, (start_day, end_day) in enumerate(quarter_days, start=1):
            start = pd.Timestamp(year=year, month=month, day=start_day, tz="UTC")

            if q < 4:  # Q1–Q3 fixed end day
                end = pd.Timestamp(
                    year=year, month=month, day=end_day, hour=23, minute=59, second=59, tz="UTC"
                )
            else:  # Q4 goes until the day before the next month starts
                if month == 12:
                    next_month_start = pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC")
                else:
                    next_month_start = pd.Timestamp(year=year, month=month + 1, day=1, tz="UTC")
                end = next_month_start - timedelta(seconds=1)

            params = {
                "dateFrom": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "dateTo": end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "dataProductCode": DATA_PRODUCT_CODE,
                "deviceCategoryCode": DEVICE_CATEGORY_CODE,
                "locationCode": LOCATION_CODE,
                "dpo_includeRadials": 0,
                "extension": EXT,
            }

            dp_id = make_dp_request(filters=params)

            rows.append({
                "dpRequestId": dp_id,
                "runIds": None,
                "batch_id": f"{year}-{month:02d}-{q}Q",
                "start": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "end":   end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "last_call_ran": "requestDataProduct",
                "call_status": "complete",
                "file_count": 0,
                "downloaded": False,
            })

    return pd.DataFrame(rows)

def load_or_init_batch_manifest(year: int) -> None:
    """ 
    Points global BATCH_MANIFEST to the exisiting csv file df or a new df. 
    Schema: [dpRequestId: int, runIds: int or None, batch_id: str, start: iso str , end: iso str, last_call_ran: str, call_status: str, file_count: int, downloaded: bool]
    """
    global BATCH_MANIFEST

    if BATCH_MANIFEST_PATH.exists():
        BATCH_MANIFEST = pd.read_csv(BATCH_MANIFEST_PATH)

        # Re make requestIds for all rows wehere downloaded == False -- also reset last_call_ran, call_status, runIds
        for idx, row in BATCH_MANIFEST.iterrows():
            if row["downloaded"] != True: 
                re_start = row["start"]
                re_end = row["end"]

                params = {
                    "dateFrom": re_start,
                    "dateTo": re_end,
                    "dataProductCode": DATA_PRODUCT_CODE,
                    "deviceCategoryCode": DEVICE_CATEGORY_CODE,
                    "locationCode": LOCATION_CODE,
                    "dpo_includeRadials": 0,
                    "extension": EXT,
                }

                req_id = make_dp_request(filters=params)

                BATCH_MANIFEST.loc[idx, ["dpRequestId", "runIds", "last_call_ran", "call_status"]] = [req_id, None, "make_dp_request", "complete"]

    else:
        BATCH_MANIFEST = build_quartermonth_batches(year=year)

def make_dp_request(filters: dict) -> int:

    response = PLOT_CLIENT.requestDataProduct(filters=filters)
    dpRequestId = response.get("dpRequestId")

    print(f"[requestDataProduct] dpRequestId: {dpRequestId}")

    return dpRequestId

# THREAD FUNCTIONS
def save_batch_manifest():
    """Write batch manifest to disk (call after finishing a batch)."""
    global BATCH_MANIFEST
    with batch_lock:
        BATCH_MANIFEST.to_csv(BATCH_MANIFEST_PATH, index=False)

def cart_complete(id: int) -> bool:
    """ Returns True if cart is closed, i.e. last_call_ran is complete. """
    response = PLOT_CLIENT.checkDataProduct(dpRequestId=id) 
    cart_status = response.get('cartStatus') # -> int where 0 = Closed and 1 = Open

    return True if cart_status == 0 else False

def run_dp(dpRequestId: int) -> list[int]:
    """ 
    Takes dpRequestId from requestDataProduct and returns runIds. 
    Updates BATCH_MANIFEST with runIds, last_call_ran, call_status.

    NOTE: No error handling? either returns the Id or an error
    """
    with log_lock:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[run_dp] trying to get runIds for requestId {dpRequestId} at {now}.")

    global BATCH_MANIFEST

    response = PLOT_CLIENT.runDataProduct(dpRequestId=dpRequestId, waitComplete=True)
    runIds = response.get('runIds') #NOTE: times out here and never seems to finish running?

    if not runIds:
        raise ValueError(f"Missing runIds in response: {response}")
    else:
        with log_lock:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[run_dp] got runIds {runIds} for requestId {dpRequestId} at {now}.")

    call_status = response.get("status") # Get status of runDataProduct -- waitComplete should wait for status == 'complete'
    num_files = response.get("fileCount")

    with batch_lock: # update batch manifest last_call_ran
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dpRequestId, "runIds"] = runIds
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dpRequestId, "last_call_ran"] = "runDataProduct"
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dpRequestId, "call_status"] = call_status
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dpRequestId, "file_count"] = num_files
    
    save_batch_manifest()
    
    return runIds

def download_dp(runIds: int, dpRequestId: int):
    """ Takes dpRequestId from requestDataProduct and runIds from runDataProduct and returns True unless error is raised by API. """
    
    with log_lock:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[download_dp] trying to download for requestId {dpRequestId} runIds {runIds} at {now}.")

    global BATCH_MANIFEST

    response = PLOT_CLIENT.downloadDataProduct(runId=runIds, 
                                            maxRetries=3, 
                                            downloadResultsOnly=False, 
                                            includeMetadataFile=True, 
                                            overwrite=False)

    # Poll until cart closes
    while cart_complete(id=dpRequestId) is False:
        time.sleep(3)
        with log_lock:
            print(f"[download_dp] requestId {dpRequestId} downloading in progess.")

    with log_lock:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[download_dp] downloaded runIds {runIds} at {now}.")

    with batch_lock:
        # update batch manifest last_call_ran
        BATCH_MANIFEST.loc[BATCH_MANIFEST["runIds"] == runIds, "last_call_ran"] = "downloadDataProduct"
        BATCH_MANIFEST.loc[BATCH_MANIFEST["runIds"] == runIds, "call_status"] = "complete"
        BATCH_MANIFEST.loc[BATCH_MANIFEST["runIds"] == runIds, "downloaded"] = True
    
    save_batch_manifest()

    return True

def worker():
    """
    Each worker:
    - pulls file_info dict/ row from download queue
    - attempts to get runIds except errors, requeues if no runIds returned (max retries = 3)
    - attempts to download (max retries = 3, API retries = 3) 
    """

    while True:

        # 1. Pull from queue / break when empty
        try:
            batch_info = DOWNLOAD_QUEUE.get(timeout=5) # -> batch_info: dict
        except queue.Empty:
            break

        req_id = batch_info['dpRequestId'] # -> dpRequestId: int

        # 2. Run data product
        for attempt in range(MAX_RETRIES):
            try:
                run_ids = run_dp(dpRequestId=req_id) # -> runIds: list[int]
                run_id = run_ids[0]
                break
            except Exception as e:
                print(f"[worker] runDataProduct failed (attempt {attempt+1}): {e}")
                traceback.print_exc()
                time.sleep(5)
                
        if run_ids is None:
            print(f"[worker] runDataProduct failed permanently for {req_id}")
            DOWNLOAD_QUEUE.task_done()
            continue
        
        with log_lock:
            print(f"[worker] manifest after run_dp request id {req_id}.")
            print(BATCH_MANIFEST)

        print(f"[worker] done run_dp for request id {req_id}")

        # 3. Download data product -- 
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                download_dp(runIds=run_id, dpRequestId=req_id)
                success = True
                break
            except Exception as e:
                print(f"[worker] download_dp failed (attempt {attempt+1}): {e}")
                traceback.print_exc()
                time.sleep(5)
        if not success:
            print(f"[worker] download_dp failed permanently for request id {req_id}.")
     
        DOWNLOAD_QUEUE.task_done()
  
        with log_lock:
                print(f"[worker] manifest after download_dp for request id {req_id}.")
                print(BATCH_MANIFEST)


def main():
    # A. Initial Logging
    start_time = datetime.now()  # start timer
    print(f"[main] ===== BATCH START ===== at {start_time.strftime("%Y-%m-%d %H:%M:%S")}.")

    global BATCH_MANIFEST, DOWNLOAD_QUEUE

    # 1. Init batch manifest
    load_or_init_batch_manifest(year=2022)
    print(BATCH_MANIFEST)

    # 2. Build download queue -- do we need to check current request vs historical batch?
    for _, row in BATCH_MANIFEST.iterrows():
        if row["downloaded"] != True: # Enqueue all batches not yet downloaded
            DOWNLOAD_QUEUE.put(row.to_dict())
            print(f"[main] queueing dpRequestId: {row['dpRequestId']}, start: {row['start']}, end: {row['end']}")
    
    # 3. Start threads
    num_workers = 15
    threads = []

    print(f"[main] Starting {num_workers} threads.")
    for _ in range(num_workers):
        t = threading.Thread(target=worker)
        t.start() # Initialize and start thread object 't'
        threads.append(t)

    # 4. Finish threads
    DOWNLOAD_QUEUE.join() # Wait for all tasks in the queue to be processed - signaled by task_done() in worker

    # Main thread waits for all threads to finish
    for t in threads:
        t.join()

    # 5. Logging / Summary
    manifest_to_prov()

    end_time = datetime.now()
    elapsed_sec = end_time - start_time

    total_batches = len(BATCH_MANIFEST)
    success_batches = len(BATCH_MANIFEST[BATCH_MANIFEST["downloaded"] == True])
    failed_batches = len(BATCH_MANIFEST[BATCH_MANIFEST["downloaded"] == False])
    files_downloaded = BATCH_MANIFEST.loc[BATCH_MANIFEST["downloaded"] == True, "file_count"].sum()

    print("\n===== BATCH DOWNLOAD SUMMARY =====")
    print(f"Total batches processed : {total_batches}")
    print(f"Total files downloaded  : {files_downloaded}")
    print(f"Successful batches      : {success_batches}")
    print(f"Failed batches          : {failed_batches}")
    print(f"Elapsed time            : {elapsed_sec/60} mins")

if __name__ == "__main__":
    main()
