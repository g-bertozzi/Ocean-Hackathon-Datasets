# Import SDKs
import calendar
import threading
import traceback
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
DATA_ROOT = PROJECT_ROOT / "data/plots"
METADATA_ROOT = PROJECT_ROOT / "metadata/plots"
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

# FUNCTIONS
def manifest_to_prov(output_path="prov.yaml"):
    """ Writes a provevnance to yaml according to the final BATCH_MANIFEST. """
    global BATCH_MANIFEST
    
    prov = {
        "challenge": "coastal-radar",
        "data-type": "plots",
        "base_url": "https://data.oceannetworks.ca/api",
        "api_calls": {}
    }

    for _, row in BATCH_MANIFEST.iterrows():  # loop properly over DataFrame rows
        batch = row.to_dict()
        cur_req_id = batch['dpRequestId']
        cur_run_id = batch['runIds']
        cur_batch_id = batch['batch_id']
        cur_start = pd.Timestamp(batch['start'], tz='UTC')
        cur_end = pd.Timestamp(batch['end'], tz='UTC')

        prov["api_calls"][cur_batch_id] = {

            "requestDataProduct": {
                "endpoint": "/dataProductDelivery/request", 
                "parameters": {
                    "filters": {
                        "locationCode": "SOGCS",
                        "deviceCategoryCode": "OCEANOGRAPHICRADAR",
                        "dataProductCode": "CODARQCSC",
                        "extension": "png",
                        "dpo_includeRadials": 0,
                        "dateFrom": cur_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                        "dateTo": cur_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    },
                },
                "Request ID": cur_req_id,
            },
            "runDataProduct": {
                "endpoint": "/dataProductDelivery/run", 
                "parameters": {
                    "Request ID": cur_req_id,
                    "waitComplete": True,
                },
                "Run IDs": cur_run_id,
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
    with open(PROVENANCE_PATH, "w") as f:
        yaml.dump(prov, f, default_flow_style=False, sort_keys=False)

def build_quartermonth_batches(year: int = 2023) -> pd.DataFrame:
    """
    Make quarter-of-month batches:
    Schema: [dpRequestId: int, runIds: int, batch_id: str, start: pd.Datetime, end:pd.Datetime, last_call_ran: str, call_status: str, attempt_count: int, downloaded: bool]
    """
    
    # set base parameters
    data_product_code = "CODARQCSC"
    device_category_code = "OCEANOGRAPHICRADAR"
    etx = "png"
    location_code = "SOGCS"

    rows = []
    for month in range(1, 2):
        # 1st–7th
        q1_start = pd.Timestamp(year=year, month=month, day=1, tz='UTC')
        q1_start_str = q1_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        q1_end = q1_start + timedelta(hours=12)
        #q1_end = pd.Timestamp(year=2024, month=8, day=7, hour=23, minute=59, second=59, tz='UTC')
        q1_end_str = q1_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        q1_params = {
            "dateFrom": q1_start_str,
            "dateTo": q1_end_str,
            "dataProductCode": data_product_code,
            "deviceCategoryCode": device_category_code,
            "locationCode": location_code,
            "dpo_includeRadials": 0,
            "extension": etx
        }

        q1_dp_id = make_dp_request(filters= q1_params) # Get dpRequestId
        
        rows.append({
            "dpRequestId": q1_dp_id,
            "runIds": None,
            "batch_id": f"{year}-{month:02d}-1Q",
            "start": q1_start,
            "end": q1_end,
            "last_call_ran": "requestDataProduct",
            "call_status": "complete",
            "attempt_count": 0,
            "downloaded": False
        })

        # 8th–15th
        q2_start = pd.Timestamp(year, month, day=8, tz='UTC')
        q2_start_str = q2_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        q2_end = q2_start + timedelta(hours=12)
        # q2_end = pd.Timestamp(year, month, day=9, hour=23, minute=59, second=59, tz='UTC')
        q2_end_str = q2_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        q2_params = {
            "dateFrom": q2_start_str,
            "dateTo": q2_end_str,
            "dataProductCode": data_product_code,
            "deviceCategoryCode": device_category_code,
            "locationCode": location_code,
            "dpo_includeRadials": 0,
            "extension": etx
        }

        q2_dp_id = make_dp_request(filters= q2_params) # Get dpRequestId

        rows.append({
            "dpRequestId": q2_dp_id,
            "runIds": None,
            "batch_id": f"{year}-{month:02d}-2Q",
            "start": q2_start,
            "end": q2_end,
            "last_call_ran": "requestDataProduct",
            "call_status": "complete",
            "attempt_count": 0,
            "downloaded": False
        })

        # 16th–23rd
        q3_start = pd.Timestamp(year, month, day=16, tz='UTC')
        q3_start_str = q3_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        q3_end = q3_start + timedelta(hours=12)
        # q3_end = pd.Timestamp(year, month, day=23, hour=23, minute=59, second=59, tz='UTC')
        q3_end_str = q3_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        q3_params = {
            "dateFrom": q3_start_str,
            "dateTo": q3_end_str,
            "dataProductCode": data_product_code,
            "deviceCategoryCode": device_category_code,
            "locationCode": location_code,
            "dpo_includeRadials": 0,
            "extension": etx
        }

        q3_dp_id = make_dp_request(filters= q3_params) # Get dpRequestId

        rows.append({
            "dpRequestId": q3_dp_id,
            "runIds": None,
            "batch_id": f"{year}-{month:02d}-3Q",
            "start": q3_start,
            "end": q3_end,
            "last_call_ran": "requestDataProduct",
            "call_status": "complete",
            "attempt_count": 0,
            "downloaded": False
        })

        # 24th → 1st of next month (exclusive)
        if month == 12:
            # For December, roll into Jan 1 of next year
            next_month_start = pd.Timestamp(year=year+1, month=1, day=1, tz='UTC')
        else:
            next_month_start = pd.Timestamp(year=year, month=month+1, day=1, tz='UTC')

        q4_start = pd.Timestamp(year, month, day=24, tz='UTC')
        q4_start_str = q4_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        q4_end = q4_start + timedelta(hours=12)
        # q4_end = next_month_start - timedelta(seconds=1)
        q4_end_str = q4_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        q4_params = {
            "dateFrom": q4_start_str,
            "dateTo": q4_end_str,
            "dataProductCode": data_product_code,
            "deviceCategoryCode": device_category_code,
            "locationCode": location_code,
            "dpo_includeRadials": 0,
            "extension": etx
        }

        q4_dp_id = make_dp_request(filters= q4_params) # Get dpRequestId

        rows.append({
            "dpRequestId": q4_dp_id,
            "runIds": None,
            "batch_id": f"{year}-{month:02d}-4Q",
            "start": q4_start,
            "end": q4_end,  # 23:59:59 of last day
            "last_call_ran": "requestDataProduct",
            "call_status": "complete",
            "attempt_count": 0,
            "downloaded": False
        })

    return pd.DataFrame(rows)

def load_or_init_batch_manifest(year: int = 2023) -> None:
    """ 
    Points global BATCH_MANIFEST to the exisiting csv file df or a new df. 
    Schema: [dpRequestId: int, runId: int or None, batch_id: str, start: pd.Datetime, end:pd.Datetime, last_call_ran: str, call_status: str, attempt_count: int, downloaded: bool]
    """
    global BATCH_MANIFEST

    if BATCH_MANIFEST_PATH.exists():
        BATCH_MANIFEST = pd.read_csv(BATCH_MANIFEST_PATH)
        BATCH_MANIFEST["attempt_count"] = 0 # Reset attempt_count to 0 for all batches
    else:
        BATCH_MANIFEST = build_quartermonth_batches(year)

def save_batch_manifest():
    """Write batch manifest to disk (call after finishing a batch)."""
    global BATCH_MANIFEST
    with batch_lock:
        BATCH_MANIFEST.to_csv(BATCH_MANIFEST_PATH, index=False)

def make_dp_request(filters: dict) -> int:

    response = PLOT_CLIENT.requestDataProduct(filters=filters)
    dpRequestId = response.get("dpRequestId")

    print(f"[requestDataProduct] dpRequestId: {dpRequestId}")

    return dpRequestId

# THREAD FUNCTIONS
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
    global BATCH_MANIFEST

    response = PLOT_CLIENT.runDataProduct(dpRequestId=dpRequestId, waitComplete=True)
    runIds = response.get('runIds')

    if not runIds:
        raise ValueError(f"Missing runIds in response: {response}")

    call_status = response.get("status") # Get status of runDataProduct -- waitComplete should wait for status == 'complete'

    with batch_lock: # update batch manifest last_call_ran
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dpRequestId, "runIds"] = runIds
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dpRequestId, "last_call_ran"] = "runDataProduct"
        BATCH_MANIFEST.loc[BATCH_MANIFEST["dpRequestId"] == dpRequestId, "call_status"] = call_status
    
    save_batch_manifest()
    
    return runIds

def download_dp(runIds: int, dpRequestId: int):
    """ Takes dpRequestId from requestDataProduct and runIds from runDataProduct and returns ------. """
    global BATCH_MANIFEST

    try:
        response = PLOT_CLIENT.downloadDataProduct(runId=runIds, 
                                                maxRetries=0, 
                                                downloadResultsOnly=False, 
                                                includeMetadataFile=False, 
                                                overwrite=False)

        # Poll until cart closes
        while cart_complete(id=dpRequestId) is False:
            time.sleep(3)
            print(f"[download_dp] requestId {dpRequestId} downloading in progess.")

        with batch_lock:
            # update batch manifest last_call_ran
            BATCH_MANIFEST.loc[BATCH_MANIFEST["runIds"] == runIds, "last_call_ran"] = "downloadDataProduct"
            BATCH_MANIFEST.loc[BATCH_MANIFEST["runIds"] == runIds, "call_status"] = "complete"
            BATCH_MANIFEST.loc[BATCH_MANIFEST["runIds"] == runIds, "downloaded"] = True
        
        save_batch_manifest()

        return True

    except Exception as e:
        print(f"[download_dp] error: {e}")
        return False


def worker():
    """
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
                with log_lock:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[worker] trying to get runIds for requestId {req_id} at {now}.")

                run_ids = run_dp(dpRequestId=req_id) # -> runIds: int OR list[int]
                
                with log_lock:
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[worker] got runIds {run_ids} at {now}.")

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
            print(f"[worker] after running ")
            print(BATCH_MANIFEST)

        # 3. Download data product 
        success = False
        for i, run_id in enumerate(run_ids):
            for attempt in range(MAX_RETRIES):
                try:
                    with log_lock:
                        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        print(f"[worker] trying to download for requestId {req_id}[{i}] runIds {run_id} at {now}.")
                    download_dp(runIds=run_id, dpRequestId=req_id)
                    success = True
                    
                    with log_lock:
                        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        print(f"[worker] downloaded runIds {run_id} at {now}.")

                    break
                except Exception as e:
                    print(f"[worker] download failed (attempt {attempt+1}): {e}")
                    traceback.print_exc()
                    time.sleep(5)
            if not success:
                print(f"[worker] download failed permanently for {req_id}")
     
        DOWNLOAD_QUEUE.task_done()
  
    with log_lock:
            print(f"[worker] after downloading ")
            print(BATCH_MANIFEST)


        # RETRY logic? for run and download or seperately?




def main():
    # A. Initial Logging
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # start timer
    print(f"[main] Starting at {start_time}.")

    global BATCH_MANIFEST, DOWNLOAD_QUEUE

    # 1. Init batch and file manifest
    load_or_init_batch_manifest(year=2023) # points BATCH_MANIFEST
    print(BATCH_MANIFEST)

    # 2. Build download queue 
    for _, row in BATCH_MANIFEST.iterrows():
        if row["downloaded"] != True: # Enqueue all batches not yet downloaded
            DOWNLOAD_QUEUE.put(row.to_dict())
            print(f"[main] queueing dpRequestId: {row['dpRequestId']}, start: {row['start']}, end: {row['end']}")
    

    # 3. Start threads
    num_workers = 4
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

    """
    Flow:


    1. load or init batch manifest
        - if csv exists: load into BATCH_MANIFEST global dataframe
        - else build 1/4 month batches df and point BATCH_MANIFEST global dataframe
    

    2. iterate through BATCH_MANIFEST and append row to queue if not downloaded 

    3. workers
        - pull batch from queue
        - runDataProduct(dpRequestId) -> runIds saved to batch manifest

        - downloadDataProdct()
        - orderDataProduct
            - {params, maxRetries = 3, downloadResultsOnly = False, includeMetadataFile = False, overwrite = False}

        - at end of each batch:
            
            - retry logic for failed downloads?
                - if ANY file fails we would have to requeue the entire batch
                    - would successful ones just be over written?

            - batch records:
                - mark batch as done in batch manifest
                - write batch manifest to csv

    """

if __name__ == "__main__":
    main()
