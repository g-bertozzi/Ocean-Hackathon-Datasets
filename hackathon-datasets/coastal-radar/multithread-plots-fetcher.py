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
PROJECT_ROOT = Path(__file__).resolve().parent # NOTE: for local testing
# PROJECT_ROOT = Path("/Volumes/Ocean-Hackathon/coastal-radar") # NOTE: for shared drive download
DATA_ROOT = PROJECT_ROOT / "data/plots"
METADATA_ROOT = PROJECT_ROOT / "metadata/plots"
BATCH_MANIFEST_PATH = METADATA_ROOT / "batch-manifest.csv"
FILES_MANIFEST_PATH = METADATA_ROOT / "files-manifest.csv"
PROVENANCE_PATH = METADATA_ROOT / "provenance.yaml"

# Make sure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# ONC client
TOKEN = os.getenv("ONC_TOKEN") # NOTE: Change this to your ONC token
PLOT_CLIENT = onc.ONC(TOKEN, outPath=str(DATA_ROOT))


# --- Globals ---

from typing import Optional
import pandas as pd

BATCH_MANIFEST: Optional[pd.DataFrame] = None
FILES_MANIFEST: Optional[pd.DataFrame] = None

batch_lock = threading.Lock()
files_lock = threading.Lock()

DOWNLOAD_QUEUE = queue.Queue() # Queue for files to download

""" 
LOGIC


Metadata logging:

1. batch manifest (coarse)
cloumns: batch_id, start, end, status, requested_at, completed_at,

[start,end]: always 1/2 month -> 1st to 15th, 15th to 1st
[batch_id]: determinstic -> '2023-01-1H' for first half, '2023-01-2H' for second half

- Update status column as we complete each batch
- Immediately write the dataframe batch manifest to csv?

- RETRY: any batch with status != 'done' gets requeued

2. file manifest (fine)
columns: batch_id, timestamp, locationCode, deviceCategoryCode, deviceCode, filename, path, status

- Append rows as you download files
- Write the dataframe file manifest to csv WHEN

3. period progress logging to console

Flow:

1. load/init both manifests

2. workers
    - pick batch from queue
    - orderDataProduct
    - for each file
        - extract meta for file manifest
        - write row to file manifest

    - for each batch
        - mark batch as done in batch manifest
        - write batch manifest to csv
        - write file manifest to csv


1. load csv / init manifest to dataframe
2. break year into 1/2 month sections

3. somehow check if these time batches overlap or don't overlap with the manifest df

4. append the time frames that either weren't successful or werent in the manifest- same strat at vectors- status column with nan or failed
"""




# --- Batch manifest helpers ---

def build_halfmonth_batches(year: int = 2023) -> pd.DataFrame:
    """
    Make 24 half-month batches for a single year.

    ex.
          batch_id      start                 end   status requested_at completed_at
    0   2023-01-1H 2023-01-01 2023-01-15 23:59:59  pending         None         None
    1   2023-01-2H 2023-01-16 2023-01-31 23:59:59  pending         None         None
    2   2023-02-1H 2023-02-01 2023-02-15 23:59:59  pending         None         None
    ...
    """
    rows = []
    for month in range(1, 13):
        # first half
        start_1 = datetime(year, month, 1)
        end_1 = datetime(year, month, 15, 23, 59, 59)
        rows.append({
            "batch_id": f"{year}-{month:02d}-1H",
            "start": start_1,
            "end": end_1,
            "status": "pending",
            "requested_at": None,
            "completed_at": None,
        })

        # second half
        last_day = calendar.monthrange(year, month)[1]
        start_2 = datetime(year, month, 16)
        end_2 = datetime(year, month, last_day, 23, 59, 59)
        rows.append({
            "batch_id": f"{year}-{month:02d}-2H",
            "start": start_2,
            "end": end_2,
            "status": "pending",
            "requested_at": None,
            "completed_at": None,
        })

    return pd.DataFrame(rows)

def load_or_init_batch_manifest(year: int) -> None:
    """ Points global BATCH_MANIFEST to the exisiting csv file df or a new df. """
    global BATCH_MANIFEST
    if BATCH_MANIFEST_PATH.exists():
        BATCH_MANIFEST = pd.read_csv(BATCH_MANIFEST_PATH, parse_dates=["requested_at", "completed_at"])
    else:
        BATCH_MANIFEST = build_halfmonth_batches(year)

def load_or_init_file_manifest() -> None:
    """ Points global FILES_MANIFEST to the exisiting csv file df or a new df. """
    global FILES_MANIFEST
    if FILES_MANIFEST_PATH.exists():
        FILES_MANIFEST = pd.read_csv(FILES_MANIFEST_PATH)
    else:
        FILES_MANIFEST = pd.DataFrame(columns=["batch_id", "timestamp", "locationCode",
                                   "deviceCategoryCode", "deviceCode",
                                   "filename", "path", "status"])
        

def append_file_rows(rows: list[dict]):
    """Append new file rows in memory only (no disk write)."""
    global FILES_MANIFEST
    df_new = pd.DataFrame(rows)
    with files_lock:
        FILES_MANIFEST = pd.concat([FILES_MANIFEST, df_new], ignore_index=True)


def save_file_manifest():
    """Flush file manifest to disk (call after finishing a batch)."""
    global FILES_MANIFEST
    with files_lock:
        FILES_MANIFEST.to_csv(FILES_MANIFEST_PATH, index=False)


def save_batch_manifest():
    """Flush batch manifest to disk (call after finishing a batch)."""
    global BATCH_MANIFEST
    with batch_lock:
        BATCH_MANIFEST.to_csv(BATCH_MANIFEST_PATH, index=False)

def worker():
    """
    """
    

def main():
    """
    """
    # 1. Init batch and file manifest
    load_or_init_batch_manifest(year=2023)
    load_or_init_file_manifest()


    # 2. Build queue
    """ append all batches where status != 'done"""
    for _, row in BATCH_MANIFEST.iterrows():
        if row["status"] != "done":
            DOWNLOAD_QUEUE.put(row) # append row
            print(f"[Queueing] batch ID: {row['batch_id']}, start: {row['start']}, end: {row['end']}")



    # 5. Worker Threads
    num_workers = 10
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



    




if __name__ == "__main__":
    main()
