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

# --- Manifest Management ---
manifest_lock = threading.Lock()
manifest_df = None

def load_or_init_manifest():
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
    return manifest_df

def update_manifest_df(file_info, success: bool):
    """Update in-memory manifest DataFrame with download results"""
    global manifest_df
    row = {
        "timestamp": file_info.get("dataDate"),
        "locationCode": file_info.get("locationCode"),
        "deviceCategoryCode": file_info.get("deviceCategoryCode"),
        "deviceCode": file_info.get("deviceCode"),
        "filename": file_info.get("filename"),
        "path": str(DATA_ROOT / file_info.get("filename")),
        "status": "success" if success else "failed"
    }
    # Drop any existing row for the same filename
    manifest_df = manifest_df[manifest_df["filename"] != row["filename"]]
    # Append new row
    manifest_df = pd.concat([manifest_df, pd.DataFrame([row])], ignore_index=True)

def get_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list[str]:

    # Get list of filenames via getArchivefileByLocation
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

    # Parse filenames to extract metadata and build rows
    print(f"Creating information table for {locationCode}.")

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


def main():
    """"""
    # 1. Get list of filenames
    yr_start = "2023-01-01T00:00:00.000Z"
    yr_end = "2023-01-03T00:00:00.000Z"

    filenames = get_filenames(locationCode=SOG_LOCATION, dateFrom=yr_start, dateTo=yr_end) # List of filenames from ONC
    file_info = filenames_to_file_info(filenames=filenames, locationCode=SOG_LOCATION) # DataFrame with metadata parsed from filenames


    # 2. Build manifest
    prev_manifest = load_or_init_manifest() # Either from memory if it exists else empty df 


    # 3. Compare list of filenames and the new/loaded manifest
    #      - natural join these (new files will get a NaN in 'status' column)
    #      - queue file or skip it depending on 'status' column (either NaN or 'failed' means queue it)

    merged_manifest = file_info.merge(prev_manifest[['filename', 'status']], on='filename', how='left')


    # 4. Queue only missing or failed files
    for _, row in merged_manifest.iterrows():
        if pd.isna(row['status']) or row['status'] == 'failed': # If nan or failed
            DOWNLOAD_QUEUE.put(row) # Then enqueue NOTE: THREAD SAFE

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



if __name__ == "__main__":
    main()