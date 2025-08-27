# SDKs
import threading
import yaml
import urllib
import os, shutil
from pathlib import Path
from datetime import UTC, datetime, timedelta
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

# --- Manifest Management ---
manifest_lock = threading.Lock()
manifest_df = None

def load_or_init_manifest():
    global manifest_df
    if MANIFEST_PATH.exists():
        manifest_df = pd.read_csv(MANIFEST_PATH, parse_dates=["timestamp"])
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

    params = {
    'locationCode': locationCode,
    'dateFrom': dateFrom,
    'dateTo': dateTo,
    'deviceCategoryCode': "OCEANOGRAPHICRADAR",
    'dataProductCode': "CODARCD",
    'fileExtension': ".tuv"
    }

    response = VECTOR_CLIENT.getArchivefileByLocation(params) # Make request 
    files = response.get('files', []) # Isolate list of files
    n = len(files) # Number of files

    print(f"{locationCode} YEAR TOTAL: {len(files)} files")

    return files


def main():
    """"""
    # 1. Get list of filenames

    yr_start = "2023-01-01T00:00:00.000Z"
    yr_end = "2023-01-03T00:00:00.000Z"

    files = get_filenames(locationCode=SOG_LOCATION, dateFrom=yr_start, dateTo=yr_end)


    # 2. Build manifest
    manifest_df = load_manifest() # Either from memory if it exists else empty df 


    # 4. Queue only missing or failed files
    for f in files:
        fname = f["fileName"]
        if not ((manifest_df["filename"] == fname) & (manifest_df["status"] == "success")).any():
            DOWNLOAD_QUEUE.put(f)

    print(f"📥 {DOWNLOAD_QUEUE.qsize()} files queued for download")


if __name__ == "__main__":
    main()