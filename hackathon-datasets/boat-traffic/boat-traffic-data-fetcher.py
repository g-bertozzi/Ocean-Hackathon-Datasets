"""
Boat Traffic Challenge Datasets

1 year of still image from each location:
 * A collection of still images taken every 5 minutes by a video camera positioned onshore near China Creek, Alberni Inlet.
 * A collection of still images taken every 5 minutes by a video camera positioned onshore near Cape Mudge in Discovery Passage, Campbell River. 
 * Possible addition: hydrophone recordings from underwater offshore from the Alberni Inlet location.
"""

import os, sys, argparse, hashlib, tempfile, shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
import requests
import time   
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import onc
from onc import ONC

load_dotenv()

# API connection
TOKEN = os.getenv("ONC_TOKEN")
MY_ONC = onc.ONC(TOKEN)

# Global variables
CHINA_LOCATION = "CCSS"
MUDGE_LOCATION = "CRSS"

challenge_info = {
    "boat-traffic": [CHINA_LOCATION , MUDGE_LOCATION],

}

def calculate_difference(date1: str, date2: str) -> int:
    """
    Calculates the difference in days between two dates in ISO 8601 format.

    Preconditions:
    - date1 and date2 are strings in ISO 8601 format (e.g., "2023-09-01T00:00:00.000Z")
    - date1 is before date2

    """
    # Convert string inputs defined in the params above to datetime objects
    start_time = datetime.fromisoformat(date1.replace("Z", "+00:00"))
    end_time = datetime.fromisoformat(date2.replace("Z", "+00:00"))

    # Calculate the duration in hours (there are 3600 seconds in an hour)
    return (end_time - start_time).days



def get_6_month_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list:
    """
    Returns a list of filenames for still images from the video camera at the specified location within a 6 month period.
    """

    params = {
    'locationCode': locationCode,
    'dateFrom': dateFrom,
    'dateTo': dateTo,
    'deviceCategoryCode': "VIDEOCAM",
    'fileExtension': "jpg", # 'jpg' for still images
    # 'rowLimit': 18, # max 18 
    }

    response = MY_ONC.getArchivefileByLocation(params) # Make request 
    files = response.get('files', []) # Isolate list of files
    n = len(files) # Number of files

    # NOTE: debugging output
    print("6 months of data")
    print(f"file 1: {files[0] if files else 'No files found.'}")
    print(f"file {n}: {files[-1] if files else 'No files found.'}")
    print(f"num files: {n}")

    return files

def get_yr_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list:
    """
    Returns a list of filenames for still images from the video camera at the specified location for a full year.
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
    print(f"TOTAL for year: {len(all_files)}")

    return all_files

def filenames_to_manifest(filenames: list[str], locationCode: str, challenge: str, metadata_root: str = "./metadata") -> pd.DataFrame:
    """
    Converts a list of filenames to a manifest DataFrame with additional metadata.
    
    Inputs:

    Output: Pandas DataFrame with columns: 
    - 'timestamp': the date extracted from the filename
    - 'locationCode': the location code of the file
    - 'filename': the name of the file
    - 'path': planned local download path for the file

    Goal: 

    boat-traffic/
        data/
            CCSS/                 # all CCSS images in one folder (1 year max)
            <ONC_filename>.jpg
            ...
            CRSS/
            <ONC_filename>.jpg
            ...
        metadata/
            manifest.csv          # 4 columns: timestamp, locationCode, filename, path

    """
   # Create list of dictionaries: list is dataframe, each dict is a row
    manifest_list = []
    
    for fname in filenames:
        # Extract time info: YYYYMMDDHHMMSS.mmmZ
        ts_str = fname.split("_")[1].replace(".jpg", "") # Extract timestamp between the first underscore and the file extension
        ts = pd.to_datetime(ts_str, utc = True)  # Convert to datetime object in UTC

        path = Path(locationCode) / fname # Planned local path

        manifest_list.append({
            "timestamp": ts,
            "locationCode": locationCode,
            "filename": fname,
            "path": str(path)
        })
    
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(manifest_list)

    # Save to CSV
    metadata_path = Path(metadata_root) / "manifest.csv" # Build metadata path
    os.makedirs(metadata_path.parent, exist_ok=True) # Ensure the parent directory exists
    df.to_csv(metadata_path, index=False) # Save CSV

    return df


def download_from_manifest(manifest_df: pd.DataFrame, data_root: str = "./data", subsample: int | None = None, per_file_sleep: float = 0.05, retries: int = 3) -> None:
    """
    Download files listed in `manifest_df` (must have ['filename','path'] columns).

    subsample:
      - If given, only download the first `subsample` rows (keeps time order).
    """
    df = manifest_df.head(subsample) if subsample else manifest_df

    ok = skipped = failed = 0
    
    # Iterate through the manifest DataFrame and download each file
    for _, row in df.iterrows():
        fname = row["filename"]
        dest = Path (data_root) / row["path"]

        # Skip if already present
        if dest.exists():
            skipped += 1
            continue

        # Ensure destination directory exists
        dest.parent.mkdir(parents=True, exist_ok=True)

        try:
            # 1) Download to ./output/<filename>
            MY_ONC.getFile(fname)  # <-- client call

            # 2) Move into place
            src = Path("output") / fname # Automatically goes to 'output' folder 
            if not src.exists():
                print(f"[ERROR] after download, missing: {src}")
                failed += 1
                continue

            shutil.move(str(src), str(dest)) # Move the file to the correct location
            ok += 1

            # if per_file_sleep > 0: 
            #     time.sleep(per_file_sleep)

        except Exception as e:
            print(f"[ERROR] {fname}: {e}")
            failed += 1

        # Optional periodic progress
        total = ok + skipped + failed
        if total % 100 == 0:
            print(f"[progress] ok={ok} skipped={skipped} failed={failed}")

    # Delte temporary 'output' directory if it exists
    if src.exists():
        shutil.rmtree(src)

    summary = {"ok": ok, "skipped": skipped, "failed": failed}
    print(f"[done] {summary}")
    
    return summary


def main():
    # get filenames
    china_files = get_yr_filenames(locationCode = CHINA_LOCATION, dateFrom = "2023-09-01T00:00:00.000Z", dateTo = "2024-09-01T00:00:00.000Z")

    # subsampled test files
    small_test_files = china_files[:5]

    import math
    step = math.ceil(len(china_files) / 200)  # 200 evenly spaced samples
    medium_test_files = china_files[::step]


    # build directory structure?
    manifest_df = filenames_to_manifest(filenames = medium_test_files, locationCode = CHINA_LOCATION, challenge = "boat-traffic")

    # store files?
    download_from_manifest(manifest_df = manifest_df, subsample = 10)

if __name__ == "__main__":
    main()