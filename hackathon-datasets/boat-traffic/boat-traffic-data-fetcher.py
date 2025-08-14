"""
Boat Traffic Challenge Datasets

1 year of still image from each location:
 * A collection of still images taken every 5 minutes by a video camera positioned onshore near China Creek, Alberni Inlet.
 * A collection of still images taken every 5 minutes by a video camera positioned onshore near Cape Mudge in Discovery Passage, Campbell River. 
 * Possible addition: hydrophone recordings from underwater offshore from the Alberni Inlet location.
"""

import os, sys, argparse, hashlib, tempfile, shutil
from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import onc
from onc import ONC

load_dotenv()

# API connection
token = os.getenv("ONC_TOKEN")
my_onc = onc.ONC(token)

# Global variables
china_location = "CCSS"
mudge_location = "CRSS"

challenge_info = {
    "boat-traffic": [china_location, mudge_location],

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

    response = my_onc.getArchivefileByLocation(params) # Make request 
    files = response.get('files', []) # Isolate list of files
    n = len(files) # Number of files

    # NOTE: debugging output
    print(f"file 1: {files[0] if files else 'No files found.'}")
    print(f"file {n}: {files[-1] if files else 'No files found.'}")
    print(f"num files: {n}")

    return files

def get_yr_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list:
    """
    Returns a list of filenames for still images from the video camera at the specified location for a full year.

    """
    from datetime import datetime, timedelta

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

def main():


    # get filenames
    china_files = get_yr_filenames(locationCode = china_location, dateFrom = "2023-09-01T00:00:00.000Z", dateTo = "2024-09-01T00:00:00.000Z")

    # get files

    # store files?

if __name__ == "__main__":
    main()