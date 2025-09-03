"""
Coastal Radar Challenge

- 365 x 24 plots of surface current direction and magnitude in the Strait of Georgia. These currents are captured by an array of 4 shore-based coastal radars. 
    - -> ~2.5 GB size
    - dataProduct: 
    
- Underlying u and v data vectors
    - 2MB for 2 days -> ~ 0.365 GB size

- Wind data from a buoy and/or coastal meteorological stations 

- River flow data: Fraser River discharge data is available for the full year of 2024 (This is NOT ONC data, you have to search elsewhere)
- Possibly: Acoustic Doppler Current Profiler (ADCP) data: We could also incorporate ADCP current measurements from the Strait of Georgia, depending on data completeness. (No need to gather this now.)
"""
# SDKs
import os
from dotenv import load_dotenv, find_dotenv
import onc
from onc import ONC
import urllib
import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import UTC, datetime, timezone, timedelta
from dateutil import rrule
import yaml

load_dotenv(find_dotenv())

# --- Project-root based paths ---
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data" 
METADATA_ROOT = PROJECT_ROOT / "metadata"

PLOT_ROOT = DATA_ROOT / "plots"
VECTOR_ROOT = DATA_ROOT / "vectors"

# Make sure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# --- ONC API client setup ---
TOKEN = os.getenv("ONC_TOKEN")
MY_ONC = onc.ONC(TOKEN, outPath=str(Path(DATA_ROOT)))

PLOT_CLIENT = onc.ONC(TOKEN, outPath=str(PLOT_ROOT))
VECTOR_CLIENT = onc.ONC(TOKEN, outPath=str(VECTOR_ROOT))

# Global variables
API_CALL_N = 0 # Count of API calls made

PROV_INFO = {
    "challenge": "coastal-radar",
    "description": "1 year of hourly CODAR gridded plots and underlying u and v vectors from the Strait of Georgia CODAR array",
    "api": {
        # "call_1": {
        #     "endpoint": "",
        #     "parameters": {}, # locationCode, deviceCategoryCode, fileExtension, dateFrom, dateTo
        #     "queryUrl": "",
        #     "citation": "",
        #     "doi": "",
        # }
        # "call_2": { ...
    },
    "manifest": {
        "manifest_path": "",
        "manefist_schema": [], # ['timestamp', 'locationCode', 'filename', 'path']
        "last_updated": "" # Now
    }
}

# Functions


def fetch_month_plots(locationCode: str, dateFrom: str, dateTo: str, dataProductCode: str =  "CODARQCSC", deviceCategoryCode: str = "OCEANOGRAPHICRADAR"):
    """ 
    #  downloadResultsOnly: bool = False,
    #  extension: str = "png", 

    Breaking into
    1. request
    - returns: ['citations', 'disclaimer', 'dpRequestId', 'estimatedFileSize', 'estimatedProcessingTime', 'messages', 'queryPids', 'queryURL'])

    2. run
    - returns: {'runIds': [52976615], 'fileCount': 24, 'runTime': 152.1184630393982, 'requestCount': 26}

    3. download
    - update api calls & prov

    """

    # 1. Request
    params = {
        "dataProductCode": dataProductCode,
        "locationCode": locationCode,
        "deviceCategoryCode": deviceCategoryCode,
        "dateFrom": dateFrom,
        "dateTo": dateTo,
        "extension": "png",
        "dpo_includeRadials": 0,  # don't included radials

    }

    req = PLOT_CLIENT.requestDataProduct(params)
    reqID = req.get("dpRequestId")

    print(f"[request] dpRequestId={reqID}, est. size={req['estimatedFileSize']}") # NOTE: debugging

    # 2. Run 
    run = PLOT_CLIENT.runDataProduct(dpRequestId= reqID)

    file_count = run.get("fileCount", 0)
    print(f"[run] generated {file_count} files")



    # --- 3. Download (skip if exists) ---
    saved_files = []
    for i in range(file_count):
        # Ask ONC what the filename will be
        fname = PLOT_CLIENT.getDataProductFilename(dpRequestId=reqID, index=i)

        dest = PLOT_ROOT / fname
        if dest.exists():
            print(f"[skip] already have {fname}")
            continue

        # Download only if missing
        dl_name = PLOT_CLIENT.downloadDataProduct(dpRequestId=reqID, index=i)
        saved_files.append(dl_name)
        print(f"[downloaded] {dl_name}")

    return saved_files





def fetch_year_plots(locationCode: str, dateFrom: str, dateTo: str):
    """
    """
    # Convert string dates to datetime objects
    start_time = datetime.fromisoformat(dateFrom.replace("Z", "+00:00"))
    end_time = datetime.fromisoformat(dateTo.replace("Z", "+00:00"))

    # NOTE: TEST
    fetch_month_plots(locationCode = locationCode, dateFrom = dateFrom, dateTo = dateTo)

    # Iterate through by periods of a month
    for i, dt in enumerate(rrule.rrule(rrule.MONTHLY, dtstart = start_time, until = end_time)):
        if i != 0:
            # Pass dates as a strings for the API call
            fetch_month_plots(locationCode = locationCode, dateFrom = prev.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z", dateTo = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")
        prev = dt

    return 

def fetch_vectors():
    """
    """



def main():

    start = "2023-05-01T00:00:00.000Z"
    end = "2023-05-02T00:00:00.000Z"
    # # Fetch plots
    fetch_year_plots(locationCode = "SOGCS", dateFrom = start, dateTo = end)

if __name__ == "__main__":
    main()
