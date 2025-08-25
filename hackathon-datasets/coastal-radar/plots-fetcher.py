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
import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dateutil import rrule

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

# Functions

def fetch_month_codar(locationCode: str, 
                         extension: str, 
                         downloadResultsOnly: bool,
                         dateFrom: str, 
                         dateTo: str, 
                         dataProductCode: str =  "CODARQCSC", 
                         deviceCategoryCode: str = "OCEANOGRAPHICRADAR", 
                         dpo_includeRadials: int = 0) -> None:
    
    """
    Fetches 1 month of CODAR plots for the given location.

    Inputs:
    Output:
    """

    params = {
        "dataProductCode": dataProductCode,
        "locationCode": locationCode,
        "deviceCategoryCode": deviceCategoryCode,
        "dateFrom": dateFrom,
        "dateTo": dateTo,
        "extension": extension,
        "dpo_includeRadials": dpo_includeRadials,  # don't included radials

    }

    print(f"Requesting {dataProductCode} data for {locationCode} from {dateFrom} to {dateTo}")

    response = PLOT_CLIENT.orderDataProduct(params, downloadResultsOnly = downloadResultsOnly) # downloadResultsOnly = True returns only the metadata

    print(response)

def fetch_yr_codar(locationCode: str, extension: str, dateFrom: str, dateTo: str, downloadResultsOnly: bool = False) -> None:
    """
    Fetches the CODAR data product for a given location and year.
    - current use: png plots , 1 per hour
    
    Inputs:
        locationCode (str): The location code for the CODAR data.
        dataProductCode (str): The data product code for the CODAR data.
        dateFrom (str): Start date in ISO 8601 format.
        dateTo (str): End date in ISO 8601 format.
    
    Output:
        
    """
    # Convert string dates to datetime objects
    start_time = datetime.fromisoformat(dateFrom.replace("Z", "+00:00"))
    end_time = datetime.fromisoformat(dateTo.replace("Z", "+00:00"))

    # NOTE: TEST
    # fetch_month_codar(locationCode = locationCode, extension = extension, downloadResultsOnly = downloadResultsOnly, dateFrom = dateFrom, dateTo = dateTo)

    # Iterate through by periods of a month
    for i, dt in enumerate(rrule.rrule(rrule.MONTHLY, dtstart = start_time, until = end_time)):
        if i != 0:
            # Pass dates as a strings for the API call
            fetch_month_codar(locationCode = locationCode, dateFrom = prev.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z", dateTo = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")
        prev = dt

    return 

def fetch_yr_vectors(locationCode: str, 
                    downloadResultsOnly: bool, 
                    dateFrom: str, 
                    dateTo: str,
                    extension: str = "nc", 
                    dataProductCode: str =  "CODARQCSC", 
                    deviceCategoryCode: str = "OCEANOGRAPHICRADAR", 
                    dpo_includeRadials: int = 0) -> None:
    
    """
    Fetches all nc files for the year - probably 1 per month
    
    """
    
    # Convert string dates to datetime objects
    start_time = datetime.fromisoformat(dateFrom.replace("Z", "+00:00"))
    end_time = datetime.fromisoformat(dateTo.replace("Z", "+00:00"))

    params = {
        "dataProductCode": dataProductCode,
        "locationCode": locationCode,
        "deviceCategoryCode": deviceCategoryCode,
        "dateFrom": dateFrom,
        "dateTo": dateTo,
        "extension": extension,
        "dpo_includeRadials": dpo_includeRadials,  # don't included radials

    }

    print(f"Requesting {dataProductCode} data for {locationCode} from {dateFrom} to {dateTo}")

    response = VECTOR_CLIENT.orderDataProduct(params, downloadResultsOnly = downloadResultsOnly) # downloadResultsOnly = True returns only the metadata

    # print(response)

    return

def clean_and_manifest(data_type: str) -> pd.DataFrame:
    """"""

def main():
    # Fetch plots
    fetch_yr_codar(locationCode = "SOGCS", extension = "png", downloadResultsOnly = False, dateFrom = "2023-05-01T00:00:00.000Z", dateTo = "2023-05-02T00:00:00.000Z")

    # Fetch u and v vectors
    fetch_yr_vectors(locationCode = "SOGCS", extension = "nc", downloadResultsOnly = False, dateFrom = "2023-05-01T00:00:00.000Z", dateTo = "2023-05-02T00:00:00.000Z")



if __name__ == "__main__":
    main()
