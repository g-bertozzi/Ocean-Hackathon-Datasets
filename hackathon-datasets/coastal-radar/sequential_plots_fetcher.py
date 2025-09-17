"""
Coastal Radar Challenge

- 365 x 24 plots of surface current direction and magnitude in the Strait of Georgia. These currents are captured by an array of 4 shore-based coastal radars. 
    - -> ~2.5 GB size
    - dataProduct: 
    
- Underlying u and v data vectors
    - 2MB for 2 days -> ~ 0.365 GB size
    - ALREADY IN ARCHIVE FOR YEAR OF 2023

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

PLOT_ROOT = DATA_ROOT / "seq-plots"


# Make sure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# --- ONC API client setup ---
TOKEN = os.getenv("ONC_TOKEN")
MY_ONC = onc.ONC(TOKEN, outPath=str(Path(DATA_ROOT)))

PLOT_CLIENT = onc.ONC(TOKEN, outPath=str(PLOT_ROOT))


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
        #     "citation": "", # Prob empty
        #     "doi": "", # Prob empty
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

    response = PLOT_CLIENT.orderDataProduct(params, downloadResultsOnly = False, overwrite= False, includeMetadataFile= False) # downloadResultsOnly = True returns only the metadata

    # # Isolate info for provenance - NOTE: done for each API call
    # query_url = response.get('queryUrl')
    # citations_info = response.get('citations')[0]
    # url_parsed = urllib.parse.urlparse(query_url) # Parse the URL and break into components

    # params_minus_token = params.copy()
    # del params_minus_token['token']
    
    # # Update global PROV_INFO with API call details
    # global API_CALL_N, PROV_INFO

    # PROV_INFO["api"][f"call_{API_CALL_N + 1}"] = {
    # "endpoint": url_parsed.path.replace("/api", "", 1),
    # "parameters": params_minus_token,
    # "queryUrl": query_url,
    # "citation": citations_info['citation'],
    # "doi": citations_info['doi'],
    # }

    global API_CALL_N
    API_CALL_N += 1 # Update global API call count

    print(f"[monthly plot] {response}")

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
    fetch_month_codar(locationCode = locationCode, extension = extension, downloadResultsOnly = downloadResultsOnly, dateFrom = dateFrom, dateTo = dateTo)

    # # Iterate through by periods of a month
    # for i, dt in enumerate(rrule.rrule(rrule.MONTHLY, dtstart = start_time, until = end_time)):
    #     if i != 0:
    #         # Pass dates as a strings for the API call
    #         fetch_month_codar(locationCode = locationCode, dateFrom = prev.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z", dateTo = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")
    #     prev = dt

    return 


def clean_and_manifest(data_type: str) -> pd.DataFrame:
    """
    Moves *_META.xml files into metadata/<data_type>/ and builds a manifest
    for the remaining data files in data/<data_type>/.

    Returns:
        Pandas DataFrame manifest with [timestamp, filename, path, datatype].
    """
    src_dir = DATA_ROOT / data_type
    meta_dir = METADATA_ROOT / data_type
    meta_dir.mkdir(parents=True, exist_ok=True)

    manifest_list = []

    # Iterate through directory
    for f in src_dir.iterdir():

        # Move metadata file into metadata/<data_type>
        if f.suffix.lower() == ".xml" and f.name.endswith("_META.xml"):
            target = meta_dir / f.name
            f.replace(target)
            print(f"[moved meta] {f.name} → {target}")
            continue

        # Add real data files (png, nc, etc.) to manifest
        if f.suffix.lower() in [".png", ".nc"]:
            ts = None
            try:
                # Take the first timestamp-looking chunk after the system name
                parts = f.name.split("_")
                for p in parts:
                    if p.endswith("Z"):   # timestamp candidate
                        ts = pd.to_datetime(p, utc=True)
                        break
                print(f"fname: {f}")
                print(f"timestamp: {ts}")

            except Exception:
                pass

            manifest_list.append({
                "timestamp": ts,
                "filename": f.name,
                "path": str(f.relative_to(DATA_ROOT)),  # relative path under data root
                "datatype": data_type
            })

    df = pd.DataFrame(manifest_list)

    # Save manifest for this data type
    manifest_path = METADATA_ROOT / f"manifest_{data_type}.csv"
    df.to_csv(manifest_path, index=False)
    print(f"[saved manifest] {manifest_path}")

    # Update provenance dict
    global PROV_INFO
    PROV_INFO["manifest"] = {
        "manifest_path": str(METADATA_ROOT),
        "manifest_schema": ['timestamp', 'locationCode', 'filename', 'path'],
        "last_updated": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    }

    return df

def make_prov() -> None:
    """
    Save provenance info dictionary to a YAML file.

    Inputs:
    Output:
    """
    output_path = Path(METADATA_ROOT) / "provenance.yaml"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        yaml.dump(PROV_INFO, f, sort_keys=False, default_flow_style=False)
    print(f"[saved provenance] {output_path}") # NOTE: debug output
    

def main():

    start = "2023-05-01T00:00:00.000Z"
    end = "2023-05-01T10:00:00.000Z"

    # # Fetch plots
    fetch_yr_codar(locationCode = "SOGCS", extension = "png", downloadResultsOnly = False, dateFrom = start, dateTo = end)


    # plots_manifest = clean_and_manifest("plot")


    # make_prov()



if __name__ == "__main__":
    main()
