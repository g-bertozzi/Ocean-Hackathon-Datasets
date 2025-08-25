"""
Boat Traffic Challenge Datasets

1 year of still image from each location:
 * A collection of still images taken every 5 minutes by a video camera positioned onshore near China Creek, Alberni Inlet.
 * A collection of still images taken every 5 minutes by a video camera positioned onshore near Cape Mudge in Discovery Passage, Campbell River. 
 * Possible addition: hydrophone recordings from underwater offshore from the Alberni Inlet location.

Goal layout: 

boat-traffic/
    data/
        CCSS/                 # 1 year of CCSS images (every 5 minutes)
        <ONC_filename>.jpg
        ...
        CRSS/                 # 1 year of CRSS images (every 5 minutes)
        <ONC_filename>.jpg
        ...
    metadata/
        manifest.csv          # 4 columns: timestamp, locationCode, filename, (local) path
        provenance.yaml       # challenge description, api call descriptions

Size estimate: 28.5 GB
- 105,120 files in 1 year (every 5 minutes)
- Upper lim file size: 135606 bytes

- Upper memory lim for challenge: 105120 x 135606 x 2 = 28,509,805,440 bytes = 28.5 GB 

"""
# SDKs
import yaml
import urllib
import os, shutil
from pathlib import Path
from datetime import UTC, datetime, timedelta
import math
import pandas as pd
from dotenv import load_dotenv
import onc

load_dotenv()

# --- Project-root based paths ---
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data" 
METADATA_ROOT = PROJECT_ROOT / "metadata"

CCSS_ROOT = DATA_ROOT / "CCSS"
CRSS_ROOT = DATA_ROOT / "CRSS"

# Make sure folders exist
DATA_ROOT.mkdir(parents=True, exist_ok=True)
METADATA_ROOT.mkdir(parents=True, exist_ok=True)

# --- ONC API client setup ---
TOKEN = os.getenv("ONC_TOKEN")
MY_ONC = onc.ONC(TOKEN, outPath=str(Path(DATA_ROOT)))

CRSS_CLIENT = onc.ONC(TOKEN, outPath=str(CRSS_ROOT))
CCSS_CLIENT = onc.ONC(TOKEN, outPath=str(CCSS_ROOT))

# Global variables
CHINA_LOCATION = "CCSS"
MUDGE_LOCATION = "CRSS"

API_CALL_N = 0 # Count of API calls made
NEW_MANIFEST = True

PROV_INFO = {
    "challenge": "boat-traffic",
    "description": "1 year of still images (every 5 minutes) from shore station cameras at China Creek and Cape Mudge",
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
def calculate_difference(date1: str, date2: str) -> int:
    """
    Calculates the difference in days between two dates in ISO 8601 format.

    Preconditions:
    - date1 and date2 are strings in ISO 8601 format (e.g., "2023-09-01T00:00:00.000Z")
    - date1 is before date2

    Inputs:
    Output:
    """
    # Convert string inputs defined in the params above to datetime objects
    start_time = datetime.fromisoformat(date1.replace("Z", "+00:00"))
    end_time = datetime.fromisoformat(date2.replace("Z", "+00:00"))

    # Calculate the duration in hours (there are 3600 seconds in an hour)
    return (end_time - start_time).days

def get_6_month_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list:
    """
    Returns a list of filenames for still images from the video camera at the specified location within a 6 month period.

    Inputs:
    Output:
    """

    params = {
    'locationCode': locationCode,
    'dateFrom': dateFrom,
    'dateTo': dateTo,
    'deviceCategoryCode': "VIDEOCAM",
    'fileExtension': "jpg", # 'jpg' for still images
    # 'returnOptions': "all" # NOTE: debugging to find file size demands
    }

    response = MY_ONC.getArchivefileByLocation(params) # Make request 
    files = response.get('files', []) # Isolate list of files
    n = len(files) # Number of files

    # Isolate info for provenance - NOTE: done for each API call
    query_url = response.get('queryUrl')
    citations_info = response.get('citations')[0]
    url_parsed = urllib.parse.urlparse(query_url) # Parse the URL and break into components

    params_minus_token = params.copy()
    del params_minus_token['token']
    
    # Update global PROV_INFO with API call details
    global API_CALL_N, PROV_INFO

    PROV_INFO["api"][f"call_{API_CALL_N + 1}"] = {
    "endpoint": url_parsed.path.replace("/api", "", 1),
    "parameters": params_minus_token,
    "queryUrl": query_url,
    "citation": citations_info['citation'],
    "doi": citations_info['doi'],
    }

    API_CALL_N += 1 # Update global API call count

    # NOTE: debugging output
    # print("6 months of data")
    # print(f"file 1: {files[0] if files else 'No files found.'}")
    # print(f"file {n}: {files[-1] if files else 'No files found.'}")
    # print(f"num files: {n}")

    return files

def get_yr_filenames(locationCode: str, dateFrom: str, dateTo: str) -> list:
    """
    Returns a list of filenames for still images from the video camera at the specified location for a full year.

    Inputs:
    Output:
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
    print(f"{locationCode} YEAR TOTAL: {len(all_files)} files")

    return all_files

def filenames_to_manifest(filenames: list[str], locationCode: str, challenge: str) -> pd.DataFrame:
    """
    Converts a list of filenames to a manifest DataFrame with additional metadata.
    
    Inputs:

    Output: Pandas DataFrame with columns: 
    - 'timestamp': the date extracted from the filename
    - 'locationCode': the location code of the file
    - 'filename': the name of the file
    - 'path': planned local download path for the file
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

    # Either append or start a fresh manifest depending on API call
    global NEW_MANIFEST
    if NEW_MANIFEST: # Overwrite existing manifest
        mode = "w" 
        header = True 
    else: # Append to existing manifest
        mode = "a"
        header = False

    # Save to CSV
    metadata_path = Path(METADATA_ROOT) / "manifest.csv" # Build metadata path
    os.makedirs(metadata_path.parent, exist_ok = True) # Ensure the parent directory exists
    df.to_csv(metadata_path, mode = mode, header = header,  index = False) # Save CSV

    # Isolate info for provenance - NOTE: DONE ONLY ONCE for all API calls together
    if not NEW_MANIFEST:
        global PROV_INFO
        PROV_INFO["manifest"] = {
            "manifest_path": str(metadata_path),
            "manifest_schema": ['timestamp', 'locationCode', 'filename', 'path'],
            "last_updated": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }

    # NOTE: debug output
    # if NEW_MANIFEST:
    #     print("Created new manifest.")
    # else:
    #     print("Updated existing manifest.")

    NEW_MANIFEST = False # Set to false for next location

    return df

def download_from_manifest(manifest_df: pd.DataFrame, locationCode: str, subsample: int | None = None, per_file_sleep: float = 0.05) -> None:
    """
    Download files listed in `manifest_df`.

    subsample:
      - If given, only download the first `subsample` rows (keeps time order).

    Inputs:
    Output:
    """
    df = manifest_df.head(subsample) if subsample else manifest_df

    ok = skipped = failed = 0

    # Different clients for different output folders
    if locationCode == "CCSS":
        client = CCSS_CLIENT
    elif locationCode == "CRSS":
        client  = CRSS_CLIENT
    else: 
        client = MY_ONC

    for _, row in df.iterrows():
        # Build relative path: locationCode/filename
        fname= Path(row["filename"])
        dest = client.outPath / fname
        print(f"dest: {dest}")

        # Skip if already downloaded
        if dest.exists():
            skipped += 1
            continue

        try:
            client.getFile(str(fname))

            ok += 1
        except Exception as e:
            print(f"[ERROR] {fname}: {e}")
            failed += 1

    print(f"[done] ok={ok} skipped={skipped} failed={failed}")

    return

def make_prov(metadata_root: str = "./metadata") -> None:
    """
    Save provenance info dictionary to a YAML file.

    Inputs:
    Output:
    """
    output_path = Path(metadata_root) / "provenance.yaml"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        yaml.dump(PROV_INFO, f, sort_keys=False, default_flow_style=False)
    print(f"[saved provenance] {output_path}") # NOTE: debug output

def main():

    # Chosen year
    yr_start = "2023-09-01T00:00:00.000Z"
    yr_end = "2024-09-01T00:00:00.000Z"

    # get filenames
    china_files = get_yr_filenames(locationCode = CHINA_LOCATION, dateFrom = yr_start, dateTo = yr_end)
    mudge_files = get_yr_filenames(locationCode = MUDGE_LOCATION, dateFrom = yr_start, dateTo = yr_end)

    # # Small test files
    # small_test_files = china_files[:5]

    # # S/M test files
    # sm_step = math.ceil(len(china_files) / 200)  
    # sm_test_files = china_files[::sm_step]

    # # Medium test files
    # m_step = math.ceil(len(china_files) / 1000)  
    # medium_test_files = china_files[::m_step]

    # # Large test files
    # l_step = math.ceil(len(china_files) / 10000)  
    # large_test_files = china_files[::l_step]

    # Build manifest
    china_manifest_df = filenames_to_manifest(filenames = china_files, locationCode = CHINA_LOCATION, challenge = "boat-traffic")
    mudge_manifest_df = filenames_to_manifest(filenames = mudge_files, locationCode = MUDGE_LOCATION, challenge = "boat-traffic")

    # Build prov
    make_prov()

    # Store files - tester for only 10 files
    download_from_manifest(manifest_df = china_manifest_df, locationCode = CHINA_LOCATION, subsample= 10)
    download_from_manifest(manifest_df = mudge_manifest_df, locationCode = MUDGE_LOCATION, subsample= 10)


if __name__ == "__main__":
    main()