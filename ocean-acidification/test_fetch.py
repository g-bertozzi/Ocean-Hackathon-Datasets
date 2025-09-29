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
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data/vectors"
METADATA_ROOT = PROJECT_ROOT / "metadata/vectors"
MANIFEST_PATH = METADATA_ROOT / "manifest.csv"
PROVENANCE_PATH = METADATA_ROOT / "provenance.yaml"

# ONC client
TOKEN = os.getenv("ONC_TOKEN") # NOTE: Change this to your ONC token
CLIENT = onc.ONC(TOKEN, outPath=str(DATA_ROOT))

LOCATION = "BSM.J3" # Baynes Sound Mooring 40mbss
# need deviceCategoryCode=PHSENSOR and deviceCategoryCode=CO2SENSOR ?

def main():
    # 
    params = {
        "locationCode": LOCATION,
        "deviceCategoryCode": "PHSENSOR",
        "dateFrom": "2025-06-23T00:00:00.000Z",
        "dateTo": "2025-06-24T00:00:00.000Z"

    }

    response = CLIENT.getScalardata(params)
    print(response)


if __name__ == "__main__":
    main()