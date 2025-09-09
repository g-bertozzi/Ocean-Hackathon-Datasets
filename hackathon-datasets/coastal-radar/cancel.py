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
import os
import pandas as pd
from dotenv import load_dotenv
import onc
import pandas as pd


# --- Setup ---
load_dotenv()

# ONC client
TOKEN = os.getenv("ONC_TOKEN") # NOTE: Change this to your ONC token
MY_ONC = onc.ONC(TOKEN, outPath=str("/Users/catherinebertozzi/hackathon-datasets/coastal-radar/data/plots(2024)"))

def cancel_product(id:int):
    print(MY_ONC.cancelDataProduct(id))

def check_status(id:int):
    print (MY_ONC.checkDataProduct(id))

def req_prod(filters: dict) -> int:
    req_response = MY_ONC.requestDataProduct(filters=filters)
    dpRequestId = req_response.get("dpRequestId")

    print(f"[requestDataProduct] dpRequestId: {dpRequestId}")

    return dpRequestId

def run_prod(id:int) -> int:
    run_response = MY_ONC.runDataProduct(dpRequestId=id)
    runId = run_response.get("runIds")

    print(f"[runDataProduct] {run_response}")
    print(f"[runDataProduct] dpRequestId: {id} runId: {runId}")

    return runId

def download_prod(id:int):
    return MY_ONC.downloadDataProduct(runId=id) #, maxRetries= 0, downloadResultsOnly= False, includeMetadataFile = False, overwrite = False)

def main():

    params = { "dateFrom": "2024-01-01T00:00:00.000Z",
        "dateTo": "2025-01-01T00:00:00.000Z",
        "locationCode": "SOGCS",
        "dataProductCode": "CODARQCSC",
        "deviceCategoryCode": "OCEANOGRAPHICRADAR",
        "extension": "png",
        "dpo_includeRadials": 0}
    
    req_id = req_prod(filters=params)

    """
    yr of 2023
    Estimated File Size: 898 MB
    Estimated Processing Time: 8 h
    [requestDataProduct] dpRequestId: 27111093

    yr of 2024 
    Request Id: 27111127
    Estimated File Size: 385 kB
    Estimated Processing Time: 50 s
    [requestDataProduct] dpRequestId: 27111127
    """ 
    

    run_id = run_prod(id=req_id)

    print(f"[main] dpRequestId {req_id} dpRunId {run_id}")

    # print(download_prod(run_id))

    # cancel_product(id=27102642)
    # check_status(id=27102642)
    


if __name__ == "__main__":
    main()
