# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

import urllib.request
import shutil
from modules.data_loader.file_downloader import download_file

try:
    # Construct the URL for the Parquet file corresponding to this month
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    # Open a connection and stream the remote file
    response = urllib.request.urlopen(url)

    # Define and create the local directory for this date's data
    dir_path = f"/Volumes/az_core_realstate_internal_training_catalog/00_landing_jrg/data_sources/lookup"
    os.makedirs(dir_path, exist_ok=True)

    # Define the full path for the downloaded file
    local_path = f"{dir_path}/taxi_zone_lookup.csv"

    # Save the streamed content to the local file in binary mode
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)  # Copy data from response to file
    
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File succesfully uploaded")
except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File download failed: {str(e)}")