# imports and environment variables
import argparse
import json
import pandas as pd
import pytz
import requests
import time

from datetime import datetime
from google.cloud import storage
from google.cloud import storage as gcs
from oauth2client.client import GoogleCredentials


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def get_job_result(job_id):
    """retrieveJobResult"""

    # first check job status - retrieveJob
    uri = f"https://data.terra.bio/api/repository/v1/jobs/{job_id}/result"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}

    response = requests.get(uri, headers=headers)
    response_json = json.loads(response.text)
    status_code = response.status_code

    return status_code, response_json


def get_job_status(job_id):
    """retrieveJobStatus"""

    # first check job status - retrieveJob
    uri = f"https://data.terra.bio/api/repository/v1/jobs/{job_id}"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}

    response = requests.get(uri, headers=headers)
    response_json = json.loads(response.text)
    status_code = response.status_code

    return status_code, response_json


def monitor_ingest(request_body):
    """Monitor status of a submitted ingestDataset request."""

    # get job id from ingestDataset request body
    job_id = request_body["id"]

    # 202 = job is running as confirmed in ingest_dataset()
    job_status_code, job_status_response = get_job_status(job_id)

    while job_status_code == 202:           # while job is running
        print(f"{job_id} --> running")
        time.sleep(10) # wait 10 seconds before getting job status
        job_status_code, job_status_response = get_job_status(job_id) # get updated status info

    # job completes (‘failed’ or ‘succeeded’) with 200 status_code
    # consider any combination other than 200 + succeeded as failure
    if not (job_status_code == 200 and job_status_response["job_status"] == "succeeded"):
        print(f"{job_id} --> failed")
        # if failed, get the resulting error message
        job_result_code, job_result_response = get_job_result(job_id)
        raise ValueError(job_result_response)    

    # job completes successfully
    # success_code is 200 and "job_status" = is "succeeded"
    print(f"{job_id} --> succeeded")
    
    # get final job response for successfully completed ingest
    job_result_code, job_result_response = get_job_result(job_id)

    return job_result_response


def ingest_dataset(dataset_id, data):
    """Load data into TDR with ingestDataset."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/ingest"

    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json",
               "Content-Type": "application/json"}

    response = requests.post(uri, headers=headers, data=data)
    status_code = response.status_code

    if status_code != 202:  # if ingest start-up fails
        raise ValueError(response.text)

    # if ingest start-up succeeds
    return json.loads(response.text)


def create_ingest_dataset_request(ingest_filepath, target_table_name):
    """Create the ingestDataset request body."""

    load_dict = {"format": "json",
                 "path": ingest_filepath,
                 "table": target_table_name,
                 "resolve_existing_files": "true",
                 "updateStrategy": "replace"
                }

    load_json = json.dumps(load_dict) # dict -> json

    return load_json


def run_ingest(dataset_id, data_filepaths):
    """For each json path, create ingest request and ingest to TDR dataset."""

    for path in data_filepaths:
        # path = gs://bucket_name/subdir/filename.json
        # filename = ingest destination table in dataset 
        table_name = path.split("/")[-1].split(".")[0]
        print(f"Starting ingest of {path} to table: {table_name}.")

        # create request for ingestDataset
        ingest_request = create_ingest_dataset_request(path, table_name) 
        print(f"Submit ingest request body: \n {ingest_request} \n")

        # call ingestDatset
        ingest_response = ingest_dataset(dataset_id, ingest_request) # call ingestDataset
        print(f"Submit ingest response body: \n {ingest_response} \n")

        ingest_complete_response = monitor_ingest(ingest_response)
        print(f"Finished ingest response body: \n {ingest_response} \n")

        print(f"Finished ingest for table {table_name}. \n\n")


def get_json_paths(bucket_name, subdir):
    """Get list of gs:// paths for each new-line delimited json."""

    storage_client = storage.Client()

    # add tailing "/" for subdir as required by storage_client
    blobs = storage_client.list_blobs(bucket_name, prefix=f"{subdir}/", delimiter='/')

    # get paths to json files at listed bucket path
    paths = []
    for blob in blobs:
        if blob.name.endswith(".json"):
            paths.append(f"gs://{bucket_name}/{blob.name}")

    if not paths:
        raise ValueError(f"Error: There were no .json files found at gs://{bucket_name}{subdir}. Please confirm path and try again.")
    
    print(f"Gathered all json files from gs://{bucket_name}/{subdir}.")
    return paths


if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='Push Arrays.wdl outputs to TDR dataset.')

    parser.add_argument('-b', '--bucket_path', required=True, type=str, help='gs://bucket_name/subdir/ pointing to new-line delimited json files for ingest to dataset')
    parser.add_argument('-d', '--dataset_id', required=True, type=str, help='id of TDR dataset for destination of outputs')

    args = parser.parse_args()

    if args.bucket_path.startswith("gs://"):
        bucket_name = args.bucket_path.split("/")[2] # bucket_name
        subdir = "/".join(args.bucket_path.split("/")[3:]).strip("/") # subdirectory path in bucket
    else:
        bucket_name = args.bucket_path.split("/")[0] # bucket_name
        subdir = "/".join(args.bucket_path.split("/")[1:]).strip("/") # subdirectory path in bucket
    
    paths = get_json_paths(bucket_name, subdir)
    run_ingest(args.dataset_id, paths)