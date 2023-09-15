# imports and environment variables
import argparse
import json
import requests
import time

from google.cloud import storage
from oauth2client.client import GoogleCredentials


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def get_job_result(job_id):
    """Call retrieveJobResult."""

    # first check job status - retrieveJob
    uri = f"https://data.terra.bio/api/repository/v1/jobs/{job_id}/result"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}

    response = requests.get(uri, headers=headers)
    response_json = json.loads(response.text)
    status_code = response.status_code

    return status_code, response_json


def get_job_status(job_id):
    """Call retrieveJobStatus."""

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


def create_ingest_dataset_request(ingest_filepath, target_table_name, update_strategy="replace"):
    """Create the ingestDataset request body."""

    load_dict = {"format": "json",
                 "path": ingest_filepath,
                 "table": target_table_name,
                 "resolve_existing_files": "true",
                 "updateStrategy": update_strategy
                }

    load_json = json.dumps(load_dict) # dict -> json

    return load_json


def run_ingest(dataset_id, ingest_data_json_filepaths, update_strategy="replace"):
    """For each json path containing data to ingest, create ingest request and ingest to corresponding TDR dataset table."""

    for path in ingest_data_json_filepaths:
        # path = gs://bucket_name/subdir/filename.json
        # filename = ingest destination table in dataset 
        table_name = path.split("/")[-1].split(".")[0]
        print(f"Starting ingest of {path} to table: {table_name}.")

        # create request for ingestDataset
        ingest_request = create_ingest_dataset_request(path, table_name, update_strategy) 
        print(f"Submitted ingest request body: \n {ingest_request} \n")

        # call ingestDatset
        ingest_response = ingest_dataset(dataset_id, ingest_request) # call ingestDataset
        print(f"Submitted ingest response body: \n {ingest_response} \n")

        ingest_complete_response = monitor_ingest(ingest_response)
        print(f"Finished ingest response body: \n {ingest_complete_response} \n")

        print(f"Finished ingest for table {table_name}. \n\n")


def get_ingest_data_json_filepaths(bucket_name, subdir):
    """Get list of gs:// paths for each new-line delimited json containing data to ingest into TDR dataset table."""

    storage_client = storage.Client()

    if not subdir:
        prefix = None
        delimiter = None
    else:
        # add tailing "/" for subdir as required by storage_client
        prefix = f"{subdir}/"
        delimiter = "/"

    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    # capture paths to json files containing data to ingest at listed bucket path if they are not empty
    paths = []
    for blob in blobs:
        if blob.name.endswith(".json") and blob.size > 0:
            paths.append(f"gs://{bucket_name}/{blob.name}")

    if not paths:
        raise ValueError(f"Error: There were no .json files found at gs://{bucket_name}/{subdir}. Please confirm path and try again.")
    
    print(f"Finished gathering {len(paths)} json files from gs://{bucket_name}/{subdir} for ingest into TDR dataset.")
    return paths


if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description="Ingest data from json files in a GCP bucket into a TDR dataset.")

    parser.add_argument('-b', '--bucket_path', required=True, type=str, help="gs://bucket_name/subdir/ pointing to new-line delimited json files for ingest to dataset")
    parser.add_argument('-d', '--dataset_id', required=True, type=str, help="id of TDR dataset for destination of outputs")
    parser.add_argument('-u', '--update_strategy', default="replace", type=str, help="update strategy choices: merge, replace, or append. default = replace")

    args = parser.parse_args()

    # parse bucket_name and subdirectories
    if args.bucket_path.startswith("gs://"):
        bucket_name = args.bucket_path.split("/")[2] # bucket_name
        subdir = "/".join(args.bucket_path.split("/")[3:]).strip("/") # subdirectory path in bucket
    else:
        bucket_name = args.bucket_path.split("/")[0] # bucket_name
        subdir = "/".join(args.bucket_path.split("/")[1:]).strip("/") # subdirectory path in bucket

    # get list of gs:// paths pointing to data ingest json files
    paths = get_ingest_data_json_filepaths(bucket_name, subdir)
    # run ingest of each json file to corresponding TDR dataset table
    run_ingest(args.dataset_id, paths, args.update_strategy)