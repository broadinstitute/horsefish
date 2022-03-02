import argparse
import datetime
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage as gcs
from oauth2client.client import GoogleCredentials
from pprint import pprint
from time import sleep


# define some utils functions
def get_access_token():
    """Get access token."""
    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)
    return credentials.get_access_token().access_token

def get_headers(request_type='get'):
    headers = {"Authorization": "Bearer " + get_access_token(),
                "accept": "application/json"}
    if request_type == 'post':
        headers["Content-Type"] = "application/json"
    return headers

def write_file_to_bucket(filename, bucket):
    dir = "tdr"
    storage_client = gcs.Client()
    dest_bucket = storage_client.get_bucket(bucket)
    blob = dest_bucket.blob(f"{dir}/{filename}")
    blob.upload_from_filename(filename)
    control_file_full_path = f"gs://{bucket}/{dir}/{filename}"
    print(f"Successfully copied {filename} to {control_file_full_path}.")
    return control_file_full_path

def configure_path_json(v):
    tdr_path = v.replace("gs://","/")

    return { 
        "sourcePath": v,
        "targetPath": tdr_path
    }

def configure_list(v_list):
    v_list_recoded = []

    # check if any of the list values are non-string types
    for v in v_list:
        if isinstance(v, str) and v.startswith("gs://"):
            # update json for loading files
            v_list_recoded.append(configure_path_json(v))
        else:
            # don't change it
            v_list_recoded.append(v)
    
    return v_list_recoded

def recode_json_with_filepaths(json_object):
    """Takes a dict, transforms files for upload as needed for TDR ingest, returns updated dict."""
    for k in json_object.keys():
        v = json_object[k]
        if v is None:
            # nothing needed
            continue
        
        if isinstance(v, str) and v.startswith("gs://"):
            # update json for loading files
            json_object[k] = configure_path_json(v)
        elif isinstance(v, str) and v.startswith("[") and v.endswith("]"):  # if value is an array
            v_list = json.loads(v)  # convert <str> to <list>
            json_object[k] = configure_list(v_list)
        elif isinstance(v, list):
            json_object[k] = configure_list(v)

    return json_object

def wait_for_job_status_and_result(job_id, wait_sec=10):
    # first check job status
    uri = f"https://data.terra.bio/api/repository/v1/jobs/{job_id}"

    headers = get_headers()
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    while status_code == 202:
        print(f"job running. checking again in {wait_sec} seconds")
        sleep(wait_sec)
        response = requests.get(uri, headers=headers)
        status_code = response.status_code

    if status_code != 200:
        print(f"error retrieving status for job_id {job_id}")
        return "internal error", response.text

    job_status = response.json()['job_status']
    print(f'job_id {job_id} has status {job_status}')
    # if job status = done, check job result
    if job_status in ['succeeded', 'failed']:
        result_uri = uri + "/result"
        print(f'retrieving job result from {result_uri}')
        response = requests.get(result_uri, headers=get_headers())

    return job_status, response.json()


def main(dataset_id, bucket, target_table, outputs_json, sample_id):
    # clean up bucket prefix
    bucket = bucket.replace("gs://","")

    # read workflow outputs from file
    print(f"reading data from outputs_json file {outputs_json}")
    with open(outputs_json, "r") as infile:
        outputs_dict = json.load(infile)

    # recode any paths (files) for TDR ingest
    print("recoding paths for TDR ingest")
    outputs_to_load = recode_json_with_filepaths(outputs_dict)

    # update version_timestamp field
    new_version_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    outputs_to_load['version_timestamp'] = new_version_timestamp

    # write update json to disk and upload to staging bucket
    loading_json_filename = f"{sample_id}_{new_version_timestamp}_recoded_ingestDataset.json"
    with open(loading_json_filename, 'w') as outfile:
        outfile.write(json.dumps(outputs_to_load))
        outfile.write("\n")
    load_file_full_path = write_file_to_bucket(loading_json_filename, bucket)

    # ingest data to TDR
    load_json = json.dumps({"format": "json",
                        "path": load_file_full_path,
                        "table": target_table,
                        "resolve_existing_files": True,
                        "updateStrategy": "update"
                        })
    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/ingest"
    response = requests.post(uri, headers=get_headers('post'), data=load_json)
    load_job_id = response.json()['id']
    job_status, job_info = wait_for_job_status_and_result(load_job_id)
    if job_status != "succeeded":
        print(f"job status {job_status}:")
        error_msg = f"{job_info["message"]}; {job_info["errorDetail"]}"
        raise ValueError(error_msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest workflow outputs to TDR')
    parser.add_argument('-d', '--dataset_id', required=True,
        help='UUID of destination TDR dataset')
    parser.add_argument('-b', '--bucket', required=True,
        help='GCS bucket to use for TDR API json payloads')
    parser.add_argument('-t', '--target_table', required=True,
        help='name of destination table in the TDR dataset')
    parser.add_argument('-o', '--outputs_json', required=True,
        help='path to a json file defining the outputs to be loaded to TDR')
    parser.add_argument('-s', '--sample_id', required=True,
        help='the sample_id of the sample to be ingested')
        
    args = parser.parse_args()

    main(args.dataset_id, args.bucket, args.target_table, args.outputs_json, args.sample_id)
