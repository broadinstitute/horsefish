# imports and environment variables
import argparse
import json
import os
import pandas as pd
import pytz
import requests
import sys
import time

from datetime import datetime, tzinfo
from firecloud import api as fapi
from google.cloud import bigquery
from google.cloud import storage as gcs
from oauth2client.client import GoogleCredentials
from pprint import pprint


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def get_job_status_and_result(job_id):
    # first check job status
    uri = f"https://data.terra.bio/api/repository/v1/jobs/{job_id}"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}
    time.sleep(10)
    retrieveJob_response = requests.get(uri, headers=headers)
    retrieveJob_response_json = json.loads(retrieveJob_response.text)
    retrieveJob_status_code = retrieveJob_response.status_code

    # job_status = response_json['job_status']
    if retrieveJob_status_code == 202:                                           # still running
        print(f"load job {job_id} --> running")
        return retrieveJob_status_code, retrieveJob_response_json

    if retrieveJob_status_code == 200:                                          # finished successfully
        print(f"load job {job_id} --> success")
        retrieveJobResult_response = requests.get(uri + "/result", headers=headers)
        retrieveJobResult_response_json = json.loads(retrieveJobResult_response.text)
        retrieveJobResult_status_code = retrieveJobResult_response.status_code
        return retrieveJobResult_status_code, retrieveJobResult_response_json

    print(f"load job {job_id} --> not successful")  # finished but not successfully
    return retrieveJob_status_code, retrieveJob_response_json


def load_data(dataset_id, ingest_data):
    """Load data into TDR"""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/ingest"

    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json",
               "Content-Type": "application/json"}

    response = requests.post(uri, headers=headers, data=ingest_data)
    status_code = response.status_code

    if status_code != 202:
        return response.text

    return json.loads(response.text)


def call_ingest_dataset(control_file_path, target_table_name, dataset_id):
    """Create the ingestDataset API json request body and call API."""

    load_json = json.dumps({"format": "json",
                            "path": control_file_path,
                            "table": target_table_name,
                            "resolve_existing_files": "true",
                            "updateStrategy": "replace"
                            })
    load_job_response = load_data(dataset_id, load_json)

    print(f"ingestDataset request body: \n {load_json} \n")
    print(f"ingestDataset response body: \n {load_job_response} \n")

    ingestDataset_job_id = load_job_response["id"]
    ingestDataset_status_code = load_job_response["status_code"]

    if ingestDataset_status_code != 202:
        print("file ingest to TDR dataset failed.")
        print(f"please refer to error message and retry ingest.")
        print(f"ingestDataset response body: \n {load_job_response} \n")
        return

    # if ingestDataset_status_code == 202:  # if ingestDataset starts running successfully
    job_status_code, job_response = get_job_status_and_result(ingestDataset_job_id)
    while job_status_code == 202:           # while job is still running
        time.sleep(10)  # wait 10 seconds
        job_status_code, job_response = get_job_status_and_result(ingestDataset_job_id)

    if job_status_code != 200:              # when job completes but not success
        error_message = job_response["errorDetail"]
        print(f"Load job finished but did not succeed: {error_message}")
        return

    # when job completes but successful
    failed_files = job_response["load_result"]["loadSummary"]["failedFiles"]
    succeeded_files = job_response["load_result"]["loadSummary"]["succeededFiles"]
    total_files = job_response["load_result"]["loadSummary"]["totalFiles"]

    print(f"Total files to load count: {total_files}")
    print(f"Successfully loaded file count: {succeeded_files}")
    print(f"Failed to load file count: {failed_files}")

    print("File ingest to TDR dataset completed successfully.")


def write_load_json_to_bucket(bucket, recoded_rows_json, timestamp):
    """Write the list of recoded rows (list of dictionaries) to a file and copy to workspace bucket."""

    loading_json_filename = f"{timestamp}_recoded_ingestDataset.json"
    # write load json to the workspace bucket
    control_file_destination = f"{bucket}/control_files"

    with open(loading_json_filename, 'w') as final_newline_json:
        json.dump(recoded_rows_json, final_newline_json)

    storage_client = gcs.Client()
    dest_bucket = storage_client.get_bucket(bucket)

    blob = dest_bucket.blob(f"control_files/{loading_json_filename}")
    blob.upload_from_filename(loading_json_filename)

    print(f"Successfully copied {loading_json_filename} to {control_file_destination}.")
    return f"gs://{control_file_destination}/{loading_json_filename}"


def create_recoded_json(row_json):
    """Update dictionary with TDR's dataset relative paths for keys with gs:// paths."""

    recoded_row_json = dict(row_json)  # update copy instead of original

    for key in row_json.keys():  # for column name in row
        value = row_json[key]    # get value
        if value is not None:  # if value exists (non-empty cell)
            if isinstance(value, str):  # and is a string
                if value.startswith("gs://"):  # starting with gs://
                    relative_tdr_path = value.replace("gs://","/")  # create TDR relative path
                    # recode original value/path with expanded request
                    # TODO: add in description = id_col + col_name
                    recoded_row_json[key] = {"sourcePath":value,
                                    "targetPath":relative_tdr_path,
                                    "mimeType":"text/plain"
                                    }
                    continue

                recoded_row_json_list = []  # instantiate empty list to store recoded values for arrayOf:True cols
                if value.startswith("[") and value.endswith("]"):  # if value is an array
                    value_list = json.loads(value)  # convert <str> to <liist>

                    # check if any of the list values are non-string types
                    non_string_list_values = [isinstance(item, str) for item in value_list]
                    # if non-string types, add value without recoding
                    if not any(non_string_list_values):
                        recoded_row_json[key] = value_list
                        continue

                    # check if any of the list_values are strings that start with gs://
                    gs_paths = [item.startswith('gs://') for item in value_list]
                    # TODO: any cases where an item in a list is not gs:// should be a user error?
                    if any(gs_paths):
                        for item in value_list:  # for each item in the array
                            relative_tdr_path = item.replace("gs://","/")  # create TDR relative path
                            # create the json request for list member
                            recoded_list_member = {"sourcePath":item,
                                                   "targetPath":relative_tdr_path,
                                                   "mimeType":"text/plain"
                                                   }
                            recoded_row_json_list.append(recoded_list_member)  # add json request to list
                        recoded_row_json[key] = recoded_row_json_list  # add list of json requests to larger json request
                        continue

                    else:  # when list values are strings that DO NOT start with gs:// (like filerefs)
                        for item in value_list:  # for each string item in non gs:// path array
                            recoded_row_json_list.append(item)
                        recoded_row_json[key] = recoded_row_json_list
                        continue
                # if value is string but not a gs:// path or list of gs:// paths
                recoded_row_json[key] = value

    return recoded_row_json


def parse_json_outputs_file(input_tsv):
    """Format the json file containing workflow outputs and headers."""

    tsv_df = pd.read_csv(input_tsv, sep="\t")

    basename = input_tsv.split(".tsv")[0]
    output_filename = f"{basename}_recoded_newline_delimited.json"

    last_modified_date = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")

    all_rows = []
    for index, row in tsv_df.iterrows():
        # drop empty columns and add in timestamp
        remove_row_nan = row.dropna()
        remove_row_nan["last_modified_date"] = last_modified_date

        recoded_row_dict = create_recoded_json(remove_row_nan)

    return recoded_row_dict, last_modified_date


if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='Push Arrays.wdl outputs to TDR dataset.')

    parser.add_argument('-f', '--tsv', required=True, type=str, help='tsv file of files to ingest to TDR')
    parser.add_argument('-b', '--bucket', required=True, type=str, help='workspace bucket to copy recoded json file')
    parser.add_argument('-d', '--dataset_id', required=True, type=str, help='id of TDR dataset for destination of outputs')
    parser.add_argument('-t', '--target_table_name', required=True, type=str, help='name of target table in TDR dataset')

    args = parser.parse_args()

    # created the recoded json
    recoded_row_dict, timestamp = parse_json_outputs_file(args.tsv)
    # write recoded json to a GCS storage location - to the bucket given (workspace bucket)
    control_file_path = write_load_json_to_bucket(args.bucket, recoded_row_dict, timestamp)
    # create request to ingestDataset, call API, return response, and status update
    call_ingest_dataset(control_file_path, args.target_table_name, args.dataset_id)
