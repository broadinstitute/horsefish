# imports and environment variables
import argparse
import json
import pandas as pd
import pytz
import requests
import time

from datetime import datetime
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


def create_ingest_dataset_request(control_file_path, target_table_name, load_tag=None):
    """Create the ingestDataset request body."""

    load_dict = {"format": "json",
                 "path": control_file_path,
                 "table": target_table_name,
                 "resolve_existing_files": "true",
                 "updateStrategy": "replace"
                }
    # if user provides a load_tag, add it to request body
    if load_tag:
        load_dict["load_tag"] = load_tag

    load_json = json.dumps(load_dict) # dict -> json

    return load_json


def call_ingest_dataset(control_file_path, target_table_name, dataset_id, load_tag=None):
    """Create the ingestDataset API json request body and call API."""

    ingest_dataset_request = create_ingest_dataset_request(control_file_path, target_table_name, load_tag) # create request for ingestDataset
    print(f"ingestDataset request body: \n {ingest_dataset_request} \n")

    ingest_response = ingest_dataset(dataset_id, ingest_dataset_request) # call ingestDataset
    print(f"ingestDataset response body: \n {ingest_response} \n")

    ingest_job_id = ingest_response["id"] # check for id in ingest_dataset()
    ingest_status_code = ingest_response["status_code"]

    if ingest_status_code != 202: # job fails to start
        print(f"{ingest_job_id} --> failed")
        raise ValueError(ingest_response)

    # 202 = job starts running
    job_status_code, job_status_response = get_job_status(ingest_job_id)
    # job_status_code, job_response = get_job_status_and_result(ingest_job_id)

    while job_status_code == 202:           # while job is running
        print(f"{ingest_job_id} --> running")
        time.sleep(10) # wait 10 seconds before getting job status
        job_status_code, job_status_response = get_job_status(ingest_job_id) # get updated status info

    # job completes
    # with failure --> success_code not 200 and "job_status" is "failed"
    # any other combination is failure
    if job_status_code != 200 or job_status_response["job_status"] != "succeeded":
        print(f"{ingest_job_id} --> failed")
        job_result_code, job_result_response = get_job_result(ingest_job_id)
        raise ValueError(job_result_response)    

    # job completes successfully
    # success_code is 200 and "job_status" =is"succeeded"
    print(f"{ingest_job_id} --> succeeded")
    
    # write load tag to output file from final succeeded job result response
    job_result_code, job_result_response = get_job_result(ingest_job_id)
    result_load_tag = job_result_response["load_tag"]
    with open("load_tag.txt", "w") as loadfile:
        loadfile.write(result_load_tag)

    print("File ingest to TDR dataset completed successfully.")


def write_load_json_to_bucket(bucket, filename):
    """Copy newline delimited json file to workspace bucket."""

    control_file_destination = f"{bucket}/control_files"

    storage_client = gcs.Client()
    dest_bucket = storage_client.get_bucket(bucket)

    blob = dest_bucket.blob(f"control_files/{filename}")
    blob.upload_from_filename(filename)

    print(f"Successfully copied {filename} to {control_file_destination}/{filename}.")
    return f"gs://{control_file_destination}/{filename}"


def create_newline_recoded_json(recoded_json_list, outfile_prefix):
    """Create a newline delimited json file from recoded dictionaries in list."""

    output_filename = f"{outfile_prefix}_newline_delimited.json"
    with open(output_filename, "w") as outfile:
        for recoded_dict in recoded_json_list:
            json.dump(recoded_dict, outfile)
            outfile.write("\n")

    return output_filename


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
                                    "targetPath":relative_tdr_path}
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
                                                   "targetPath":relative_tdr_path}
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
    """Create a recoded json dictionary per row in input."""

    tsv_df = pd.read_csv(input_tsv, sep="\t")
    all_recoded_row_dicts = []

    for index, row in tsv_df.iterrows():
        last_modified_date = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")
    
        # drop empty columns and add in timestamp
        remove_row_nan = row.dropna()
        remove_row_nan["last_modified_date"] = last_modified_date
        recoded_row_dict = create_recoded_json(remove_row_nan)

        all_recoded_row_dicts.append(recoded_row_dict)

    return all_recoded_row_dicts, last_modified_date


if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='Push Arrays.wdl outputs to TDR dataset.')

    parser.add_argument('-f', '--tsv', required=True, type=str, help='tsv file of files to ingest to TDR')
    parser.add_argument('-b', '--bucket', required=True, type=str, help='workspace bucket to copy recoded json file')
    parser.add_argument('-d', '--dataset_id', required=True, type=str, help='id of TDR dataset for destination of outputs')
    parser.add_argument('-t', '--target_table_name', required=True, type=str, help='name of target table in TDR dataset')
    parser.add_argument('-l', '--load_tag', required=False, type=str, help="load tag from a previous job")

    args = parser.parse_args()

    all_recoded_row_dicts, last_modified_date = parse_json_outputs_file(args.tsv)
    newline_recoded_outfile = create_newline_recoded_json(all_recoded_row_dicts, last_modified_date)
    control_file_path = write_load_json_to_bucket(args.bucket, newline_recoded_outfile)
    call_ingest_dataset(control_file_path, args.target_table_name, args.dataset_id, args.load_tag)