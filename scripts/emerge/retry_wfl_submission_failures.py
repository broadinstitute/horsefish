import argparse
import json
import requests

from firecloud import api as fapi
from oauth2client.client import GoogleCredentials
import time

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


def ingest_dataset(dataset_id, request):
    """Load data into TDR with ingestDataset."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/ingest"

    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json",
               "Content-Type": "application/json"}

    response = requests.post(uri, headers=headers, data=request)
    status_code = response.status_code

    if status_code != 202:  # if ingest start-up fails
        raise ValueError(response.text)

    # if ingest start-up succeeds
    return json.loads(response.text)


def call_ingest_dataset(dataset_id, request):
    """Create the ingestDataset API json request body and call API."""

    ingest_response = ingest_dataset(dataset_id, request) # call ingestDataset
    print(f"ingestDataset response body: \n {ingest_response} \n")

    ingest_job_id = ingest_response["id"] # check for presence of id in ingest_dataset()
    ingest_status_code = ingest_response["status_code"]

    # 202 = job is running as confirmed in ingest_dataset()
    job_status_code, job_status_response = get_job_status(ingest_job_id)

    while job_status_code == 202:           # while job is running
        print(f"{ingest_job_id} --> running")
        time.sleep(10) # wait 10 seconds before getting job status
        job_status_code, job_status_response = get_job_status(ingest_job_id) # get updated status info

    # job completes (‘failed’ or ‘succeeded’) with 200 status_code
    # consider any combination other than 200 + succeeded as failure
    if not (job_status_code == 200 and job_status_response["job_status"] == "succeeded"):
        print(f"{ingest_job_id} --> failed")
        # if failed, get the resulting error message
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


def format_records(data, pk_key):
    """Create formatted records from failed_samples for ingestDataset request body."""

    # create list to hold sample dict
    records = []
    # for sample in list of failed samples
    for value in data:
        value_dict = {pk_key: value}
        records.append(value_dict)
    
    return records


def create_ingest_dataset_request(table_name, ingest_data, pk_key):
    """Create ingestDataset request for failed samples."""

    records = format_records(ingest_data, pk_key)
    load_dict = {"table": table_name,
                "format": "array",
                 "records": records,
                 "updateStrategy": "merge"
                }

    load_json = json.dumps(load_dict) # dict -> json

    return load_json


def get_submission_failures(ws_project, ws_name, submission_id, include_aborted=False):
    """Return list of failed workflows from a specified submission."""
    if include_aborted:
        statuses_to_capture = ['Failed', 'Aborting', 'Aborted']
    else:
        statuses_to_capture = ['Failed']

    # get the submission data
    sub_details_json = fapi.get_submission(ws_project, ws_name, submission_id).json()

    sample_list = []
        
    # pull out workflow info
    for wf in sub_details_json['workflows']:
        if wf['status'] in statuses_to_capture:
            sample_list.append(wf['workflowEntity']['entityName'])

    statuses_string = '/'.join(statuses_to_capture)
    print(f'Found {len(sample_list)} {statuses_string} workflows in submission: {submission_id}')

    return sample_list


def main(submission_id, dataset_id, table, pk_key, ws_project, ws_name, include_aborted=False):
    """Get failed and/or aborted workflows from submission and re-ingest to TDR dataset table for WFL submission retry."""
    
    # gather list of submission failures
    # note that we assume that the primary key in Terra is the primary key in TDR
    failed_samples = get_submission_failures(ws_project, ws_name, submission_id, include_aborted)

    # get request body for failed samples
    ingest_request = create_ingest_dataset_request(table, failed_samples, pk_key)

    # ingest failed samples into dataset
    call_ingest_dataset(dataset_id, ingest_request)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create new snapshot from rows that failed a Terra workflow submission')
    
    parser.add_argument('-s', '--submission_id', required=True, help='UUID of the Terra submission containing failures')
    parser.add_argument('-d', '--dataset_id', required=True, help='UUID of source TDR dataset')
    parser.add_argument('-t', '--table', required=True, help='name of source table in the TDR dataset')
    parser.add_argument('-k', '--pk_key', default='chip_well_barcode', help='primary key of the TDR dataset table, defaults to chip_well_barcode')
    parser.add_argument('-p', '--ws_project', required=True, help='workspace project/namespace')
    parser.add_argument('-w', '--ws_name', required=True, help='workspace name')
    parser.add_argument('-a', '--include_aborted', action='store_true', help='include aborted workflows')

    args = parser.parse_args()

    main(args.submission_id,
        args.dataset_id,
         args.table,
         args.pk_key,
         args.ws_project,
         args.ws_name,
         args.include_aborted)

# python3 retry_wfl_submission_failures.py -s dfad4484-ea8d-4bea-af3b-879bbb7b0d52 -d 092d50cf-8e18-40c3-b1be-66e3f788c1ad -t PrsInputsTable -k chip_well_barcode -p emerge_prod -w PRS_test -a