# imports and environment variables
import argparse
from datetime import datetime, tzinfo
import json
import os
import pytz
import requests
import sys
import time

from firecloud import api as fapi
from google.cloud import bigquery
from google.cloud import storage as gcs
from oauth2client.client import GoogleCredentials
from pprint import pprint

WORKFLOW_OUTPUTS_DICT = {
    "Arrays.chip_well_barcode_output": "chip_well_barcode_output",
    "Arrays.analysis_version_number_output": "analysis_version_number_output",
    "Arrays.gtc_file": "gtc_file",
    "Arrays.arrays_variant_calling_control_metrics_file": "arrays_variant_calling_control_metrics_file",
    "Arrays.arrays_variant_calling_detail_metrics_file": "arrays_variant_calling_detail_metrics_file",
    "Arrays.arrays_variant_calling_summary_metrics_file": "arrays_variant_calling_summary_metrics_file",
    "Arrays.baf_regress_metrics_file": "baf_regress_metrics_file",
    "Arrays.fingerprint_detail_metrics_file": "fingerprint_detail_metrics_file",
    "Arrays.fingerprint_summary_metrics_file": "fingerprint_summary_metrics_file",
    "Arrays.genotype_concordance_contingency_metrics_file": "genotype_concordance_contingency_metrics_file",
    "Arrays.genotype_concordance_detail_metrics_file": "genotype_concordance_detail_metrics_file",
    "Arrays.genotype_concordance_summary_metrics_file": "genotype_concordance_summary_metrics_file",
    "Arrays.last_modified_date": "last_modified_date",
    "Arrays.output_vcf": "output_vcf",
    "Arrays.output_vcf_index": "output_vcf_index"
}

def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def check_user():
    
    uri = f"https://data.terra.bio/api/repository/v1/register/user"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}
    
    response = requests.get(uri, headers=headers)
    status_code = response.status_code
    
    if status_code != 200:
        print(f"Check user request failed")
        pprint(response.text)
        return

    print(f"Successfully retrieved user info.")
    return json.loads(response.text)


def get_dataset_info(dataset_id):
    """"Get dataset details from retrieveDataset API given a datasetID."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}?include=SCHEMA%2CPROFILE%2CDATA_PROJECT%2CSTORAGE"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}
    
    response = requests.get(uri, headers=headers)
    status_code = response.status_code
    
    if status_code != 200:
        return response.text
    
    print(f"Successfully retrieved details for dataset with datasetID {dataset_id}.")
    return json.loads(response.text)


def get_dataset_access_info(dataset_id):
    """"Get dataset access details from retrieveDataset API given a datasetID."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}?include=ACCESS_INFORMATION"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        return response.text

    print(f"Successfully retrieved access information for dataset with datasetID {dataset_id}.")
    return json.loads(response.text)


def get_snapshot_access_info(snapshot_id):
    uri = f"https://data.terra.bio/api/repository/v1/snapshots/{snapshot_id}?include=ACCESS_INFORMATION"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        return response.text

    print(f"Successfully retrieved snapshot access information for snapshotID {snapshot_id}.")
    return json.loads(response.text)


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

    print(f"Successfully retrieved access information for snapshot with snapshotID {snapshot_id}.")
    return json.loads(response.text)


# s.c recoding json function expanded to handle all data types as well as array type columns
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

                # if value is string but not a gs:// path or list of gs:// paths
                recoded_row_json[key] = value

    return recoded_row_json


def get_job_status_and_result(job_id):
    # first check job status
    uri = f"https://data.terra.bio/api/repository/v1/jobs/{job_id}"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}
    
    response = requests.get(uri, headers=headers)
    status_code = response.status_code
    
    if status_code != 200:
        return response.json()
    
    job_status = response.json()['job_status']
    print(f'job_id {job_id} has status {job_status}')
    # if job status = done, check job result
    if job_status in ['succeeded', 'failed']:
        print('retrieving job result')
        response = requests.get(uri + "/result", headers=headers)
        status_code = response.status_code

    return json.loads(response.text)


def get_fq_table(entity_id, table_name, entity_type):
    """Given a datset or snapshot id, table name, and entity type {dataset,snapshot}, retrieve its fully qualified BQ table name"""
    if entity_type == 'dataset':
        access_info = get_dataset_access_info(entity_id)
    elif entity_type == 'snapshot':
        access_info = get_snapshot_access_info(entity_id)

    project_id = access_info['accessInformation']['bigQuery']['projectId']
    tables = access_info['accessInformation']['bigQuery']['tables']

    # pull out desired table
    table_fq = None  # fq = fully qualified name, i.e. project.dataset.table
    for table_info in tables:
        if table_info['name'] == table_name:
            table_fq = table_info['qualifiedName'] 

    return table_fq


def get_single_attribute(fq_bq_table, datarepo_row_id, desired_field, gcp_project):
    """Performs a BQ lookup of a desired attribute in a specified snapshot or dataset table, for a specified datarepo_row_id"""
    
    # create BQ connection
    bq = bigquery.Client(gcp_project)
    
    # execute BQ query
    # datarepo_row_id_list_string = "('" + "','".join(datarepo_row_id_list) + "')"
    query = f"""SELECT datarepo_row_id, {desired_field} FROM `{fq_bq_table}` WHERE datarepo_row_id = '{datarepo_row_id}'"""
    executed_query = bq.query(query)
    
    result = executed_query.result()
    
    df_result = result.to_dataframe().set_index('datarepo_row_id')
    
    return df_result[desired_field][datarepo_row_id]


def get_all_attributes(fq_bq_table, datarepo_row_id, gcp_project):
    """Performs a BQ lookup of all attributes in the specified snapshot or dataset table"""
    # create BQ connection
    bq = bigquery.Client(gcp_project)
    
    # execute BQ query
    query = f"""SELECT * FROM `{fq_bq_table}` WHERE datarepo_row_id = '{datarepo_row_id}'"""
    executed_query = bq.query(query)
    
    result = executed_query.result()
    
    df_result = result.to_dataframe().set_index('datarepo_row_id')
    
    json_result = convert_df_to_json(df_result)
    
    return json_result


def call_ingest_dataset(control_file_path, target_table_name, dataset_id):
    """Create the ingestDataset API json request body and call API."""

    load_json = json.dumps({"format": "json",
                            "path": control_file_path,
                            "table": target_table_name
                            })

    load_job_response = load_data(dataset_id, load_json)

    job_id = load_job_response["id"]
    job_status = load_job_response["job_status"]

    print("Starting data ingest to complete.")
    while job_status == "running":
        time.sleep(10)
        response = get_job_status_and_result(job_id)

        # waiting for the status of the job to change from running (done could also mean failed)
        if "job_status" in response.keys():
            job_status = response["job_status"]
            print("Load job is still running.")
        # determine if done = failed or done = succeeded
        else:
            failed_files = response["load_result"]["loadSummary"]["failedFiles"]
            succeeded_files = response["load_result"]["loadSummary"]["succeededFiles"]
            total_files = response["load_result"]["loadSummary"]["totalFiles"]
            # if not success
            if total_files != succeeded_files:
                print(f"Total files to load count: {total_files}")
                print(f"Successfully loaded file count: {succeeded_files}")
                print(f"Failed to load file count: {failed_files}")
                print(f"Full error for more details: {response}")
                return
            # if success, mark as done
            job_status = "success"

    print("File ingest to TDR dataset completed.")


def write_load_json_to_bucket(submission_id, bucket, recoded_rows_json):
    """Write the list of recoded rows (list of dictionaries) to a file and copy to workspace bucket."""

    loading_json_filename = f"TESTING_arrays_sub-{submission_id}_recoded_ingestDataset.json"
    # write load json to the workspace bucket
    control_file_destination = f"{bucket}/emerge_prod_test_dataset"

    with open(loading_json_filename, 'w') as final_newline_json:
        for r in recoded_rows_json:
            json.dump(r, final_newline_json)
            final_newline_json.write('\n')


    storage_client = gcs.Client()
    dest_bucket = storage_client.get_bucket(bucket)

    blob = dest_bucket.blob(f"emerge_prod_test_dataset/{loading_json_filename}")
    blob.upload_from_filename(loading_json_filename)

    print(f"Successfully copied {loading_json_filename} to {control_file_destination}.")
    return f"gs://{control_file_destination}/{loading_json_filename}"


def create_recoded_json_list(project, workspace, submission_id, snapshot_sample_table_fq, gcp_project, workflow_name):
    """Create a list of recoded jsons where each item represents a workflow's outputs."""

    # empty list to collect per-row recoded json requests to ingest dataset
    all_rows_recoded_data_to_upload = []
    # empty dictionary to hold single row outputs
    single_row_data_to_upload = {}
    # generate timestamp for last_modified_column --> current datetime in UTC
    last_modified_date = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")

    # this is the desired format of data_to_upload (specific format for reblocking wdl):
    # data_to_upload = {
    #     "sample_id": None,
    #     "reblocked_gvcf_path": None,
    #     "reblocked_gvcf_index_path": None
    # }

    for datarepo_row_id, workflow_id in workflows.items():
        # retrieve chip_well_barcode from snapshot data
        chip_well_barcode = get_single_attribute(snapshot_sample_table_fq, datarepo_row_id, 'chip_well_barcode', gcp_project)
        single_row_data_to_upload['chip_well_barcode'] = chip_well_barcode

        # get reblocked gvcf & index paths
        workflow_outputs_json = fapi.get_workflow_outputs(project, workspace, submission_id, workflow_id).json()
        workflow_outputs = workflow_outputs_json['tasks'][workflow_name]['outputs']
        
        # pull out all the desired workflow outputs (defined in workflow_outputs_dict) and save them in data_to_upload
        for output_name, output_value in workflow_outputs.items():
            if output_name in WORKFLOW_OUTPUTS_DICT.keys():
                single_row_data_to_upload[WORKFLOW_OUTPUTS_DICT[output_name]] = output_value
        
        # add timestamp to single_row_data_to_upload before recoding for ingestDataset API call
        single_row_data_to_upload["last_modified_date"] = last_modified_date
        # recode the single row for ingestDataset API call
        single_row_recoded_ingest_json = create_recoded_json(single_row_data_to_upload)
        # add recoded single row to list of all recoded rows
        all_rows_recoded_data_to_upload.append(single_row_recoded_ingest_json)

    # print(all_rows_recoded_data_to_upload)
    return all_rows_recoded_data_to_upload


def gather_bq_table_info(snapshot_id, dataset_id):
    """Gather BQ table information"""

    # TODO/question: should we just always query the underlying dataset?
    # TODO: will these two tables ever be different
    snapshot_input_table = "ArraysInputsTable"
    dataset_input_table = "ArraysInputsTable"

    # for the snapshot (for this sample's info)
    snapshot_sample_table_fq = get_fq_table(snapshot_id, snapshot_input_table, 'snapshot')
    print(f'SNAPSHOT {snapshot_input_table} table: {snapshot_sample_table_fq}')

    # and for the underlying dataset (for updates)
    # retrieve existing data for row to update
    dataset_sample_table_fq = get_fq_table(dataset_id, dataset_input_table, 'dataset')
    print(f'DATASET {dataset_input_table} table: {dataset_sample_table_fq}')

    return snapshot_sample_table_fq, dataset_sample_table_fq


def extract_submission_outputs(project, workspace, submission_id):
    """Extract information from the submission whose outputs you're importing back to TDR"""

    sub_info = fapi.get_submission(project, workspace, submission_id).json()
    snapshot_id = sub_info['externalEntityInfo']['dataStoreId']
    print(f"The Arrays.wdl submission [{submission_id}] was run with inputs from snapshot ID [{snapshot_id}]")

    # find all successful workflows
    # NOTE: this assumes you're going to have exactly one successful workflow per sample, so...
    # TODO: build in some edge case handling here
    workflows = {}  # format will be {datarepo_row_id: workflow_id}
    for workflow in sub_info['workflows']:
        if workflow['status'] != 'Succeeded':
            continue
        datarepo_row_id = workflow['workflowEntity']['entityName']
        workflows[datarepo_row_id] = workflow['workflowId']

    # for key, value in workflows.items():
    #     print(f'datarepo_row_id {key}: workflow_id {value}')

    return snapshot_id, workflows


if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='Push Arrays.wdl outputs to TDR dataset.')

    parser.add_argument('-s', '--submission_id', required=True, type=str, help='id of submission from which to gather outputs')
    parser.add_argument('-p', '--project', required=True, type=str, help='workspace namespace/project')
    parser.add_argument('-w', '--workspace', required=True, type=str, help='workspace name')
    parser.add_argument('-b', '--bucket', required=True, type=str, help='workspace bucket')
    parser.add_argument('-g', '--gcp_project', required=True, type=str, help='gcp project for BQ')
    parser.add_argument('-n', '--workflow_name', required=True, type=str, help='name of WDL')
    parser.add_argument('-d', '--dataset_id', required=True, type=str, help='id of TDR dataset for destination of outputs')
    parser.add_argument('-t', '--target_table_name', required=True, type=str, help='name of target table in TDR dataset')

    # workflow outputs from Arrays.WDL

    args = parser.parse_args()

    snapshot_id, workflows = extract_submission_outputs(args.project, args.workspace, args.submission_id)
    snapshot_sample_table_fq, dataset_sample_table_fq = gather_bq_table_info(snapshot_id, args.dataset_id)
    recoded_upload_json = create_recoded_json_list(args.project, args.workspace, args.submission_id, snapshot_sample_table_fq, args.gcp_project, args.workflow_name)
    control_file_path = write_load_json_to_bucket(args.submission_id, args.bucket, recoded_upload_json)
    ingest_dataset_request = call_ingest_dataset(control_file_path, args.target_table_name, args.dataset_id)

# python3 arrays_wdl_outputs_to_TDR.py -s 9159f7fb-9bb4-49a7-a822-4e916b260677 -p emerge_prod -w Arrays_test 
# -b fc-e036248e-067b-4365-8c1e-5eb25103681f -g terra-e97dc6ac -n Arrays -d 6f2bb559-34ae-4ba0-b2ce-1d8be76ada9f -t ArraysOutputsTable