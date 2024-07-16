"""Common TDR-related functions used for TDR scripts."""

import json
import requests

from google.cloud import bigquery
from pprint import pprint
from time import sleep

from utils import get_access_token, get_headers, convert_df_to_json


### Functions to access TDR data from BigQuery ###

def get_fq_table(entity_id, table_name, entity_type='dataset'):
    """Given a datset or snapshot id, table name, and entity type {dataset,snapshot}, retrieve its fully qualified BQ table name"""
    if entity_type == 'dataset':
        access_info = get_dataset_access_info(entity_id)
    elif entity_type == 'snapshot':
        access_info = get_snapshot_access_info(entity_id)

    # TODO error handling in case you gave a bad value for entity_id
    tables = access_info['accessInformation']['bigQuery']['tables']

    # pull out desired table
    table_fq = None  # fq = fully qualified name, i.e. project.dataset.table
    for table_info in tables:
        if table_info['name'] == table_name:
            table_fq = table_info['qualifiedName']

    return table_fq


def get_snapshot_access_info(snapshot_id):
    """Get snapshot access information from retrieveSnapshot API given a snapshotID"""

    uri = f"https://data.terra.bio/api/repository/v1/snapshots/{snapshot_id}?include=ACCESS_INFORMATION"

    response = requests.get(uri, headers=get_headers())
    status_code = response.status_code

    if status_code != 200:
        return response.text

    print(f"Successfully retrieved access information for snapshot with snapshotID {snapshot_id}.")
    return json.loads(response.text)


def get_dataset_access_info(dataset_id):
    """"Get dataset access details from retrieveDataset API given a datasetID."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}?include=ACCESS_INFORMATION"

    response = requests.get(uri, headers=get_headers())
    status_code = response.status_code

    if status_code != 200:
        return response.text

    print(f"Successfully retrieved access information for dataset with datasetID {dataset_id}.")
    return json.loads(response.text)


def get_single_attribute(fq_bq_table, bq_project, datarepo_row_id, desired_field):
    """Performs a BQ lookup of a desired attribute in a specified snapshot or dataset table, for a specified datarepo_row_id"""

    # create BQ connection
    bq = bigquery.Client(bq_project)

    # execute BQ query
#     datarepo_row_id_list_string = "('" + "','".join(datarepo_row_id_list) + "')"
    query = f"""SELECT datarepo_row_id, {desired_field} FROM `{fq_bq_table}` WHERE datarepo_row_id = '{datarepo_row_id}'"""
    executed_query = bq.query(query)

    result = executed_query.result()

    df_result = result.to_dataframe().set_index('datarepo_row_id')

    return df_result[desired_field][datarepo_row_id]


def format_output(df_result, fmt):
    if fmt == 'dataframe':
        return df_result
    elif fmt == 'json':
        # df_result = df_result.set_index('datarepo_row_id')
        json_result = convert_df_to_json(df_result)
        return json_result
    else:
        print(f'unrecognized format request: fmt = {fmt}')
        return None


def get_all_attributes_by_sample_id(fq_bq_table, gcp_project, sample_id_list, fmt='dataframe'):
    """Performs a BQ lookup of all attributes in the specified snapshot or dataset table
    for a list of sample_ids
    """
    # create BQ connection
    bq = bigquery.Client(gcp_project)

    # execute BQ query
    where_clause = "WHERE sample_id IN ('" + "','".join(sample_id_list) + "')"

    query = f"""SELECT * FROM `{fq_bq_table}` {where_clause}"""
    executed_query = bq.query(query)

    result = executed_query.result()

    df_result = result.to_dataframe()

    print(f'retrieved {len(df_result)} rows')

    return format_output(df_result, fmt)


def get_all_attributes(fq_bq_table, gcp_project, datarepo_row_id_list=None, fmt='dataframe'):
    """Performs a BQ lookup of all attributes in the specified snapshot or dataset table
    for a list of datarepo_row_ids, or for all rows if no list of ids is provided
    """
    # create BQ connection
    bq = bigquery.Client(gcp_project)

    # execute BQ query
    if datarepo_row_id_list:
        where_clause = "WHERE datarepo_row_id IN ('" + "','".join(datarepo_row_id_list) + "')"
    else:
        where_clause = ""
    query = f"""SELECT * FROM `{fq_bq_table}` {where_clause}"""
    executed_query = bq.query(query)

    result = executed_query.result()

    df_result = result.to_dataframe()

    print(f'retrieved {len(df_result)} rows')

    return format_output(df_result, fmt)


### Functions for interacting with TDR APIs

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
        return response.text

    job_status = response.json()['job_status']
    print(f'job_id {job_id} has status {job_status}')
    # if job status = done, check job result
    if job_status in ['succeeded', 'failed']:
        print('retrieving job result')
        response = requests.get(uri + "/result", headers=headers)
        status_code = response.status_code

    return job_status, response.json()


def share_snapshot(snapshot_id, email, permission_level='reader'):
    """Share a snapshot with an email or group"""

    uri = f"https://data.terra.bio/api/repository/v1/snapshots/{snapshot_id}/policies/{permission_level}/members"

    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json",
               "Content-Type": "application/json"}

    body = json.dumps({
        "email": email
    })

    response = requests.post(uri, headers=headers, data=body)
    status_code = response.status_code

    if status_code != 200:
        print('error sharing snapshot')
        print(response.text)
        exit(1)

    print(f"Successfully granted {permission_level} access for snapshotID {snapshot_id} to {email}.")
    return json.loads(response.text)


def load_data(dataset_id, ingest_data):
    """Load data into TDR"""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/ingest"

    response = requests.post(uri, headers=get_headers('post'), data=ingest_data)
    status_code = response.status_code

    if status_code != 202:
        return response.text

    return json.loads(response.text)


def call_ingest_dataset(control_file_path, target_table_name, dataset_id):
    """Create the ingestDataset API json request body and call API."""

    load_json = json.dumps({"format": "json",
                            "path": control_file_path,
                            "table": target_table_name,
                            "resolve_existing_files": True,
                            })

    load_job_response = load_data(dataset_id, load_json)

    print(f"Load Job Response: {load_job_response}")

    job_id = load_job_response["id"]

    job_status, response = wait_for_job_status_and_result(job_id)

    if job_status == "succeeded":
        print("File ingest to TDR dataset completed.")
        return response
    else:
        pprint(response)
        exit(1)


### Misc functions

def recode_json_with_filepaths(json_to_load_list):
    """Takes a list of dicts, transforms files for upload as needed for TDR ingest, returns list of updated dicts."""
    recoded_json_to_load_list = []

    for json_object in json_to_load_list:
        for k in json_object.keys():
            v = json_object[k]

            if v is not None and "gs://" in v:
                tdr_path = v.replace("gs://", "/")

                # update json
                json_object[k] = {
                    "sourcePath": v,
                    "targetPath": tdr_path
                }
        recoded_json_to_load_list.append(json_object)

    return recoded_json_to_load_list


def resolve_drs(input_value):
    # don't do anything if this isn't a drs_url
    if not isinstance(input_value, str):
        return input_value
    if not input_value.startswith('drs://'):
        return input_value

    # we know that input_value is a drs_url
    # print(f"attempting to resolve {input_value}")
    # TODO explore whether using the jade endpoint to retrieve gs paths is better than this
    uri = f"https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"

    body = json.dumps({
        "url": input_value
    })

    response = requests.post(uri, headers=get_headers('post'), data=body)
    # status_code = response.status_code

    if response.status_code != 200:
        print(response.text)

    # this will error if response is bad, it won't be pretty but that's what we want
    return response.json()['gsUri']