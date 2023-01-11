import argparse
import datetime
import json
import requests
import tenacity as tn
import warnings

from firecloud import api as fapi
from google.cloud import bigquery
from oauth2client.client import GoogleCredentials
from pprint import pprint
from time import sleep

# DEVELOPER: update this field anytime you make a new docker image
docker_version = "1.0"

# supress that annoying message about using ADC from google
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


# define some utils functions
def my_before_sleep(retry_state):
    """Print a status update before a retry."""
    print('Retrying %s with %s in %s seconds; attempt #%s ended with: %s',
        retry_state.fn, retry_state.args, str(int(retry_state.next_action.sleep)), retry_state.attempt_number, retry_state.outcome)

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
    print(f'found {len(sample_list)} {statuses_string} workflows')

    return sample_list


def get_dataset_name(dataset_id):
    """Given the uuid of a TDR dataset, return its name."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}?include=NONE"

    response = requests.get(uri, headers=get_headers())

    if response.status_code != 200:
        raise ValueError(f"Failed to retrieve dataset name, response code {response.status_code}, text: {response.text}")
    
    return response.json()["name"]

def get_dataset_access_info(dataset_id):
    """"Get dataset access details from retrieveDataset API given a datasetID."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}?include=ACCESS_INFORMATION"

    response = requests.get(uri, headers=get_headers())
    status_code = response.status_code

    if status_code != 200:
        return response.text

    print(f"Successfully retrieved access information for dataset with datasetID {dataset_id}.")
    return json.loads(response.text)


def get_fq_table(entity_id, table_name):
    """Given a datset or snapshot id, table name, and entity type {dataset,snapshot}, retrieve its fully qualified BQ table name"""
    access_info = get_dataset_access_info(entity_id)

    tables = access_info['accessInformation']['bigQuery']['tables']

    # pull out desired table
    table_fq = None  # fq = fully qualified name, i.e. project.dataset.table
    for table_info in tables:
        if table_info['name'] == table_name:
            table_fq = table_info['qualifiedName']

    return table_fq

def get_datarepo_row_ids_by_pk(fq_bq_table, key, values):
    """Performs a BQ lookup of the datarepo_row_id, given a key-values input"""

    bq_project = fq_bq_table.split(".")[0]
    values_list = "('" + "','".join(values) + "')"

    # create BQ connection
    bq = bigquery.Client(bq_project)

    # execute BQ query
    query = f"""SELECT datarepo_row_id FROM `{fq_bq_table}` WHERE {key} IN {values_list}"""
    executed_query = bq.query(query)

    result = executed_query.result()

    datarepo_row_ids = []
    for row in result:
        datarepo_row_ids.append((row[0]))

    if len(datarepo_row_ids) != len(values):
        msg = f"Retrieved {len(datarepo_row_ids)} datarepo_row_ids for the {len(values)} searched values. Check parameters. The query executed was {query}"
        raise ValueError(msg)

    return datarepo_row_ids


def get_all_columns(fq_bq_table):
    (bq_project, dataset, table) = fq_bq_table.split(".")

    # create BQ connection
    bq = bigquery.Client(bq_project)

    # execute BQ query
    query = f"""SELECT column_name FROM `{bq_project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table}'"""
    executed_query = bq.query(query)

    result = executed_query.result()

    all_columns = []
    for row in result:
        all_columns.append((row[0]))

    return all_columns


def create_snapshot_json(name, dataset_id, description, table, pk_key, pk_list, snapshot_readers_list):
    dataset_name = get_dataset_name(dataset_id)

    fq_table = get_fq_table(dataset_id, table)

    all_columns = get_all_columns(fq_table)
    datarepo_row_ids = get_datarepo_row_ids_by_pk(fq_table, pk_key, pk_list)

    create_snapshot_dict = {
        "name": name.replace("-", "_"),
        "description": description,
        "contents": [
            {
                "datasetName": dataset_name,
                "mode": "byRowId",
                "rowIdSpec": {
                    "tables": [
                    {
                        "tableName": table,
                        "columns": all_columns,
                        "rowIds": datarepo_row_ids
                    }
                    ]
                }
            }
        ],
        "policies": {
            "readers": snapshot_readers_list
        }
    }

    pprint(create_snapshot_dict)

    return json.dumps(create_snapshot_dict)

# retry once if create_snapshot fails
@tn.retry(wait=tn.wait_fixed(10),
          stop=tn.stop_after_attempt(1),
          before_sleep=my_before_sleep)
def create_snapshot(snapshot_json):
    uri = f'https://data.terra.bio/api/repository/v1/snapshots'

    response = requests.post(uri, headers=get_headers('post'), data=snapshot_json)
    status_code = response.status_code
    if status_code != 202:
        error_msg = f"Error with snapshot creation: {response.text}"
        raise ValueError(error_msg)

    load_job_id = response.json()['id']
    job_status, job_info = wait_for_job_status_and_result(load_job_id)
    if job_status != "succeeded":
        print(f"job status {job_status}:")
        message = job_info["message"]
        detail = job_info["errorDetail"]
        error_msg = f"{message}: {detail}"
        raise ValueError(error_msg)
    
    return job_info


def export_snapshot_by_reference(ws_project, ws_name, snapshot_id, snapshot_name, snapshot_description):
    """Export a TDR snapshot into a Terra workspace, using snapshot by reference"""

    uri = f'https://rawls.dsde-prod.broadinstitute.org/api/workspaces/{ws_project}/{ws_name}/snapshots/v2'

    snapshot_export_json = json.dumps({
        "snapshotId": snapshot_id,
        "name": snapshot_name,
        "description": snapshot_description
    })

    response = requests.post(uri, headers=get_headers('post'), data=snapshot_export_json)
    status_code = response.status_code
    if status_code != 201:
        error_msg = f"Error with snapshot export: {response.text}"
        raise ValueError(error_msg)
    
    print(f'Export of TDR snapshot {snapshot_name} (id {snapshot_id}) into Terra workspace {ws_project}/{ws_name} successfully kicked off')


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


def main(submission_id, dataset_id, table, pk_key, ws_project, ws_name, snapshot_readers, include_aborted=False):
    # gather submission failures
    # note that we assume that the primary key in Terra is the primary key in TDR
    failed_samples = get_submission_failures(ws_project, ws_name, submission_id, include_aborted)

    # create snapshot from the list of rows
    dataset_name = get_dataset_name(dataset_id)
    snapshot_name = f"{dataset_name}_failures_from_{submission_id}".replace("-", "_")
    description = f"{len(failed_samples)} failed sample(s) from Terra submission {submission_id}"
    snapshot_json = create_snapshot_json(snapshot_name, dataset_id, description, table, pk_key, failed_samples, snapshot_readers)

    snapshot_response = create_snapshot(snapshot_json)

    export_snapshot_by_reference(ws_project, ws_name, snapshot_response['id'], snapshot_name, description)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create new snapshot from rows that failed a Terra workflow submission')
    
    parser.add_argument('-s', '--submission_id', required=True,
        help='UUID of the Terra submission containing failures')
    parser.add_argument('-d', '--dataset_id', required=True,
        help='UUID of source TDR dataset')
    parser.add_argument('-t', '--table', required=True,
        help='name of source table in the TDR dataset')
    parser.add_argument('-k', '--pk_key', default='sample_id',
        help='Primary key of the TDR dataset table, defaults to sample_id')
    parser.add_argument('-p', '--ws_project', required=True,
        help='Workspace project/namespace')
    parser.add_argument('-w', '--ws_name', required=True,
        help='Workspace name')
    parser.add_argument('-a', '--include_aborted', action='store_true',
        help='Include aborted workflows')
    parser.add_argument('-r', '--readers', nargs='+', required=False,
        help='Email address(es) (separated by spaces if multiple) to add as snapshot reader(s)')

    args = parser.parse_args()

    main(args.submission_id,
         args.dataset_id,
         args.table,
         args.pk_key,
         args.ws_project,
         args.ws_name,
         args.readers,
         args.include_aborted)
