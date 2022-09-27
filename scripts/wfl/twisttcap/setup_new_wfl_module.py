import argparse
import datetime
import json
import requests
import tenacity as tn
from oauth2client.client import GoogleCredentials
from firecloud import api as fapi
from time import sleep

# DEVELOPER: update this field anytime you make a new docker image
docker_version = "1.4"


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

# retry once if ingest_data fails
@tn.retry(wait=tn.wait_fixed(10),
          stop=tn.stop_after_attempt(1),
          before_sleep=my_before_sleep)
def ingest_data(dataset_id, load_json):
    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/ingest"
    response = requests.post(uri, headers=get_headers('post'), data=load_json)
    status_code = response.status_code
    if status_code != 202:
        error_msg = f"Error with ingest: {response.text}"
        raise ValueError(error_msg)

    load_job_id = response.json()['id']
    job_status, job_info = wait_for_job_status_and_result(load_job_id)
    if job_status != "succeeded":
        print(f"job status {job_status}:")
        message = job_info["message"]
        detail = job_info["errorDetail"]
        error_msg = f"{message}: {detail}"
        raise ValueError(error_msg)

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


# twist-tcap workspace & workflow info. could be made into a config in future.
WORKSPACE_NAME = "TCap_Twist_WFL_Processing"
WORKSPACE_NAMESPACE = "tcap-twist-wfl"
WORKFLOW_NAME = "BroadInternalRNAWithUMIs"
WORKFLOW_NAMESPACE = "tcap-twist-wfl"


def configure_workflow(WORKSPACE_NAME, WORKSPACE_NAMESPACE, WORKFLOW_NAME, WORKFLOW_NAMESPACE, dataset_id):
    """Create a copy of the desired workflow in the same workspace, with the dataset_id as suffix.
    Return the dataset-specific copied_workflow_name."""
    # check to see whether the new workflow exists already
    copied_workflow_name = f"{WORKFLOW_NAME}_{dataset_id}"

    response = fapi.get_workspace_config(WORKSPACE_NAMESPACE, WORKSPACE_NAME, WORKFLOW_NAMESPACE, copied_workflow_name)
    if response.status_code == 200:
        print(f"WARNING: Dataset-specific workflow {copied_workflow_name} already exists.")
    elif response.status_code == 404:
        # make a copy of the base workflow
        # TODO return here
        pass
    else:
        print(f"Unexpected response code {response.status_code}")
        raise ValueError()




    return copied_workflow_name


def main(dataset_id):
    # make a copy of the workflow with _<tdr_dataset_uuid> suffix
    copied_workflow_name = configure_workflow(WORKSPACE_NAME, WORKSPACE_NAMESPACE, WORKFLOW_NAME, WORKFLOW_NAMESPACE, dataset_id)

    # set up the config of the new workflow with the tdr_dataset_uuid input set correctly

    # call the create WFL module API and then the activate WFL module API (or use the one that does both?)

    # return the WFL module info in some human readable form
    # recode any paths (files) for TDR ingest



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest workflow outputs to TDR')
    parser.add_argument('-d', '--dataset_id', required=True,
        help='UUID of source TDR dataset')

    args = parser.parse_args()

    main(args.dataset_id)
