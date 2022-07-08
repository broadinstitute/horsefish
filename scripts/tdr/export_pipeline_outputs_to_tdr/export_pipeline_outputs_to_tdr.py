import argparse
import datetime
import json
import requests
import tenacity as tn
from oauth2client.client import GoogleCredentials
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


def main(dataset_id, target_table, outputs_json, timestamp_field_list = []):
    # read workflow outputs from file
    print(f"reading data from outputs_json file {outputs_json}")
    with open(outputs_json, "r") as infile:
        outputs_dict = json.load(infile)

    # recode any paths (files) for TDR ingest
    print("recoding paths for TDR ingest")
    outputs_to_add = recode_json_with_filepaths(outputs_dict)

    # update version_timestamp field
    if timestamp_field_list:
        new_version_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        for field_name in timestamp_field_list:
            outputs_to_add[field_name] = new_version_timestamp

    # ingest data to TDR
    load_json = json.dumps({"format": "array",
                        "records": [outputs_to_add],
                        "table": target_table,
                        "resolve_existing_files": True,
                        "updateStrategy": "merge"
                        })
    ingest_data(dataset_id, load_json)

    print(f"Ingest to dataset {dataset_id} complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest workflow outputs to TDR')
    parser.add_argument('-d', '--dataset_id', required=True,
        help='UUID of destination TDR dataset')
    parser.add_argument('-t', '--target_table', required=True,
        help='name of destination table in the TDR dataset')
    parser.add_argument('-o', '--outputs_json', required=True,
        help='path to a json file defining the outputs to be loaded to TDR')
    parser.add_argument('-f', '--timestamp_field_list', action='append',
        help='field that should be populated with timestamp at ingest time (can have more than one)')

    args = parser.parse_args()

    main(args.dataset_id,
         args.target_table,
         args.outputs_json,
         args.timestamp_field_list)
