import argparse
import json
import requests
import google.auth as googleauth
from firecloud import api as fapi
from time import sleep
from pprint import pprint


def get_access_token():
    """Get access token."""
    creds, _ = googleauth.default()
    auth_req = googleauth.transport.requests.Request()
    creds.refresh(auth_req)

    return creds.token

def get_headers(request_type='get'):
    headers = {"Authorization": "Bearer " + get_access_token(),
                "accept": "application/json"}
    if request_type == 'post':
        headers["Content-Type"] = "application/json"
    return headers



def retrieve_WFL_workloads(wfl_project):
    """Retrieve all workloads, filtered for wfl_project.
    Return a list of jsons."""

    # replace / with %2 for use in API URI
    safe_project_name = wfl_project.replace("/", "%2")

    # uri = f"https://gotc-prod-wfl.gotc-prod.broadinstitute.org/api/v1/workload?project={safe_project_name}"
    # that filtering doesn't actually work
    uri = f"https://gotc-prod-wfl.gotc-prod.broadinstitute.org/api/v1/workload"

    response = requests.get(uri, headers=get_headers())

    if response.status_code != 200:
        print(response.status_code)
        print(response.text)
    
    response_json = response.json()

    filtered_json = []

    for workload in response_json:
        if "labels" in workload:
            if f"project:{wfl_project}" in workload["labels"]:
                filtered_json.append(workload)

    return filtered_json


def show_active_workloads(workloads):
    """Given a list of workloads, print the WFL uuid and source TDR dataset name and uuid."""

    print("\nActive workloads, by TDR dataset:")

    for workload in workloads:
        if not "stopped" in workload:
            uuid = workload["uuid"]
            tdr_dataset_name = [x.replace('dataset:', '') for x in workload['labels'] if 'dataset' in x][0]
            start_time = workload["started"]
            target_workflow = workload["executor"]["methodConfiguration"]

            print(f"""
{tdr_dataset_name}
    WFL uuid : {uuid}
    started  : {start_time}
    workflow : {target_workflow}""")


def main(wfl_project):
    # retrieve all WFL workloads, filtered to the project
    filtered_workloads = retrieve_WFL_workloads(wfl_project)

    print(f"found {len(filtered_workloads)} total workloads for {wfl_project}")

    # filter for only running workloads and return info
    show_active_workloads(filtered_workloads)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest workflow outputs to TDR')
    parser.add_argument('-p', '--wfl_project',
        default='tcap-twist-wfl/TCap_Twist_WFL_Processing',
        help='WFL project identifier to search for')

    args = parser.parse_args()

    main(args.wfl_project)
