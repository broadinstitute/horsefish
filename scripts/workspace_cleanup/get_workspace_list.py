#! /usr/bin/env python
"""Generate a csv of workspaces in a project, along with their creators/owners and a link to the workspace."""

import argparse
import requests

from oauth2client.client import GoogleCredentials
from time import sleep

from firecloud import api as fapi


def get_access_token():
    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def get_storage_cost_estimate(workspace, project, access_token, retry=0):
    # don't retry too much
    if retry > 3:
        print(f"WARNING: internal errors not resolved by retries. storage cost for {project}/{workspace} was NOT retrieved.")
        print("Exiting.")
        exit(1)

    sleep_time = 5  # seconds to wait between retries

    # try to retrieve the cost estimate
    try:
        uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace}/storageCostEstimate"
        # Get access token and and add to headers for requests.
        headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*"}
        #  -H  "accept: */*" -H  "Authorization: Bearer [token]"
        response = requests.get(uri, headers=headers)
        status_code = response.status_code
        if status_code == 200:
            cost_estimate = response.json()["estimate"]
            print(f"retrieved storage cost estimate for {project}/{workspace}: {cost_estimate}")
            return cost_estimate
        elif status_code == 403:
            print(f"WARNING: Insufficient permissions to estimate storage cost for {project}/{workspace}.")
            return None
        elif status_code == 404:
            print(f"404: workspace {project}/{workspace} not found.")
            return None
        elif status_code == 500:
            # try again
            incremented_retry = retry + 1
            print(f"Retrying storage cost estimate of {project}/{workspace} (attempt {incremented_retry}) after {sleep_time} seconds")
            sleep(sleep_time)
            return get_storage_cost_estimate(workspace, project, incremented_retry)
        else:
            print(f"Unknown status code: {status_code}. Exiting.")
            exit(1)
    except Exception:
        # try again
        incremented_retry = retry + 1
        print(f"Retrying storage cost estimate of {project}/{workspace} (attempt {incremented_retry}) after {sleep_time} seconds")
        sleep(sleep_time)
        return get_storage_cost_estimate(workspace, project, incremented_retry)


def export_workspaces(project, get_cost):
    # call list workspaces
    response = fapi.list_workspaces(fields="workspace.namespace,workspace.name,workspace.createdBy,workspace.createdDate")
    fapi._check_response_code(response, 200)
    all_workspaces = response.json()

    # limit the workspaces to the desired project
    workspaces = [ws['workspace'] for ws in all_workspaces if ws['workspace']['namespace'] == project]

    print(f"Found {len(workspaces)} workspaces in Terra project {project}")

    if get_cost:
        print(f"Retrieving workspace bucket cost estimates")
        ws_costs = {}
        for ws in workspaces:
            ws_costs[ws['name']] = get_storage_cost_estimate(ws['name'], project, get_access_token())

    # write to csv
    csv_name = f"Terra_workspaces_{project}.csv"
    with open(csv_name, "w") as csvout:
        if get_cost:
            # add header with attribute values to csv
            csvout.write("workspace,created by,storage cost estimate,date created,link\n")
            for ws in workspaces:
                name_for_link = ws['name'].replace(" ", "%20")
                csvout.write(f"{ws['name']},{ws['createdBy']},{ws_costs[ws['name']]},{ws['createdDate']},https://app.terra.bio/#workspaces/{project}/{name_for_link}\n")
        else:
            # add header with attribute values to csv
            csvout.write("workspace,created by,date created,link\n")
            for ws in workspaces:
                name_for_link = ws['name'].replace(" ", "%20")
                csvout.write(f"{ws['name']},{ws['createdBy']},{ws['createdDate']},https://app.terra.bio/#workspaces/{project}/{name_for_link}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--project', '-p', type=str, default='broad-firecloud-dsde', help='Terra project')
    parser.add_argument('--get_cost', '-c', action='store_true', help='retrieve estimate of workspace bucket cost')

    parser.add_argument('--verbose', '-v', action='store_true', help='print progress text')

    args = parser.parse_args()

    export_workspaces(args.project, args.get_cost)
