"""Put/update user/group permissions to workspace - parsed from input tsv file.

Usage:
    > python3 add_user_to_workspace.py -t TSV_FILE [-p BILLING-PROJECT]"""

import argparse
import pandas as pd
import requests

from oauth2client.client import GoogleCredentials


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def call_updateWorkspaceACL_api(request, workspace_name, workspace_project, email):
    """PUT request to the putLibraryMetadata API."""

    # request URL for updateWorkspaceACL
    uri = f"https://api.firecloud.org/api/workspaces/{workspace_project}/{workspace_name}/acl?inviteUsersNotFound=false"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"

    # capture response from API and parse out status code
    response = requests.patch(uri, headers=headers, data=request)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code == 200:
        print(f"Successfully added/updated {workspace_project}/{workspace_name} with user/group {email}.")

    else:
        print(f"WARNING: Failed to add/update user/group to {workspace_project}/{workspace_name}")
        print("Please see full response for error:")
        print(response.text)


def add_workspace_user(tsv):
    """Create individual request body per workspace and user/group listed in tsv file."""

    # read full tsv into dataframe, workspace name = index
    tsv_all = pd.read_csv(tsv, sep="\t")
    # tsv_modified['canShare'] = np.where(tsv_all['accessLevel'] == 'READER', False, True)
    tsv_all['canShare'] = [False if x == 'READER' else True for x in tsv_all['accessLevel']]
    tsv_all['canCompute'] = [False if x == 'READER' else True for x in tsv_all['accessLevel']]

    # make json request for each workspace and call API
    for row in tsv_all.index:
        # get workspace name from row (Series)
        workspace_name = tsv_all.loc[row].get(key='workspace_name')
        workspace_project = tsv_all.loc[row].get(key='workspace_project')
        email = tsv_all.loc[row].get(key='email')
        # create json request (remove workspace_name)
        row_json_request = "[" + tsv_all.loc[row].drop(labels='workspace_name').to_json() + "]"

        # call to API call with request and workspace name
        call_updateWorkspaceACL_api(row_json_request, workspace_name, workspace_project, email)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with workspace and matching authdomains.')

    args = parser.parse_args()

    # call to create request body PER row and make API call to update attributes
    add_workspace_user(args.tsv)
