"""Create AnVIL workspaces, set up with auth domain, add workspace READER access to auth domain, and OWNER access to AnVIL admins.

Usage:
    > python3 set_up_anvil_workspace.py -t INPUT_TSV_FILE [-p BILLING-PROJECT] """

import argparse
import json
import pandas as pd
import requests

from oauth2client.client import GoogleCredentials


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def add_members_to_workspace(workspace_name, auth_domain_name, project="anvil_datastorage"):
    """Add members to workspace permissions."""

    acls = []
    # add auth domain as READER, anvil-admins as WRITER
    acls.append({'email': f'{auth_domain_name}@firecloud.org', 'accessLevel': 'READER', 'canShare': False, 'canCompute': False})
    acls.append({'email': 'anvil-admins@firecloud.org', 'accessLevel': 'WRITER', 'canShare': True, 'canCompute': True})

    json_request = json.dumps(acls)

    # request URL for updateWorkspaceACL
    uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace_name}/acl?inviteUsersNotFound=false"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"

    # capture response from API and parse out status code
    response = requests.patch(uri, headers=headers, data=json_request)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code == 200:
        print(f"Successfully added/updated {project}/{workspace_name} with user/group: {auth_domain_name}@firecloud.org.")

    else:
        print(f"WARNING: Failed to add/update {project}/{workspace_name} with user/group: {auth_domain_name}@firecloud.org")
        print("Please see full response for error:")
        print(response.text)


def create_workspace(json_request):
    """Create the Terra workspace with given auth domain."""

    # request URL for createWorkspace
    uri = f"https://api.firecloud.org/api/workspaces"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token] -H  "Content-Type: application/json"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json", "Content-Type": "application/json"}

    # capture response from API and parse out status code
    response = requests.post(uri, headers=headers, data=json.dumps(json_request))
    status_code = response.status_code

    workspace_name = json_request["name"]
    # print success or fail message based on status code
    if status_code == 201:
        print(f"Successfully created workspace with name: {workspace_name}.")

    else:
        print(f"WARNING: Failed to create workspace with name: {workspace_name}.")
        print("Please see full response for error:")
        print(response.text)


def add_members_to_auth_domain(auth_domain_name):
    """Add the anvil-admins@firecloud.org group as ADMIN to auth domain."""

    admin_group_name = "anvil-admins@firecloud.org"

    # request URL for addUserToGroup
    uri = f"https://api.firecloud.org/api/groups/{auth_domain_name}/admin/{admin_group_name}"

    # Get access token and and add to headers for requests.
    # -H  "accept: */*" -H  "Authorization: Bearer [token]"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*"}

    # capture response from API and parse out status code
    response = requests.put(uri, headers=headers)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code == 204:
        print(f"Successfully updated Authorization Domain, {auth_domain_name}, with group: {admin_group_name}.")

    else:
        print(f"WARNING: Failed to update Authorization Domain, {auth_domain_name}, with group: {admin_group_name}.")
        print("Please see full response for error:")
        print(response.text)


def create_auth_domain(auth_domain_name):
    """Create the Terra goolge group to be used as auth domain on workspaces."""

    # request URL for createGroup
    uri = f"https://api.firecloud.org/api/groups/{auth_domain_name}"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.post(uri, headers=headers)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code == 201:
        print(f"Successfully created Autorization Domain with name: {auth_domain_name}.")

    else:
        print(f"WARNING: Failed to create Authorization Domain with name: {auth_domain_name}.")
        print("Please see full response for error:")
        print(response.text)

    # TODO: should anvil-admins@firecloud.org be added as an AUTH DOMAIN separately from AnVIL auth domain?
    add_members_to_auth_domain(auth_domain_name)


def setup_workspaces(tsv, project="anvil-datastorage"):
    """Get the workspace and associated auth domain from input tsv file."""

    # read full tsv into dataframe
    setup_info_df = pd.read_csv(tsv, sep="\t")

    # per row in tsv/df
    for row in setup_info_df.index:
        auth_domain_name = setup_info_df.loc[row].get(key='auth_domain_name')
        create_auth_domain(auth_domain_name)

        # make json for createWorkspace
        ws_json = {}
        ws_json["namespace"] = project
        ws_json["name"] = setup_info_df.loc[row].get(key='workspace_name')
        ws_json["authorizationDomain"] = [{"membersGroupName": f'{auth_domain_name}'}]
        ws_json["attributes"] = {}
        ws_json["noWorkspaceOwner"] = False

        create_workspace(ws_json)
        add_members_to_workspace(ws_json["name"], auth_domain_name, project)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with workspace name and auth domains to create.')
    parser.add_argument('-p', '--workspace_project', type=str, default="anvil-datastorage", help='workspace project/namespace. default: anvil-datastorage')

    args = parser.parse_args()

    # call to create request body PER row and make API call to update attributes
    setup_workspaces(args.tsv, args.workspace_project)
