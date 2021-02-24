"""Create AnVIL workspaces, set up with auth domain, add workspace READER access to auth domain, and OWNER access to AnVIL admins.

Usage:
    > python3 set_up_anvil_workspace.py -t TSV_FILE [-p BILLING-PROJECT] """

import argparse
import datetime
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
    # add auth domain as READER, anvil-admins as OWNER
    acls.append({'email': f'{auth_domain_name}@firecloud.org', 'accessLevel': 'READER', 'canShare': False, 'canCompute': False})
    acls.append({'email': 'anvil-admins@firecloud.org', 'accessLevel': 'OWNER', 'canShare': True, 'canCompute': True})

    json_request = json.dumps(acls)

    # request URL for updateWorkspaceACL
    uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace_name}/acl?inviteUsersNotFound=false"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"

    # capture response from API and parse out status code
    response = requests.patch(uri, headers=headers, data=json_request)
    status_code = response.status_code

    emails = [acl['email'] for acl in acls]
    # print success or fail message based on status code
    if status_code != 200:
        print(f"WARNING: Failed to update {project}/{workspace_name} with the following user(s)/group(s): {emails}.")
        print("Check output file for error details.")
        return status_code, response.json()
    else:
        print(f"Successfully updated {project}/{workspace_name} with the following user(s)/group(s): {emails}.")
        return status_code, emails


def check_workspace_exists(workspace_name, project="anvil-datastorage"):
    """Determine if a workspace of given namespace/name already exists."""

    # don't need full response - could be very large and time consuming
    uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace_name}?fields=owners,workspace.createdBy,workspace.authorizationDomain"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code == 404:  # if workspace does not exist
        return False
    elif status_code == 200:  # if workspace already exists
        print(f"Workspace already exists with name: {project}/{workspace_name}.")
        print(f"Existing workspace details: {json.dumps(response.json(), indent=2)}")
        # make user decide if they want to update/overwrite existing workspace
        update_existing_ws = input("Would you like to continue modifying the existing workspace? (Y/N)" + "\n")
        return update_existing_ws.upper()
    else:  # if other error
        return response.json()


def create_workspace(json_request, workspace_name, project="anvil-datastorage"):
    """Create the Terra workspace with given authorization domain."""

    # check if workspace already exists
    status_ws_exists = check_workspace_exists(workspace_name, project)

    # if workspace does not exist, create workspace
    if not status_ws_exists:
        # request URL for createWorkspace
        uri = f"https://api.firecloud.org/api/workspaces"

        # Get access token and and add to headers for requests.
        # -H  "accept: application/json" -H  "Authorization: Bearer [token] -H  "Content-Type: application/json"
        headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json", "Content-Type": "application/json"}

        # capture response from API and parse out status code
        response = requests.post(uri, headers=headers, data=json.dumps(json_request))
        status_code = response.status_code

        # print success or fail message based on status code
        if status_code != 201:
            print(f"WARNING: Failed to create workspace with name: {workspace_name}. Check output file for error details.")
            return response.json()
        else:
            print(f"Successfully created workspace with name: {workspace_name}.")
            return status_code
    elif status_ws_exists == "N":
        return(f"{project}/{workspace_name} already existed. User selected not to overwrite. Try again with unique workspace name.")
    # if workspace already exists, dont make workspace
    else:
        return status_ws_exists  # ("Y" or json with other errors)


def make_create_workspace_request(workspace_name, auth_domain_name, project="anvil-datastorage"):
    """Make the json request to pass into create_workspace()."""

    # initialize empty dictionary
    create_ws_request = {}

    create_ws_request["namespace"] = project
    create_ws_request["name"] = workspace_name
    create_ws_request["authorizationDomain"] = [{"membersGroupName": f'{auth_domain_name}'}]
    create_ws_request["attributes"] = {}
    # TODO: set noWorkspaceOwner = True for data delivery workspaces - picard svc is the only owner
    create_ws_request["noWorkspaceOwner"] = False

    return create_ws_request


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
        print("Check output file for error details.")
        print(response.json())


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
        print(f"Successfully created Authorization Domain with name: {auth_domain_name}.")
        return status_code
    elif status_code in ["403", "409"]:
        print(f"WARNING: Failed to create Authorization Domain with name: {auth_domain_name}.")
        return "Authorization Domain with name already exists. Try again with unique group name."
    else:
        print(f"WARNING: Failed to create Authorization Domain with name: {auth_domain_name}.")
        print("Check output file for error details.")
        return(response.json())


def setup_workspaces(tsv, project="anvil-datastorage"):
    """Get the workspace and associated auth domain from input tsv file."""

    # read full tsv into dataframe
    setup_info_df = pd.read_csv(tsv, sep="\t")

    # create df for output tsv file
    col_names = ["input_workspace_name", "input_auth_domain_name",
                 "auth_domain_email", "auth_domain_creation_error",
                 "workspace_link", "workspace_creation_error",
                 "workspace_ACLs", "workspace_ACLs_error",
                 "workspace_setup_status"]
    output_df = pd.DataFrame(columns=col_names)

    # per row in tsv/df
    for row in setup_info_df.index:
        # authorization domain
        auth_domain_name = setup_info_df.loc[row].get(key='auth_domain_name')
        output_df.at[row, "input_auth_domain_name"] = auth_domain_name
        auth_status = create_auth_domain(auth_domain_name)

        if auth_status != 201:  # if auth creation fail
            output_df.at[row, "auth_domain_creation_error"] = auth_status  # write error response to col
            continue  # go to next row in df
        else:  # if auth creation success
            # TODO: will i need auth_domain_email again?
            auth_domain_email = f"{auth_domain_name}@firecloud.org"
            output_df.at[row, "auth_domain_email"] = auth_domain_email

        # workspace
        workspace_name = setup_info_df.loc[row].get(key='workspace_name')
        output_df.at[row, "input_workspace_name"] = workspace_name
        create_ws_request = make_create_workspace_request(workspace_name, auth_domain_name, project)  # get json for createWorkspace

        create_workspace_status = create_workspace(create_ws_request, workspace_name, project)  # createWorkspace

        # created or user wants to continue with existing
        if create_workspace_status in [201, "Y"]:
            output_df.at[row, "workspace_link"] = f"https://app.terra.bio/#workspaces/{project}/{workspace_name}"
            # add member ACLs to workspace
            member_status_code, member_response = add_members_to_workspace(workspace_name, auth_domain_name, project)
            if member_status_code == 200:
                output_df.at[row, "workspace_ACLs"] = member_response  # add emails to df
                output_df.at[row, "workspace_setup_status"] = "Success"  # denotes final workspace setup step
            else:
                output_df.at[row, "workspace_ACLs_error"] = member_response
                output_df.at[row, "workspace_setup_status"] = "Failed"
        # if "N" (user does not want to continue with existing workspace) or other error
        else:
            output_df.at[row, "workspace_creation_error"] = create_workspace_status  # write error response to col
            output_df.at[row, "workspace_ACLs_error"] = "Incomplete"  # adding members function not called
            output_df.at[row, "workspace_setup_status"] = "Failed"
            continue

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_df.to_csv(f"{timestamp}_workspaces_setup_status.tsv", sep="\t", index=False)

    print("If applicable, workspace creation and set-up complete. Check output file for full details.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with workspace name and auth domains to create.')
    parser.add_argument('-p', '--workspace_project', type=str, default="anvil-datastorage", help='workspace project/namespace. default: anvil-datastorage')

    args = parser.parse_args()

    # call to create request body PER row and make API call to update attributes
    setup_workspaces(args.tsv, args.workspace_project)
