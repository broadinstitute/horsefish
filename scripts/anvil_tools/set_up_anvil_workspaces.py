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


def write_output_report(report_df):
    """Report workspace set-up stats and create output tsv file from provided dataframe."""

    # create timestamp and use to label output file
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_filename = f"{timestamp}_workspaces_setup_status.tsv"
    report_df.to_csv(output_filename, sep="\t", index=False)

    # count success and failed workspaces and report to stdout
    successes = report_df.workspace_setup_status.str.count("Success").sum()
    fails = report_df.workspace_setup_status.str.count("Failed").sum()
    total = successes + fails
    print(f"Number of workspaces passed set-up: {successes}/{total}")
    print(f"Number of workspaces failed set-up: {fails}/{total}")
    print(f"All workspace set-up (success or fail) details available in output file: {output_filename}")


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

    if status_code == 404:          # if workspace does not exist
        return False, None, None
    elif status_code == 200:        # if workspace already exists
        return True, status_code, response
    else:                           # if other error
        return None, None, response.json()


def create_workspace(json_request, workspace_name, project="anvil-datastorage"):
    """Create the Terra workspace with given authorization domain."""

    # check if workspace already exists
    ws_exists, ws_exists_status_code, ws_exists_response = check_workspace_exists(workspace_name, project)

    if not ws_exists:  # if workspace doesn't exist, create workspace
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
    elif ws_exists:  # if workspace already exists
        print(f"Workspace already exists with name: {project}/{workspace_name}.")
        print(f"Existing workspace details: {json.dumps(ws_exists_response.json(), indent=2)}")
        # make user decide if they want to update/overwrite existing workspace
        while True:  # try until user inputs valid response
            update_existing_ws = input("Would you like to continue modifying the existing workspace? (Y/N)" + "\n")
            if update_existing_ws.upper() in ["Y", "N"]:
                break
            else:
                print("Not a valid option. Choose: Y/N")
        if update_existing_ws.upper() == "N":
            return(f"{project}/{workspace_name} already existed. User selected not to overwrite. Try again with unique workspace name.")
        elif update_existing_ws.upper() == "Y":
            return ws_exists_status_code  # 200
    else:
        return ws_exists_response.json()


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


def add_member_to_auth_domain(auth_domain_name, email, permission):
    """Add group with given permissions to authorization domain."""

    # request URL for addUserToGroup
    uri = f"https://api.firecloud.org/api/groups/{auth_domain_name}/{permission}/{email}"

    # Get access token and and add to headers for requests.
    # -H  "accept: */*" -H  "Authorization: Bearer [token]"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*"}

    # capture response from API and parse out status code
    response = requests.put(uri, headers=headers)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code == 204:
        print(f"Successfully updated Authorization Domain, {auth_domain_name}, with group: {email}.")
        return status_code

    else:
        print(f"WARNING: Failed to update Authorization Domain, {auth_domain_name}, with group: {email}.")
        print("Check output file for error details.")
        return response.json()


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
        return response.json()


def setup_single_workspace(workspace, project="anvil-datastorage"):
    """Create one workspace and set up with authorization domain and ACLs."""

    # initialize workspace dictionary with default values assuming failure
    workspace_dict = {"input_workspace_name": "NA",
                      "input_auth_domain_name": "NA",
                      "auth_domain_email": "Incomplete",
                      "auth_domain_creation_error": "NA",
                      "email_added_to_AD": "Incomplete",
                      "add_email_to_AD_error": "NA",
                      "workspace_link": "Incomplete",
                      "workspace_creation_error": "NA",
                      "workspace_ACLs": "Incomplete",
                      "workspace_ACLs_error": "NA",
                      "workspace_setup_status": "Failed"}

    # start authorization domain
    auth_domain_name = workspace['auth_domain_name']
    workspace_dict["input_auth_domain_name"] = auth_domain_name
    auth_domain_status = create_auth_domain(auth_domain_name)

    if auth_domain_status != 201:  # AD creation fail - abort setup
        workspace_dict["auth_domain_creation_error"] = auth_domain_status  # update error in dict
    else:                          # AD creation success - continue setup
        workspace_dict["auth_domain_email"] = f"{auth_domain_name}@firecloud.org"
        anvil_admin_group_name = "anvil-admins@firecloud.org"
        add_member_status = add_member_to_auth_domain(auth_domain_name, anvil_admin_group_name, "ADMIN")
        if add_member_status != 204:  # adding member to AD fail - abort setup
            workspace_dict["add_email_to_AD_error"] = auth_domain_status  # update error in dict
        else:                         # adding member to AD success - continue setup
            workspace_dict["email_added_to_AD"] = anvil_admin_group_name

            # workspace creation if AD set up succeeds
            workspace_name = workspace["workspace_name"].replace(" ", "%20")
            workspace_dict["input_workspace_name"] = workspace["workspace_name"]
            create_ws_request = make_create_workspace_request(workspace_name, auth_domain_name, project)  # json for API request

            create_workspace_status = create_workspace(create_ws_request, workspace_name, project)  # create workspace

            if create_workspace_status in [201, 200]:  # new ws created or user selected 'continue with existing ws'
                workspace_dict["workspace_link"] = f"https://app.terra.bio/#workspaces/{project}/{workspace_name}"
                # add member ACLs to workspace
                member_status_code, member_response = add_members_to_workspace(workspace_name, auth_domain_name, project)
                if member_status_code == 200:  # adding ACLs to workspace success
                    workspace_dict["workspace_ACLs"] = member_response  # add emails to df
                    workspace_dict["workspace_setup_status"] = "Success"  # final workspace setup step
                else:                          # adding ACLs to workspace fail
                    workspace_dict["workspace_ACLs_error"] = member_response
            else:  # if "N" (user does not want to continue with existing workspace) or other error
                workspace_dict["workspace_creation_error"] = create_workspace_status  # update error in dict

    return workspace_dict


def setup_workspaces(tsv, project="anvil-datastorage"):
    """Get the workspace and associated auth domain from input tsv file."""

    # read full tsv into dataframe
    setup_info_df = pd.read_csv(tsv, sep="\t")

    # create df for output tsv file
    col_names = ["input_workspace_name", "input_auth_domain_name",
                 "auth_domain_email", "auth_domain_creation_error",
                 "email_added_to_AD", "add_email_to_AD_error",
                 "workspace_link", "workspace_creation_error",
                 "workspace_ACLs", "workspace_ACLs_error",
                 "workspace_setup_status"]
    all_row_df = pd.DataFrame(columns=col_names)

    # per row in tsv/df
    for index, row in setup_info_df.iterrows():

        row_dict = setup_single_workspace(row, project)
        all_row_df = all_row_df.append(row_dict, ignore_index=True)

    write_output_report(all_row_df)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with workspace name and auth domains to create.')
    parser.add_argument('-p', '--workspace_project', type=str, default="anvil-datastorage", help='workspace project/namespace. default: anvil-datastorage')

    args = parser.parse_args()

    # call to create request body PER row and make API call to update attributes
    setup_workspaces(args.tsv, args.workspace_project)

