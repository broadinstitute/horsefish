"""Create workspaces, set up bucket in us-central region, add workspace access to users.

Usage:
    > python3 set_up_vanallen_workspaces.py -t TSV_FILE [-p NAMESPACE] """

import argparse
import json
import pandas as pd
import requests

from utils import check_workspace_exists, \
    get_access_token, \
    write_output_report


NAMESPACE = "vanallen-firecloud-nih"
BUCKET_REGION = "us-central1"


def add_members_to_workspace(workspace_name, project=NAMESPACE):
    """Add members to workspace permissions."""

    acls = []
    # add van allen group as READER, B.Reardon and J.Park OWNER(s)
    acls.append({'email': 'GROUP_vanallenlab@firecloud.org', 'accessLevel': 'READER', 'canShare': False, 'canCompute': False})
    acls.append({'email': 'breardon@broadinstitute.org', 'accessLevel': 'OWNER', 'canShare': True, 'canCompute': True})
    acls.append({'email': 'jpark@broadinstitute.org', 'accessLevel': 'OWNER', 'canShare': True, 'canCompute': True})

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
        return False, response.text

    print(f"Successfully updated {project}/{workspace_name} with the following user(s)/group(s): {emails}.")
    emails_str = ("\n".join(emails))  # write list of emails as strings on new lines
    return True, emails_str


def create_workspace(workspace_name, project=NAMESPACE):
    """Create the Terra workspace."""

    # check if workspace already exists
    ws_exists, ws_exists_response = check_workspace_exists(workspace_name, project)

    if ws_exists is None:
        return False, ws_exists_response

    if not ws_exists:  # workspace doesn't exist (404), create workspace
        # create request JSON
        create_ws_json = make_create_workspace_request(workspace_name, project)  # json for API request

        # request URL for createWorkspace (rawls) - bucketLocation not supported in orchestration
        uri = f"https://rawls.dsde-prod.broadinstitute.org/api/workspaces"

        # Get access token and and add to headers for requests.
        # -H  "accept: application/json" -H  "Authorization: Bearer [token] -H  "Content-Type: application/json"
        headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json", "Content-Type": "application/json"}

        # capture response from API and parse out status code
        response = requests.post(uri, headers=headers, data=json.dumps(create_ws_json))
        status_code = response.status_code

        if status_code != 201:  # ws creation fail
            print(f"WARNING: Failed to create workspace with name: {workspace_name}. Check output file for error details.")
            return False, response.text
        # workspace creation success
        print(f"Successfully created workspace with name: {workspace_name}.")
        return True, None

    # workspace already exists
    print(f"Workspace already exists with name: {project}/{workspace_name}.")
    print(f"Existing workspace details: {json.dumps(json.loads(ws_exists_response), indent=2)}")
    # make user decide if they want to update/overwrite existing workspace
    while True:  # try until user inputs valid response
        update_existing_ws = input("Would you like to continue modifying the existing workspace? (Y/N)" + "\n")
        if update_existing_ws.upper() in ["Y", "N"]:
            break
        else:
            print("Not a valid option. Choose: Y/N")
    if update_existing_ws.upper() == "N":       # don't overwrite existing workspace
        deny_overwrite_message = f"{project}/{workspace_name} already exists. User selected not to overwrite. Try again with unique workspace name."
        return None, deny_overwrite_message

    accept_overwrite_message = f"{project}/{workspace_name} already exists. User selected to overwrite."
    return True, accept_overwrite_message    # overwrite existing workspace - 200 status code for "Y"


def make_create_workspace_request(workspace_name, project=NAMESPACE):
    """Make the json request to pass into create_workspace()."""

    # initialize empty dictionary
    create_ws_request = {}

    create_ws_request["namespace"] = project
    create_ws_request["name"] = workspace_name
    create_ws_request["attributes"] = {}
    create_ws_request["noWorkspaceOwner"] = False
    # us-central1 default - all van allen resources to migrate to same region
    create_ws_request["bucketLocation"] = BUCKET_REGION

    return create_ws_request


def setup_single_workspace(workspace, project=NAMESPACE):
    """Create one workspace and set ACLs."""

    # initialize workspace dictionary with default values assuming failure
    workspace_dict = {"input_workspace_name": "NA",
                      "workspace_link": "Incomplete",
                      "workspace_creation_error": "NA",
                      "workspace_ACLs": "Incomplete",
                      "workspace_ACLs_error": "NA",
                      "final_workspace_status": "Failed"}

    # workspace creation
    workspace_name = workspace["workspace_name"]
    workspace_dict["input_workspace_name"] = workspace_name

    # create workspace
    create_ws_success, create_ws_message = create_workspace(workspace_name, project)

    workspace_dict["workspace_creation_error"] = create_ws_message

    if not create_ws_success:
        return workspace_dict

    # ws creation success
    workspace_dict["workspace_link"] = (f"https://app.terra.bio/#workspaces/{project}/{workspace_name}").replace(" ", "%20")

    # add ACLs to workspace if workspace creation success
    add_member_success, add_member_message = add_members_to_workspace(workspace_name, project)

    if not add_member_success:
        workspace_dict["workspace_ACLs_error"] = add_member_message
        return workspace_dict

    # adding ACLs to workspace success
    workspace_dict["workspace_ACLs"] = add_member_message  # update dict with ACL emails
    workspace_dict["final_workspace_status"] = "Success"  # final workspace setup step

    return workspace_dict


def setup_workspaces(tsv, project=NAMESPACE):
    """Get the workspace name from input tsv file."""

    # read full tsv into dataframe
    setup_info_df = pd.read_csv(tsv, sep="\t")

    # create df for output tsv file
    col_names = ["input_workspace_name",
                 "workspace_link", "workspace_creation_error",
                 "workspace_ACLs", "workspace_ACLs_error",
                 "final_workspace_status"]
    all_row_df = pd.DataFrame(columns=col_names)

    # per row in tsv/df
    for index, row in setup_info_df.iterrows():

        row_dict = setup_single_workspace(row, project)
        all_row_df = all_row_df.append(row_dict, ignore_index=True)

    write_output_report(all_row_df)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Set-up Van Allen Lab workspaces.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with workspace name to create.')
    parser.add_argument('-p', '--workspace_namespace', type=str, default=NAMESPACE, help='workspace project/namespace. default: vanallen-firecloud-nih')

    args = parser.parse_args()

    # call to create and set up workspaces
    setup_workspaces(args.tsv, args.workspace_project)
