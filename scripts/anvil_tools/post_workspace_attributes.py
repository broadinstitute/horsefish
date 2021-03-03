"""Put/update Dataset attributes to workspaces parsed from input tsv file.

Usage:
    > python3 post_workspace_attributes.py -t TSV_FILE [-p BILLING-PROJECT] """

import argparse
import datetime
import pandas as pd
import requests

from oauth2client.client import GoogleCredentials


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def write_output_report(workspace_status_dataframe):
    """Report workspace set-up statuses and create output tsv file from provided dataframe."""

    # create timestamp and use to label output file
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_filename = f"{timestamp}_workspaces_published_status.tsv"
    workspace_status_dataframe.to_csv(output_filename, sep="\t", index=False)

    # count success and failed workspaces and report to stdout
    successes = workspace_status_dataframe.workspace_setup_status.str.count("Success").sum()
    fails = workspace_status_dataframe.workspace_setup_status.str.count("Failed").sum()
    total = successes + fails
    print(f"Number of workspaces passed set-up: {successes}/{total}")
    print(f"Number of workspaces failed set-up: {fails}/{total}")
    print(f"All workspace set-up (success or fail) details available in output file: {output_filename}")


def publish_workspace(workspace_name, project="anvil-datastorage"):
    """Publish workspace to Firecloud Data Library."""

    # TODO: check if user is CURATOR and if NOT, return error message
    # request URL for publishLibraryWorkspace
    uri = f"https://api.firecloud.org/api/library/{project}/{workspace_name}/published"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.post(uri, headers=headers)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code not in [200, 204]:
        print(f"WARNING: Failed to publish workspace to Data Library: {project}/{workspace_name}.")
        print("Please see full response for error:")
        print(response.text)
        return False, response.text

    # TODO: what error code when you modify
    print(f"Successfully published {project}/{workspace_name} to Data Library.")
    return True, response.text


def put_library_metadata(request, workspace_name, workspace_project="anvil-datastorage"):
    """PUT request to the putLibraryMetadata API."""

    # request URL for putLibraryMetadata
    uri = f"https://api.firecloud.org/api/library/{workspace_project}/{workspace_name}/metadata"

    # Get access token and and add to headers for requests.
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}

    # capture response from API and parse out status code
    response = requests.put(uri, headers=headers, data=request)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code != 200:
        print(f"WARNING: Failed to add/update Dataset attributes to {workspace_project}/{workspace_name}")
        print("Please see full response for error:")
        print(response.text)
        return False, response.text

    print(f"Successfully added/updated {workspace_project}/{workspace_name} with Dataset attributes.")
    return True, response.text


def setup_single_workspace(request, project="anvil-datastorage"):
    """Update workspace with Dataset Attributes and publish to Data Library."""

    workspace_dict = {"input_workspace_name": "NA",
                      "input_project_name": "NA",
                      "workspace_link": "NA",
                      "dataset_attributes_status": "NA",
                      "publish_workspace_status": "Incomplete",
                      "workspace_setup_status": "Failed"}

    # update the workspace's dataset attributes per request in list of requests
    workspace_name = request["name"]
    workspace_dict["input_workspace_name"] = workspace_name
    workspace_dict["input_project_name"] = project
    workspace_dict["workspace_link"] = (f"https://app.terra.bio/#workspaces/{project}/{workspace_name}").replace(" ", "%20")

    # post each request (a single workspace's dataset attributes) to workspace
    post_attrs_success, post_attrs_response = put_library_metadata(request, workspace_name, project)

    if not post_attrs_success:  # if posting dataset attributes does not succeed
        workspace_dict["dataset_attributes_status"] = post_attrs_response
        return workspace_dict

    workspace_dict["post_dataset_attributes_status"] = "Success"

    # if posting dataset attributes succeeds, publish workspace
    publish_ws_success, publish_ws_response = publish_workspace(workspace_name, project)

    if not publish_ws_success:
        workspace_dict["publish_workspace_status"] = publish_ws_response
        return workspace_dict

    workspace_dict["publish_workspace_status"] = "Success"
    return workspace_dict


def setup_all_workspaces(tsv):
    """Create array of json requests per row in tsv file."""

    # read input tsv into dataframe, workspace name = index and edit dataframe
    all_workspaces = pd.read_csv(tsv, sep="\t", index_col="name")
    # remove columns that have ".itemsType" in col name - col values are AttributeValue
    all_workspaces_modified = all_workspaces[all_workspaces.columns.drop(list(all_workspaces.filter(regex='.itemsType')))]
    # remove the ".items" string from column names
    all_workspaces_modified.columns = all_workspaces_modified.columns.str.replace(".items", "")

    # create df for OUTPUT tsv file
    col_names = ["input_workspace_name", "input_project_name",
                 "workspace_link",
                 "post_dataset_attributes_status", "publish_workspace_error",
                 "workspace_setup_status"]
    published_workspace_statuses = pd.DataFrame(columns=col_names)

    # make json request for each workspace and append json request to list of requests
    row_requests = []
    for workspace in all_workspaces_modified.index:
        row_json_request = all_workspaces_modified.loc[workspace].to_json()
        row_requests.append(row_json_request)

    for request in row_requests:
        row_dict = setup_single_workspace(request)

        # append single workspace's dictionary of statuses to output dataframe
        published_workspace_statuses = published_workspace_statuses.append(row_dict, ignore_index=True)

    # convert all workspaces' dataframe into csv report
    write_output_report(published_workspace_statuses)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Post dataset attributes to workspace and publish workspace to Data Library.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with attributes to post to workspaces.')
    parser.add_argument('-p', '--workspace_project', type=str, default="anvil-datastorage", help='Workspace Project/Namespace')

    args = parser.parse_args()

    # call to create request body PER row and make API call to update attributes
    update_workspace_attributes(args.tsv, args.workspace_project)
