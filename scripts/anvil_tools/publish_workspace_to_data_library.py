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
    output_filename = f"{timestamp}_workspaces_setup_status.tsv"
    workspace_status_dataframe.to_csv(output_filename, sep="\t", index=False)

    # count success and failed workspaces and report to stdout
    successes = workspace_status_dataframe.workspace_setup_status.str.count("Success").sum()
    fails = workspace_status_dataframe.workspace_setup_status.str.count("Failed").sum()
    total = successes + fails
    print(f"Number of workspaces passed set-up: {successes}/{total}")
    print(f"Number of workspaces failed set-up: {fails}/{total}")
    print(f"All workspace set-up (success or fail) details available in output file: {output_filename}")


def call_publish_library_workspace_api(workspace_name, project="anvil-datastorage"):
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
        return False, response

    print(f"Successfully published {project}/{workspace_name} to Data Library.")
    return True, response


def publish_single_workspace(workspace_info, project="anvil-datastorage"):
    """Parse workspace information and call API to publish to Data Library."""

    # create dictionary to populate for output tsv file
    workspace_dict = {"input_workspace_name": "NA",
                      "input_project_name": "NA",
                      "workspace_link": "NA",
                      "publish_workspace_error": "NA",
                      "publish_workspace_status": "Failed"}

    workspace_name = workspace_info["workspace_name"]
    workspace_dict["input_project_name"] = "Success"
    workspace_dict["workspace_link"] = (f"https://portal.firecloud.org/#workspaces/{project}/{workspace_name}").replace(" ", "%20")

    # call api to publish workspace and get response
    success, response = call_publish_library_workspace_api(workspace_name, project)

    if not success:                                         # publish fail
        workspace_dict["workspace_publish_error"] = response
        return workspace_dict

    workspace_dict["workspace_publish_status"] = "Success"  # publish success
    return workspace_dict


def setup_all_workspaces_to_publish(tsv, project="anvil-datastorage"):
    """Publish workspace, capture success/fail, and report details in output tsv file."""

    # read input tsv into dataframe
    to_publish_workspaces = pd.read_csv(tsv, sep="\t")

    # create df for output tsv file
    col_names = ["input_workspace_name", "input_project_name",
                 "workspace_link", "publish_workspace_error",
                 "publish_workspace_error"]
    published_workspace_statuses = pd.DataFrame(columns=col_names)

    # per row in input tsv/df
    for index, row in to_publish_workspaces.iterrows():
        # publish the single workspace
        row_dict = publish_single_workspace(row, project)

        # append single workspace's dictionary of statuses to output dataframe
        published_workspace_statuses = published_workspace_statuses.append(row_dict, ignore_index=True)

    # convert all workspaces' dataframe into csv report
    write_output_report(published_workspace_statuses)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with attributes to post to workspaces.')
    parser.add_argument('-p', '--workspace_project', type=str, default="anvil-datastorage", help='Workspace Project/Namespace')

    args = parser.parse_args()

    # call to create request body PER row and make API call to update attributes
    setup_all_workspaces_to_publish(args.tsv, args.workspace_project)
