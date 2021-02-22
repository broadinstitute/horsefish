"""Put/update Dataset attributes to workspaces parsed from input tsv file.

Usage:
    > python3 post_workspace_attributes.py -t TSV_FILE [-p BILLING-PROJECT] """

import argparse
import pandas as pd
import requests

from oauth2client.client import GoogleCredentials


def get_access_token():
    '''Get access token.'''

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def call_putLibraryMetadata_api(request, workspace_name, workspace_project):
    '''PUT request to the putLibraryMetadata API.'''

    # request URL for putLibraryMetadata
    uri = f"https://api.firecloud.org/api/library/{workspace_project}/{workspace_name}/metadata"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"

    # capture response from API and parse out status code
    response = requests.put(uri, headers=headers, data=request)
    status_code = response.status_code

    # print success or fail message based on status code
    if status_code == 200:
        print(f"Successfully added/updated {workspace_project}/{workspace_name} with Dataset attributes.")

    else:
        print(f"WARNING: Failed to add/update Dataset attributes to {workspace_project}/{workspace_name}")
        print("Please see full response for error:")
        print(response.text)


def update_workspace_attributes(tsv, workspace_project):
    '''Create individual request body per workspace listed in tsv file.'''

    # read full tsv into dataframe, workspace name = index
    tsv_all = pd.read_csv(tsv, sep="\t", index_col="name", encoding='latin-1')

    # remove columns that have ".itemsType" in col name - col values are AttributeValue
    tsv_modified = tsv_all[tsv_all.columns.drop(list(tsv_all.filter(regex='.itemsType')))]

    # remove the ".items" string from column names
    tsv_modified.columns = tsv_modified.columns.str.replace(".items", "")

    # make json request for each workspace and call API
    for workspace in tsv_modified.index:
        row_json_request = tsv_modified.loc[workspace].to_json()

        # call to API call with request and workspace name
        call_putLibraryMetadata_api(row_json_request, workspace, workspace_project)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with attributes to post to workspaces.')
    parser.add_argument('-p', '--workspace_project', type=str, default="anvil-datastorage", help='Workspace Project/Namespace')

    args = parser.parse_args()

    # call to create request body PER row and make API call to update attributes
    update_workspace_attributes(args.tsv, args.workspace_project)
