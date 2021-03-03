# -*- coding: utf-8 -*-
import argparse
import datetime
import pandas as pd
import requests

from oauth2client.client import GoogleCredentials


# function to get authorization bearer token for requests
def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


# function to write output tsv file and success/fail statistics with dataframe as input
def write_output_report(workspace_status_dataframe):
    """Report workspace set-up statuses and create output tsv file from provided dataframe."""

    # create timestamp and use to label output file
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_filename = f"{timestamp}_workspaces_published_status.tsv"
    workspace_status_dataframe.to_csv(output_filename, sep="\t", index=False)

    # count success and failed workspaces and report to stdout
    successes = workspace_status_dataframe.publish_workspace_status.str.count("Success").sum()
    fails = workspace_status_dataframe.publish_workspace_status.str.count("Failed").sum()
    total = successes + fails
    print(f"Number of workspaces passed set-up: {successes}/{total}")
    print(f"Number of workspaces failed set-up: {fails}/{total}")
    print(f"All workspace set-up (success or fail) details available in output file: {output_filename}")


# API calls #

# publish a workspace to the Data Library in FireCloud
def publish_workspace_to_data_library(workspace_name, project="anvil-datastorage"):
    """Publish workspace to Firecloud Data Library."""

    # Library/publishLibraryWorkspace
    uri = f"https://api.firecloud.org/api/library/{project}/{workspace_name}/published"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture API response and status_code
    response = requests.post(uri, headers=headers)
    status_code = response.status_code

    # publishing fail
    if status_code not in [200, 204]:
        print(f"WARNING: Failed to publish workspace to Data Library: {project}/{workspace_name}.")
        print("Please see full response for error:")
        print(response.text)
        return False, response.text

    # publishiing success
    print(f"Successfully published {project}/{workspace_name} to Data Library.")
    return True, response.text


# function to add dataset/library metadata to a workspace
def add_library_metadata_to_workspace(request, workspace_name, workspace_project="anvil-datastorage"):
    """Add/update Dataset/library attributes in a workspace."""

    # Library/putLibraryMetadata
    uri = f"https://api.firecloud.org/api/library/{workspace_project}/{workspace_name}/metadata"

    # Get access token and and add to headers for requests.
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}

    # capture response from API and parse out status code
    response = requests.put(uri, headers=headers, data=request)
    status_code = response.status_code

    # adding metadata fail
    if status_code != 200:
        print(f"WARNING: Failed to add/update Dataset attributes to {workspace_project}/{workspace_name}")
        print("Please see full response for error:")
        print(response.text)
        return False, response.text

    # adding metadata success
    print(f"Successfully added/updated {workspace_project}/{workspace_name} with Dataset attributes.")
    return True, response.text
