# -*- coding: utf-8 -*-
import datetime
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
def write_output_report(dataframe):
    """Report workspace set-up statuses and create output tsv file from provided dataframe."""

    # create timestamp and use to label output file
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_filename = f"{timestamp}_workspaces_published_status.tsv"
    dataframe.to_csv(output_filename, sep="\t", index=False)

    # count success and failed workspaces and report to stdout
    successes = dataframe.final_workspace_status.str.count("Success").sum()
    fails = dataframe.final_workspace_status.str.count("Failed").sum()
    total = successes + fails
    print(f"Number of workspaces passed set-up: {successes}/{total}")
    print(f"Number of workspaces failed set-up: {fails}/{total}")
    print(f"All workspace set-up (success or fail) details available in output file: {output_filename}")


# API calls #

# function to add tags to a workspace (does not overwrite existing)
def add_tags_to_workspace(workspace_name, tags, project):
    """Add tags to workspace."""

    uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace_name}/tags"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token] -H  "Content-Type: application/json"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json", "Content-Type": "application/json"}

    # capture response from API and parse out status code
    response = requests.patch(uri, headers=headers, data=tags)
    status_code = response.status_code

    if status_code != 200:  # could not add tags
        print(f"WARNING: Failed to add tags for workspace with name: {project}/{workspace_name}.")
        print("Check output file for error details.")
        return False, response.text

    return True, response.text


# function to determine if a workspace already exists
def check_workspace_exists(workspace_name, project):
    """Determine if a workspace of given namespace/name already exists."""

    # don't need full response - could be very large and time consuming
    uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace_name}?fields=owners,workspace.createdBy,workspace.authorizationDomain"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code == 404:              # workspace does not exist
        return False, None

    if status_code != 200:              # additional errors - 403, 500
        return None, response.text

    return True, response.text       # workspace exists


def get_workspace_authorization_domain(workspace_name, project):
    """Get list of authorization domain/s of workspace."""

    uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace_name}?fields=workspace.authorizationDomain"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:  # could not get authorization domain
        print(f"WARNING: Failed to get authorization domain for workspace with name: {project}/{workspace_name}.")
        print("Check output file for error details.")
        return False, response.text

    return True, response.text


def get_workspace_members(workspace_name, project):
    """Get ACLs of workspace."""

    uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace_name}/acl"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:  # could not get acls
        print(f"WARNING: Failed to get ACLs for workspace with name: {project}/{workspace_name}.")
        print("Check output file for error details.")
        return False, response.text

    return True, response.text


def get_workspace_tags(workspace_name, project):
    """Get tags of workspace."""

    uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace_name}/tags"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:  # could not get acls
        print(f"WARNING: Failed to get tags for workspace with name: {project}/{workspace_name}.")
        print("Check output file for error details.")
        return False, response.text

    return True, response.text
