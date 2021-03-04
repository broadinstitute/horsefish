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
def add_library_metadata_to_workspace(request, workspace_name, project="anvil-datastorage"):
    """Add/update Dataset/library attributes in a workspace."""

    # Library/putLibraryMetadata
    uri = f"https://api.firecloud.org/api/library/{project}/{workspace_name}/metadata"

    # Get access token and and add to headers for requests.
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}

    # capture response from API and parse out status code
    response = requests.put(uri, headers=headers, data=request)
    status_code = response.status_code

    # adding metadata fail
    if status_code != 200:
        print(f"WARNING: Failed to add/update Dataset attributes to {project}/{workspace_name}")
        print("Please see full response for error:")
        print(response.text)
        return False, response.text

    # adding metadata success
    print(f"Successfully added/updated {project}/{workspace_name} with Dataset attributes.")
    return True, response.text


# function to add user to existing authorization domain
def add_user_to_authorization_domain(auth_domain_name, email, permission):
    """Add group with given permissions to authorization domain."""

    # request URL for addUserToGroup
    uri = f"https://api.firecloud.org/api/groups/{auth_domain_name}/{permission}/{email}"

    # Get access token and and add to headers for requests.
    # -H  "accept: */*" -H  "Authorization: Bearer [token]"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*"}

    # capture response from API and parse out status code
    response = requests.put(uri, headers=headers)
    status_code = response.status_code

    if status_code != 204:  # AD update with member fail
        print(f"WARNING: Failed to update Authorization Domain, {auth_domain_name}, with group: {email}.")
        print("Check output file for error details.")
        return False, response.text

    # AD update with member success
    print(f"Successfully updated Authorization Domain, {auth_domain_name}, with group: {email}.")
    return True, None


# function to determine if a workspace already exists
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

    if status_code == 404:              # workspace does not exist
        return False, None

    if status_code != 200:              # additional errors - 403, 500
        return None, response.text

    return True, response.text       # workspace exists


# function to create an authorization domain
def create_authorization_domain(auth_domain_name):
    """Create authorization domain with given name."""

    # request URL for createGroup
    uri = f"https://api.firecloud.org/api/groups/{auth_domain_name}"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]"
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.post(uri, headers=headers)
    status_code = response.status_code

    if status_code in [403, 409]:                    # if AD already exists - 403, 409
        ad_exists_message = "Authorization Domain with name already exists. Select unique name."
        print(f"WARNING: Failed to setup Authorization Domain with name: {auth_domain_name}. Check output file for error details.")
        # return status_code, None, ad_exists_message
        return False, ad_exists_message
    if status_code != 201:                           # other error - 500
        print(f"WARNING: Failed to create Authorization Domain with name: {auth_domain_name}. Check output file for error details.")
        return False, response.text

    print(f"Successfully setup Authorization Domain with name: {auth_domain_name}.")
    return True, None                               # create AD success - 204
