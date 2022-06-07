# -*- coding: utf-8 -*-
import argparse
import datetime
from firecloud import api as fapi
import json
import pandas as pd
import requests
from google.cloud import storage
from oauth2client.client import GoogleCredentials

# get authorization bearer token for requests
def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def add_workspace_data(workspace_name, workspace_project, request):
    """PATCH workspace with workspace level variables."""

    # create URL for rawls update_workspace
    uri = f"https://rawls.dsde-prod.broadinstitute.org/api/workspaces/{workspace_project}/{workspace_name}"

    # Get access token and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json", "Content-Type": "application/json"}

    # capture response from API and parse out status code
    response = requests.patch(uri, headers=headers, data=request)
    status_code = response.status_code

    if status_code != 200:
        print(f"WARNING: Failed to add/update workspace variables to {workspace_project}/{workspace_name}.")
        print("Please see full response for error:")
        print(response.text)
        return False, response.text

    print(f"Successfully added/updated {workspace_project}/{workspace_name} with workspace variables.")
    return True, response.text


def add_user_to_workspace(workspace_name, workspace_project, email):
    """PUT request to the putLibraryMetadata API."""

    # request URL for updateWorkspaceACL
    uri = f"https://api.firecloud.org/api/workspaces/{workspace_project}/{workspace_name}/acl?inviteUsersNotFound=false"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}

    request = "[" + json.dumps({"email": email, "accessLevel": "READER", "canShare": False, "canCompute": False}) + "]"
    # capture response from API and parse out status code
    response = requests.patch(uri, headers=headers, data=request)
    status_code = response.status_code

    # if adding user fails
    if status_code != 200:
        print(f"WARNING: Failed to add/update user/group to {workspace_project}/{workspace_name}.")
        print("Please see full response for error:")
        print(response.text)
        return False, response.text

    print(f"Successfully added/updated {workspace_project}/{workspace_name} with user/group {email}.")
    return True, response.text


# clone Terra workspace
def clone_workspace(src_namespace, src_workspace, dest_namespace, dest_workspace, auth_domains, copyFilesWithPrefix=None):
    """Clone a Terra workspace."""

    workspace = fapi.clone_workspace(src_namespace, src_workspace, dest_namespace, dest_workspace, auth_domains, copyFilesWithPrefix)
    workspace_json = workspace.json()

    # workspace clone fails
    if workspace.status_code != 201:
        print("Workspace clone failed. Please see below for full error.")
        print(workspace_json)
        return False, workspace_json

    print(f"Cloned workspace: {dest_namespace}/{dest_workspace}")
    return True, workspace_json


# function to determine if a workspace already exists
def check_workspace_exists(workspace_name, workspace_project):
    """Determine if a workspace of given namespace/name already exists."""

    # don't need full response - could be very large and time consuming
    uri = f"https://api.firecloud.org/api/workspaces/{workspace_project}/{workspace_name}?fields=owners,workspace.createdBy,workspace.authorizationDomain"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code == 404:              # workspace does not exist
        return False, None

    if status_code != 200:              # additional errors - 403, 500
        return None, response.text

    return True, response.text       # workspace exists


def copy_objects_across_buckets(src_bucket, dest_bucket, subdir=None):
    """Copy object(s) from one bucket to another."""

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(src_bucket)
    destination_bucket = storage_client.bucket(dest_bucket)

    blobs_to_copy = storage_client.list_blobs(src_bucket, prefix=subdir)
    for blob in blobs_to_copy:
        source_bucket.copy_blob(blob, destination_bucket)

    print(f"Files in bucket {src_bucket} successfully copied to {dest_bucket}.")


def get_workspace_authorization_domain(workspace_name, workspace_project):
    """Get the workspace authorization domain for a given workspace and workspace project."""

    # request URL for createGroup
    uri = f"https://api.firecloud.org/api/workspaces/{workspace_project}/{workspace_name}?fields=workspace.authorizationDomain"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        return response.text

    # returns an empty list if no auth domains
    auth_domain_list = response.json()["workspace"]["authorizationDomain"]
    # if list empty, return message
    if not auth_domain_list:
        auth_domains = []
        return auth_domains

    # if list not empty, return formatted list of auth domains
    auth_domains = [ad["membersGroupName"] for ad in auth_domain_list]
    print(f"Authorization Domain(s) for {workspace_project}/{workspace_name}: {auth_domains}")
    return auth_domains


def get_workspace_bucket(workspace_name, workspace_project):
    """Get workspace bucket id (gs://fc-) of workspace."""

    uri = f"https://api.firecloud.org/api/workspaces/{workspace_project}/{workspace_name}?fields=workspace.bucketName"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json"}

    # capture response from API and parse out status code
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:  # could not get bucket id
        print(f"WARNING: Failed to get bucket id for workspace with name: {workspace_project}/{workspace_name}.")
        print("Check output file for error details.")
        return False, response.text

    return True, response.json()


def get_workspace_attributes(workspace_name, workspace_project):
    """Get workspace variables from a given workspace."""

    ws_attributes = fapi.get_workspace(workspace_project, workspace_name, fields="workspace.attributes \n").json()

    return ws_attributes


def make_create_workspace_request(workspace_name, workspace_project, auth_domain_name):
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


# update dashboard with additional information
def update_workspace_dashboard(workspace_project, workspace_name, message):
    """Update Terra workspace dashboard with additional text."""

    # https://rawls.dsde-prod.broadinstitute.org/#/workspaces/update_workspace
    uri = f"https://rawls.dsde-prod.broadinstitute.org/api/workspaces/{workspace_project}/{workspace_name}"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json", "Content-Type": "application/json"}

    # capture API response and status_code
    dashboard = requests.patch(uri, headers=headers, data=message)
    status_code = dashboard.status_code

    # update fail
    if status_code != 200:
        print(f"Dashboard update with user message failed. Please see below for full error.")
        print(dashboard.json())
        return False, dashboard.json()

    # update success
    print(f"Dashboard update with user message successful.")
    return True, dashboard.json()