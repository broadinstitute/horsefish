# -*- coding: utf-8 -*-
import argparse
import datetime
from firecloud import api as fapi
import pandas as pd
import requests

from oauth2client.client import GoogleCredentials

# get authorization bearer token for requests
def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token

# clone Terra workspace
def clone_workspace(src_namespace, src_workspace, dest_namespace, dest_workspace, auth_domain, copyFilesWithPrefix=None):
    """Clone a Terra workspace."""

    workspace = fapi.clone_workspace(src_namespace, src_workspace, dest_namespace, dest_workspace, auth_domain, copyFilesWithPrefix)
    workspace_json = workspace.json()

    # workspace clone fails
    if workspace.status_code != 201:
        print("Workspace clone failed. Please see below for full error.")
        print(workspace_json)
        return False, workspace_json

    print(f"Workspace clone succesful.")
    print(f"Cloned workspace: {dest_namespace}/{dest_workspace}")
    return True, workspace_json


# update dashboard with additional information
def update_workspace_dashboard(workspace_namespace, workspace_name, message):
    """Update Terra workspace dashboard with additional text."""

    # https://rawls.dsde-prod.broadinstitute.org/#/workspaces/update_workspace
    uri = f"https://rawls.dsde-prod.broadinstitute.org/api/workspaces/{workspace_namespace}/{workspace_name}"

    # Get access token and and add to headers for requests.
    # -H  "accept: application/json" -H  "Authorization: Bearer [token]"
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