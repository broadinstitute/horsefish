"""Common Terra-related functions used for TDR scripts."""


import requests
import pandas as pd

from firecloud import api as fapi
from pprint import pprint
from six.moves.urllib.parse import urlencode

from utils import get_access_token, get_headers


def get_workspace_bucket(ws_project, ws_name):
    """Return the workspace bucket name - this does not include the gs:// prefix"""
    ws_response = fapi.get_workspace(ws_project, ws_name).json()

    return ws_response['workspace']['bucketName']


def upload_tsv_to_terra(entities_tsv, ws_project, ws_name):

    with open(entities_tsv, "r") as tsv:
        entity_data = tsv.read()

    uri = f"https://api.firecloud.org/api/workspaces/{ws_project}/{ws_name}/flexibleImportEntities?async=false"

    body = urlencode({"entities": entity_data})

    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "*/*",
               "Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(uri, headers=headers, data=body)
    status_code = response.status_code

    if status_code != 200:
        return response.text

    print(f"Successfully loaded entity table from file {entities_tsv}.")
    return response.text


def get_workspace_AD_list(workspace_project, workspace_name):
    """Returns a list of strings containing the authorization domain email address(es) if any exist.
    If no AD exists for the workspace, returns None."""
    response = fapi.get_workspace(workspace_project, workspace_name, fields="workspace.authorizationDomain")

    if response.status_code != 200:
        print(f"attempt to retrieve workspace AD failed for {workspace_project}/{workspace_name}")
        print(response.text)
        exit(1)

    response_json = response.json()
    '''
    response returns the following if no AD:
    {
        "workspace": {
             "authorizationDomain": []
        }
    }

    and the following if AD:
    {
    "workspace": {
        "authorizationDomain": [
        {
            "membersGroupName": "dsp_fieldeng"
        }
        ]
    }
    }
    '''

    ad_list = response_json['workspace']['authorizationDomain']

    if ad_list:
        return [ad['membersGroupName'] + '@firecloud.org' for ad in ad_list]

    # otherwise there is no AD
    return None


def get_workspace_groups(workspace_project, workspace_name):
    """Returns a dict of ALL workspace group emails, where the key is the
    access level (e.g. 'reader' or 'owner') and the value is the group email."""
    # first get workspace id
    uri = f"https://api.firecloud.org/api/workspaces/{workspace_project}/{workspace_name}?fields=workspace.workspaceId"

    response = requests.get(uri, headers=get_headers())
    status_code = response.status_code

    if status_code != 200:
        print(f"retrieving workspace info failed for workspace {workspace_project}/{workspace_name}")
        return response.text

    workspace_id = response.json()['workspace']['workspaceId']

    # now get reader policy group
    uri = f"https://sam.dsde-prod.broadinstitute.org/api/resources/v1/workspace/{workspace_id}/policies"

    response = requests.get(uri, headers=get_headers())
    status_code = response.status_code

    if status_code != 200:
        print(f"retrieving workspace policy group failed for workspaceId {workspace_id}")
        return response.text

    workspace_groups = dict()
    # this just gets the reader group - TODO should we add all groups?
    for policy_dict in response.json():
        workspace_groups[policy_dict["policyName"]] = policy_dict["email"]

    return workspace_groups


def create_set(set_name, entity_list, ws_project, ws_name, root_etype="sample"):
    """Creates a new {root_etype}_set, e.g. sample_set called {set_name} based on a list of {root_etype} ids."""
    df_set_table = pd.DataFrame({f"membership:{root_etype}_set_id": [set_name]*len(entity_list),
                                 root_etype: entity_list})

    # save to tsv
    set_tsv = f"{set_name}_{root_etype}_set_table.tsv"
    df_set_table.to_csv(set_tsv, sep="\t", index=False)

    # upload to Terra
    response = fapi.upload_entities_tsv(ws_project, ws_name, set_tsv, model="flexible")

    if response.status_code != 200:
        print("ERROR")
        print(response)
        pprint(response.text)
    else:
        print(f"successfully created new {root_etype}_set {set_name} containing {len(entity_list)} {root_etype}s")

    return