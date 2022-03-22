"""Clone AnVIL Terra workspace and paste link of source workspace in dashboard of cloned workspace.

Usage:
    > python3 clone_anvil_workspace.py -sw [src_workspace] -sn [src_namespace] -dw [dest_workspace] -dn [dest_namespace] [-ads]"""

import argparse
import json
import pandas as pd
import requests

from utils import check_workspace_exists, clone_workspace, \
    update_workspace_dashboard, make_create_workspace_request, \
    get_workspace_authorization_domain

def create_update_entity_request(attribute_name, attribute_value):
    """Return request string for a single operation to create a non-array attribute."""

    return '[{"op":"AddUpdateAttribute","attributeName":"' + attribute_name + '", "addUpdateAttribute":"' + attribute_value + '"}]'


def format_authorization_domains(src_auth_domains, input_auth_domains):
    """Create list of authorization domains to apply to destination/clone Terra workspace."""

    # src workspace ADs, user input ADs
    if src_auth_domains and input_auth_domains is not None:
        all_auth_domains = src_auth_domains.append(input_auth_domains.split(" "))
    # src workspace ADs, no user input ADs
    if src_auth_domains and input_auth_domains is None:
        all_auth_domains = src_auth_domains
    # no src workspace ADs, user input ADs
    if not src_auth_domains and input_auth_domains is not None:
        all_auth_domains = input_auth_domains.split(" ")
    # no src workspace ADs, no user input ADs
    if not src_auth_domains and input_auth_domains is None:
        all_auth_domains = []

    return all_auth_domains


def check_clone_workspace_exists(dest_namespace, dest_workspace):
    """Check if a Terra workspace with user input name for clone already eixsts."""

    # check workspace exists
    ws_exists, ws_exists_message = check_workspace_exists(dest_workspace, dest_namespace)

    # if workspace exists
    if ws_exists:
        print(f"Workspace already exists with name: {dest_namepsace}/{dest_workspace}.")
        print(f"Please choose unique name or delete existing workspace and try again.")
        print(f"Existing workspace details: {json.dumps(json.loads(ws_exists_message), indent=2)}")
        return True, ws_exists_message

    ## workspace doesn't exist (404), create workspace
    return False, ws_exists_message


def setup_anvil_workspace_clone(src_namespace, src_workspace, dest_namespace, dest_workspace, auth_domains=None):
    """Clone workspace and update dashboard with url to source workspace."""

    print(f"Starting clone of workspace.")

    # check if workspace with name already exists
    clone_exists, clone_exists_message = check_clone_workspace_exists(dest_namespace, dest_workspace)

    # workspace exists --> prompt user to pick new name and exit script
    if clone_exists:
        return

    # workspace does not exist --> start set up for cloning workspace
    # get auth domains of src workspace
    src_auth_domains = get_workspace_authorization_domain(src_workspace, src_namespace)
    dest_auth_domains = format_authorization_domains(src_auth_domains, auth_domains)

    print(f"Authorization Domain(s) for {dest_namespace}/{dest_workspace}: {dest_auth_domains}")
    is_cloned, cloned_message = clone_workspace(src_namespace, src_workspace, dest_namespace, dest_workspace, dest_auth_domains)

    # workspace clone fails
    if not is_cloned:
        return

    # workspace clone success
    src_workspace_link = f"https://app.terra.bio/#workspaces/{src_namespace}/{src_workspace}".replace(" ", "%20")
    dashboard_message = f"Source Workspace URL: {src_workspace_link}"
    update_dashboard_request = create_update_entity_request("description", src_workspace_link)
    is_updated, updated_message = update_workspace_dashboard(dest_namespace, dest_workspace, update_dashboard_request)
    print(f"Terra workspace dashboard will be updated with message: {dashboard_message}")

    # workspace dashboard update fails
    if not is_updated:
        return

    # clone and workspace dashboard update success
    print(f"Workspace clone and dashboard update success.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Clone AnVIL workspace and update cloned workspace dashboard with source workspace link.')

    parser.add_argument('-sw', '--src_workspace', required=True, type=str, help='source Terra workspace name')
    parser.add_argument('-sn', '--src_namespace', required=True, type=str, help='source Terra workspace namespace')
    parser.add_argument('-dw', '--dest_workspace', required=True, type=str, help='destination Terra workspace name')
    parser.add_argument('-dn', '--dest_namespace', required=True, type=str, help='destination Terra workspace namespace')
    parser.add_argument('-ad', '--auth_domains', type=str, help='desired destination authorization domain(s) separated by spaces')

    args = parser.parse_args()

    # call to create and set up external data delivery workspaces
    setup_anvil_workspace_clone(args.src_namespace, args.src_workspace, args.dest_namespace, args.dest_workspace, args.auth_domains)
