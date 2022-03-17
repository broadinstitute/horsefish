"""Clone AnVIL Terra workspace and paste link of original workspace in dashboard of cloned workspace.

Usage:
    > python3 clone_anvil_workspace.py -sw [src_workspace] -sn [src_namespace] -dw [dest_workspace] -dn [dest_namespace]"""

import argparse
import json
import pandas as pd
import requests

from utils import clone_workspace, update_workspace_dashboard

def create_non_array_attr_operation(var_attribute_name, var_attribute_value):
    """Return request string for a single operation to create a non-array attribute."""

    return '[{"op":"AddUpdateAttribute","attributeName":"' + var_attribute_name + '", "addUpdateAttribute":"' + var_attribute_value + '"}]'


def setup_anvil_workspace_clone(src_namespace, src_workspace, dest_namespace, dest_workspace, auth_domain=""):
    """Clone AnVIL workspace and update dashboard with url to original workspace."""

    print(f"Starting clone of workspace.")
    is_cloned, cloned_message = clone_workspace(src_namespace, src_workspace, dest_namespace, dest_workspace, auth_domain)

    # if workspace clone fail
    if not is_cloned:
        return

    src_workspace_link = f"https://app.terra.bio/#workspaces/{src_namespace}/{src_workspace}".replace(" ", "%20")
    dashboard_message = create_non_array_attr_operation("description", src_workspace_link)
    print(dashboard_message)
    is_updated, updated_message = update_workspace_dashboard(dest_namespace, dest_workspace, dashboard_message)

    # if workspace dashboard update fail
    if not is_updated:
        return

    # if clone and dashboard update success
    print(f"Workspace clone and dashboard update success.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Clone AnVIL workspace and update cloned workspace dashboard with original workspace link.')

    parser.add_argument('-sw', '--src_workspace', required=True, type=str, help='original Terra workspace name')
    parser.add_argument('-sn', '--src_namespace', required=True, type=str, help='original Terra workspace namespace')
    parser.add_argument('-dw', '--dest_workspace', required=True, type=str, help='destination Terra workspace name')
    parser.add_argument('-dn', '--dest_namespace', required=True, type=str, help='destination Terra workspace namespace')
    parser.add_argument('-ad', '--auth_domain', type=str, help='destination authorization domain')

    args = parser.parse_args()

    # call to create and set up external data delivery workspaces
    setup_anvil_workspace_clone(args.src_namespace, args.src_workspace, args.dest_namespace, args.dest_workspace, args.auth_domain)

# python3 clone_anvil_workspace.py -sw sushmac_sandbox_broad-firecloud-dsde -sn broad-firecloud-dsde -dw delete_test_dfe730 -dn broad-firecloud-dsde
# TODO: if cloned workspace exists already
# TODO: if src workspace has auth domain, carry it over