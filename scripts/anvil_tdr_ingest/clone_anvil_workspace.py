"""Clone AnVIL Terra workspace and paste link of source workspace in dashboard of cloned workspace.

Usage:
    > python3 clone_anvil_workspace.py -sw [src_workspace] -sn [src_namespace] -dw [dest_workspace] -dn [dest_namespace] [-ads]"""

import argparse
import json
import pandas as pd
import requests

from utils import add_user_to_workspace, add_workspace_data, \
    check_workspace_exists, clone_workspace, \
    copy_objects_across_buckets, get_workspace_attributes, \
    update_workspace_dashboard, make_create_workspace_request, \
    get_workspace_authorization_domain, get_workspace_bucket


def create_update_entity_request(attribute_name, attribute_value, attribute_description):
    """Return request string for a single operation to create a non-array attribute."""

    data = '{"op":"AddUpdateAttribute", "attributeName":"' + attribute_name + '", "addUpdateAttribute":"' + attribute_value + '"}, \
            {"op":"AddUpdateAttribute", "attributeName":"__DESCRIPTION__' + attribute_name + '", "addUpdateAttribute":"' + attribute_description + '"}'

    return data


def create_clone_workspace_attributes(workspace_link, workspace_bucket):
    """Create request to add src workspace and bucket information to dest workspace variables."""

    workspace_attributes = []

    # get source workspace specific attributes
    workspace_attributes.append(create_update_entity_request("data_files_src_bucket", workspace_bucket, "GCS bucket ID for source workspace"))
    workspace_attributes.append(create_update_entity_request("src_workspace_link", workspace_link, "Link to source workspace"))

    # get template workspace attributes
    template_ws_attrs = get_workspace_attributes("anvil_cmg_ingest_resources", "dsp-data-ingest")["workspace"]["attributes"]
    workspace_attributes.append(create_update_entity_request("tf_input_dir", template_ws_attrs["tf_input_dir"], "input metadata directory"))
    workspace_attributes.append(create_update_entity_request("tf_output_dir", template_ws_attrs["tf_output_dir"], "output metadata directory"))
    workspace_attributes.append(create_update_entity_request("val_output_dir", template_ws_attrs["val_output_dir"], "validation output directory"))
    workspace_attributes.append(create_update_entity_request("tdr_schema_file", template_ws_attrs["tdr_schema_file"], "time TDR dataset schema file"))

    workspace_attributes_request = "[" + ",".join(workspace_attributes) + "]"

    return workspace_attributes_request


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
        print(f"Workspace already exists with name: {dest_namespace}/{dest_workspace}.")
        print(f"Please choose unique name or delete existing workspace and try again.")
        print(f"Existing workspace details: {json.dumps(json.loads(ws_exists_message), indent=2)}")
        return True, ws_exists_message

    ## workspace doesn't exist (404), create workspace
    return False, ws_exists_message


def setup_anvil_workspace_clone(src_namespace, src_workspace, dest_namespace, dest_workspace, auth_domains=None):
    """Clone workspace and update dashboard with url to source workspace."""

    print(f"Starting clone of workspace. \n")

    # check if workspace with name already exists
    clone_exists, clone_exists_message = check_clone_workspace_exists(dest_namespace, dest_workspace)
    if clone_exists:                    # workspace exists --> prompt user to pick new name and exit script
        return

    # AUTH DOMAINS #
    # get auth domains of src workspace
    src_auth_domains = get_workspace_authorization_domain(src_workspace, src_namespace)
    dest_auth_domains = format_authorization_domains(src_auth_domains, auth_domains)
    print(f"Authorization Domain(s) for {dest_namespace}/{dest_workspace}: {dest_auth_domains}")

    # WORKSPACE CLONING #
    is_cloned, cloned_message = clone_workspace(src_namespace, src_workspace, dest_namespace, dest_workspace, dest_auth_domains)
    if not is_cloned:                   # workspace clone fails
        return

    # workspace clone success --> get src and dest workspace links
    src_workspace_link = f"https://app.terra.bio/#workspaces/{src_namespace}/{src_workspace}".replace(" ", "%20")
    dest_workspace_link = f"https://app.terra.bio/#workspaces/{dest_namespace}/{dest_workspace}".replace(" ", "%20")
    print(f"Destinationworkspace link: {dest_workspace_link}")

    # SET UP CLONED WORKSPACE #
    # get src workspace bucket id
    is_bucket, bucket_id_message = get_workspace_bucket(src_workspace, src_namespace)
    if not is_bucket:                   # if getting bucket id fails
        return
    src_bucket_id = bucket_id_message["workspace"]["bucketName"]
    src_bucket_path = f"gs://{src_bucket_id}"
    dest_bucket_id = cloned_message["bucketName"]

    # add src workspace link and src workspace bucket path as workspace variables in dest workspace
    workspace_data_request = create_clone_workspace_attributes(src_workspace_link, src_bucket_id)
    is_workspace_data_added, workspace_data_added_message = add_workspace_data(dest_workspace, dest_namespace, workspace_data_request)
    if not is_workspace_data_added:     # if workspace data variable update fails
        return

    # add jade SA as READER on destionation workspace
    jade_sa = "datarepo-jade-api@terra-datarepo-production.iam.gserviceaccount.com"
    add_user, add_user_message = add_user_to_workspace(dest_workspace, dest_namespace, jade_sa)
    if not add_user:                    # if adding jade SA fails
        return

    # copy notebooks and dataset schema from template workspace to destination workspace
    anvil_resources_workspace_bucket = "fc-9cd4583e-7855-4b5e-ae88-d8971cfd5b46"  # public, no auth domains
    dirs_to_copy = ["notebooks", "resources", "ingest_pipeline/output/tim_core/schema"]
    for dir in dirs_to_copy:
        copy_objects_across_buckets(anvil_resources_workspace_bucket, dest_bucket_id, dir)

    # clone and workspace dashboard update success
    print(f"Destination workspace clone and set-up success.")


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
