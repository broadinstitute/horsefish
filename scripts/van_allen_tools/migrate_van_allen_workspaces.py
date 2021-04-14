"""Create workspaces, set up bucket in us-central region, add workspace access to users.

Usage:
    > python3 set_up_vanallen_workspaces.py -t TSV_FILE [-p NAMESPACE] """

import argparse
import json
import pandas as pd
import requests
from firecloud import api as fapi
from utils import add_tags_to_workspace, check_workspace_exists, \
    get_access_token, get_workspace_authorization_domain, \
    get_workspace_bucket, get_workspace_members, get_workspace_tags, \
    write_output_report


NAMESPACE = "vanallen-firecloud-nih"
BUCKET_REGION = "us-central1"


def add_members_to_workspace(workspace_name, acls, namespace=NAMESPACE):
    """Add members to workspace permissions."""
    json_request = make_add_members_to_workspace_request(acls)

    # request URL for updateWorkspaceACL
    uri = f"https://api.firecloud.org/api/workspaces/{namespace}/{workspace_name}/acl?inviteUsersNotFound=false"

    # Get access token and and add to headers for requests.
    headers = {"Authorization": "Bearer " + get_access_token(), "accept": "*/*", "Content-Type": "application/json"}
    # -H  "accept: */*" -H  "Authorization: Bearer [token] -H "Content-Type: application/json"

    # capture response from API and parse out status code
    response = requests.patch(uri, headers=headers, data=json_request)
    status_code = response.status_code

    emails = [acl['email'] for acl in json.loads(json_request)]
    # print success or fail message based on status code
    if status_code != 200:
        print(f"WARNING: Failed to update {namespace}/{workspace_name} with the following user(s)/group(s): {emails}.")
        print("Check output file for error details.")
        return False, response.text

    print(f"Successfully updated {namespace}/{workspace_name} with the following user(s)/group(s): {emails}.")
    emails_str = ("\n".join(emails))  # write list of emails as strings on new lines
    return True, emails_str


def create_workspace(workspace_name, auth_domains, namespace=NAMESPACE):
    """Create the Terra workspace."""
    # check if workspace already exists
    ws_exists, ws_exists_response = check_workspace_exists(workspace_name, namespace)

    if ws_exists is None:
        return False, ws_exists_response

    if not ws_exists:  # workspace doesn't exist (404), create workspace
        # format auth_domain_response
        auth_domain_names = json.loads(auth_domains)["workspace"]["authorizationDomain"]
        # create request JSON
        create_ws_json = make_create_workspace_request(workspace_name, auth_domain_names, namespace)  # json for API request

        # request URL for createWorkspace (rawls) - bucketLocation not supported in orchestration
        uri = f"https://rawls.dsde-prod.broadinstitute.org/api/workspaces"

        # Get access token and and add to headers for requests.
        # -H  "accept: application/json" -H  "Authorization: Bearer [token] -H  "Content-Type: application/json"
        headers = {"Authorization": "Bearer " + get_access_token(), "accept": "application/json", "Content-Type": "application/json"}

        # capture response from API and parse out status code
        response = requests.post(uri, headers=headers, data=json.dumps(create_ws_json))
        status_code = response.status_code

        if status_code != 201:  # ws creation fail
            print(f"WARNING: Failed to create workspace with name: {workspace_name}. Check output file for error details.")
            return False, response.text
        # workspace creation success
        print(f"Successfully created workspace with name: {workspace_name}.")
        return True, None

    # workspace already exists
    print(f"Workspace already exists with name: {namespace}/{workspace_name}.")
    print(f"Existing workspace details: {json.dumps(json.loads(ws_exists_response), indent=2)}")
    # make user decide if they want to update/overwrite existing workspace
    while True:  # try until user inputs valid response
        update_existing_ws = input("Would you like to continue modifying the existing workspace? (Y/N)" + "\n")
        if update_existing_ws.upper() in ["Y", "N"]:
            break
        else:
            print("Not a valid option. Choose: Y/N")
    if update_existing_ws.upper() == "N":       # don't overwrite existing workspace
        deny_overwrite_message = f"{namespace}/{workspace_name} already exists. User selected not to overwrite. Try again with unique workspace name."
        return None, deny_overwrite_message

    accept_overwrite_message = f"{namespace}/{workspace_name} already exists. User selected to overwrite."
    return True, accept_overwrite_message    # overwrite existing workspace - 200 status code for "Y"


def make_add_members_to_workspace_request(response_text):
    """Make the json request to pass into add_members_to_workspace()."""
    # load response from getWorkslaceACLs
    workspace_members = json.loads(response_text)

    # reformat it to be request format for updating ACLs on new workspace
    acls_to_add = []
    for key, value, in workspace_members["acl"].items():  # need to un-nest one level and add key as kvp into value
        new_value = value
        new_value["email"] = key
        acls_to_add.append(new_value)

    add_acls_request = json.dumps(acls_to_add)
    return add_acls_request


def make_create_workspace_request(workspace_name, auth_domains, namespace=NAMESPACE):
    """Make the json request to pass into create_workspace()."""
    # initialize empty dictionary
    create_ws_request = {}

    create_ws_request["namespace"] = namespace
    create_ws_request["name"] = workspace_name
    create_ws_request["authorizationDomain"] = auth_domains
    create_ws_request["attributes"] = {}
    create_ws_request["noWorkspaceOwner"] = False
    # specific to van allen lab - migrating to this region
    create_ws_request["bucketLocation"] = BUCKET_REGION

    return create_ws_request


def setup_single_workspace(workspace):
    """Create one workspace and set ACLs."""
    # initialize workspace dictionary with default values assuming failure
    workspace_dict = {"original_workspace_name": "NA", "original_workspace_namespace": "NA",
                      "new_workspace_name": "NA", "new_workspace_namespace": "NA",
                      "workspace_link": "Incomplete", "workspace_bucket": "Incomplete",
                      "workspace_creation_error": "NA",
                      "workspace_ACLs": "Incomplete", "workspace_ACLs_error": "NA",
                      "workspace_tags": "Incomplete", "workspace_tags_error": "NA",
                      "final_workspace_status": "Failed"}

    # workspace creation
    # capture original workspace details
    original_workspace_name = workspace["original_workspace_name"]
    original_workspace_namespace = workspace["original_workspace_namespace"]
    workspace_dict["original_workspace_name"] = original_workspace_name
    workspace_dict["original_workspace_namespace"] = original_workspace_namespace

    # capture new workspace details
    new_workspace_name = workspace["new_workspace_name"]
    new_workspace_namespace = workspace["new_workspace_namespace"]
    workspace_dict["new_workspace_name"] = new_workspace_name
    workspace_dict["new_workspace_namespace"] = new_workspace_namespace

    # get original workspace authorization domain
    get_ad_success, get_ad_message = get_workspace_authorization_domain(original_workspace_name, original_workspace_namespace)

    if not get_ad_success:
        return workspace_dict

    # create workspace (pass in auth domain response.text)
    create_ws_success, create_ws_message = create_workspace(new_workspace_name, get_ad_message, new_workspace_namespace)

    workspace_dict["workspace_creation_error"] = create_ws_message

    if not create_ws_success:
        return workspace_dict

    # ws creation success
    workspace_dict["workspace_link"] = (f"https://app.terra.bio/#workspaces/{new_workspace_namespace}/{new_workspace_name}").replace(" ", "%20")

    # get the newly created workspace bucket
    get_bucket_success, get_bucket_message = get_workspace_bucket(new_workspace_name, new_workspace_namespace)

    if not get_bucket_success:
        workspace_dict["workspace_bucket"] = get_bucket_message
        return workspace_dict

    bucket_id = "gs://" + json.loads(get_bucket_message)["workspace"]["bucketName"]
    workspace_dict["workspace_bucket"] = bucket_id

    # get original workspace ACLs json - not including auth domain
    get_workspace_members_success, workspace_members_message = get_workspace_members(original_workspace_name, original_workspace_namespace)

    # if original workspace ACLs could not be retrieved - stop workspace setup
    if not get_workspace_members_success:
        workspace_dict["workspace_ACLs_error"] = workspace_members_message
        return workspace_dict

    # add ACLs to workspace if workspace creation success
    add_member_success, add_member_message = add_members_to_workspace(new_workspace_name, workspace_members_message, new_workspace_namespace)

    if not add_member_success:
        workspace_dict["workspace_ACLs_error"] = add_member_message
        return workspace_dict

    # adding ACLs to workspace success
    workspace_dict["workspace_ACLs"] = add_member_message  # update dict with ACL emails

    # add tags from original workspace to new workspace
    get_tags_success, get_tags_message = get_workspace_tags(original_workspace_name, original_workspace_namespace)

    if not get_tags_success:  # if get tags fails
        workspace_dict["workspace_tags_error"] = get_tags_message
        return workspace_dict

    add_tags_success, add_tags_message = add_tags_to_workspace(new_workspace_name, get_tags_message, new_workspace_namespace)

    if not add_tags_success:  # if add tags fails
        workspace_dict["workspace_tags_error"] = add_tags_message
        return workspace_dict

    print(f"Successfully updated {new_workspace_namespace}/{new_workspace_namespace} with the following tags: {add_tags_message}")
    workspace_dict["workspace_tags"] = add_tags_message
    workspace_dict["final_workspace_status"] = "Success"  # final workspace setup step

    return workspace_dict

def find_and_replace(attr, value, replace_this, with_this):

    updated_attr = None
    if isinstance(value, str):  # if value is just a string
        if replace_this in value:
            new_value = value.replace(replace_this, with_this)
            updated_attr = fapi._attr_set(attr, new_value)
    elif isinstance(value, dict):
        if replace_this in str(value):
            value_str = str(value)
            value_str_new = value_str.replace(replace_this, with_this)
            value_new = ast.literal_eval(value_str_new)
            updated_attr = fapi._attr_set(attr, value_new)
    elif isinstance(value, (bool, int, float, complex)):
        pass
    elif value is None:
        pass
    else:  # some other type, hopefully this doesn't exist
        print('unknown type of attribute')
        print('attr: ' + attr)
        print('value: ' + str(value))

    return updated_attr

def update_entities(workspace_name, workspace_project, replace_this, with_this):
    """Update Data Table"""
    ## update workspace entities
    print("Updating DATA ENTITIES for " + workspace_name)

    # get data attributes
    response = fapi.get_entities_with_type(workspace_project, workspace_name)
    entities = response.json()

    for ent in entities:
        ent_name = ent['name']
        ent_type = ent['entityType']
        ent_attrs = ent['attributes']
        attrs_list = []
        for attr in ent_attrs.keys():
            value = ent_attrs[attr]
            updated_attr = find_and_replace(attr, value, replace_this, with_this)
            if updated_attr:
                attrs_list.append(updated_attr)

        if len(attrs_list) > 0:
            response = fapi.update_entity(workspace_project, workspace_name, ent_type, ent_name, attrs_list)
            if response.status_code == 200:
                print('Updated entities:')
                for attr in attrs_list:
                    print('   '+str(attr['attributeName'])+' : '+str(attr['addUpdateAttribute']))


def copy_workspace_entities(migration_data):
    """Copy Van Allen Lab workspaces Data Table."""
    # update workspace entities
    original_workspace_name = migration_data["original_workspace_name"]
    original_workspace_namespace = migration_data["original_workspace_namespace"]
    new_workspace_name = migration_data["new_workspace_name"]
    new_workspace_namespace = migration_data["new_workspace_namespace"]

    # Set up the migration_data dict
    migration_data_with_dt = migration_data

    # Default copy_successful
    copy_successful = True

    # Default Error
    error = "NA"

    # get data attributes and copy non-set data table to workspace
    try:
        response = fapi.get_entities_with_type(original_workspace_namespace, original_workspace_name)
        entities = response.json()
        ent_type_before = None
        ent_names = []
        set_list = {}
        for ent in entities:
            ent_name = ent['name']
            ent_type = ent['entityType']
            if ent == entities[-1] or (ent_type_before and ent_type != ent_type_before):
                if ent == entities[-1]:
                    ent_names.append(ent_name)
                    ent_type_before = ent_type
                if "_set" in ent_type_before:
                    set_list[ent_type_before] = ent_names
                else:
                    fapi.copy_entities(original_workspace_namespace, original_workspace_name, new_workspace_namespace, new_workspace_name, ent_type_before, ent_names, link_existing_entities=True)
                    print(f"Copied {ent_type_before} data table: over to {new_workspace_namespace}/{new_workspace_name}")
                ent_names = []
            ent_names.append(ent_name)
            ent_type_before = ent_type

        # copy set Data Table to workspace
        for etype, enames in set_list.items():
            fapi.copy_entities(original_workspace_namespace, original_workspace_name, new_workspace_namespace, new_workspace_name, etype, enames, link_existing_entities=True)
            print(f"Copied {etype} data table: over to {new_workspace_namespace}/{new_workspace_name}")

        # Check if data tables match
        new_workspace_response = fapi.get_entities_with_type(new_workspace_namespace, new_workspace_name)
        new_entities = new_workspace_response.json()
        if entities != new_entities:
            print(f"Error: Data Tables don't match")
            error = f"Error: Data Tables don't match"

        # Get original workpace bucket
        get_bucket_success, get_bucket_message = get_workspace_bucket(original_workspace_name, original_workspace_namespace)
        original_bucket = json.loads(get_bucket_message)["workspace"]["bucketName"]
        print(f"Original Bucket: {original_bucket}")

        # Get new workpace bucket
        new_bucket = migration_data["workspace_bucket"].replace("gs://", "")
        print(f"New Bucket: {new_bucket}")

        # update bucket links
        update_entities(new_workspace_name, new_workspace_namespace, replace_this=original_bucket, with_this=new_bucket)
        print("Updated Data Table with new bucket path")
    except Exception as e:
        copy_successful = False
        print(f"Error: {e}")
        error = f"Error: {e}"

    migration_data_with_dt["copy_data_table"] = copy_successful
    migration_data_with_dt["data_table_error"] = error

    return migration_data_with_dt


def copy_workspaces_workflows(migration_data):
    """Copy Van Allen Lab workspaces Workflows."""
    # set workspace and namespace
    original_workspace_name = migration_data["original_workspace_name"]
    original_workspace_namespace = migration_data["original_workspace_namespace"]
    new_workspace_name = migration_data["new_workspace_name"]
    new_workspace_namespace = migration_data["new_workspace_namespace"]

    # Set up the migration_data dict
    migration_data_full = migration_data

    # Default copy_successful
    copy_successful = True

    # Default Error
    error = "NA"

    # Get the list of all the workflows
    try:
        workflow_list = fapi.list_workspace_configs(original_workspace_namespace, original_workspace_name)

        for workflow in workflow_list.json():
            # Get workflow config (overview config)
            workflow_config = workflow['methodRepoMethod']

            # Get workspace config (Detailed config with inputs, oututs, etc)
            workspace_config = fapi.get_workspace_config(original_workspace_namespace, original_workspace_name, workflow_config['methodNamespace'], workflow_config['methodName'])

            # Create a workflow based on Detailed config
            fapi.create_workspace_config(new_workspace_namespace, new_workspace_name, workspace_config.json())
            print(f"Copied {workflow_config['methodName']} workflow : over to {new_workspace_namespace}/{new_workspace_name}")
    except Exception as e:
        copy_successful = False
        print(f"Error: {e}")
        error = f"Error: {e}"

    # Adding column to tsv
    migration_data_full["copy_workflow"] = copy_successful
    migration_data_full["workflow_error"] = error

    return migration_data_full


def migrate_workspaces(tsv):
    """Create and set up migrated workspaces."""
    # read full tsv into dataframe
    setup_info_df = pd.read_csv(tsv, sep="\t")

    # create df for output tsv file
    col_names = ["original_workspace_name", "original_workspace_namespace",
                 "new_workspace_name", "new_workspace_namespace",
                 "workspace_link", "workspace_bucket",
                 "workspace_creation_error",
                 "workspace_ACLs", "workspace_ACLs_error",
                 "workspace_tags", "workspace_tags_error",
                 "final_workspace_status", "copy_data_table", 
                 "data_table_error", "copy_workflow", "workflow_error"]

    all_row_df = pd.DataFrame(columns=col_names)

    # per row in tsv/df
    for index, row in setup_info_df.iterrows():

        # Create Workspace
        migration_data = setup_single_workspace(row)

        # Copy over data table
        migration_data_with_dt = copy_workspace_entities(migration_data)

        # Copy over workflows
        migration_data_full = copy_workspaces_workflows(migration_data_with_dt)

        # Create output tsv
        migration_data_df = all_row_df.append(migration_data_full, ignore_index=True)

    # Create the report
    write_output_report(migration_data_df)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Set-up Van Allen Lab workspaces.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with original and new workspace details.')

    args = parser.parse_args()

    # call to create and set up workspaces
    migrate_workspaces(args.tsv)
