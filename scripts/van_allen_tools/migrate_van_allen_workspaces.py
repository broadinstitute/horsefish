"""Create workspaces, set up bucket in us-central region, add workspace access to users.

Usage:
    > python3 set_up_vanallen_workspaces.py -t TSV_FILE [-p NAMESPACE] """

import argparse
import json
import ast
import pandas as pd
import requests
from firecloud import api as fapi
from utils import add_tags_to_workspace, check_workspace_exists, \
    get_access_token, get_workspace_authorization_domain, \
    get_workspace_bucket, get_workspace_members, get_workspace_tags, \
    write_output_report
import query_bucket_object_inventory as get_source_objects_file


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


def copy_workspace_workflows(destination_workspace_namespace, destination_workspace_name, source_workspace_namespace, source_workspace_name):
    """Copy workflows from source workspace to a destination workspace."""

    # get the list of all the workflows in source workspaces - allRepos = agora, dockstore
    try:
        source_workflows = fapi.list_workspace_configs(source_workspace_namespace, source_workspace_name, allRepos=True)

        # Store all the workflow names
        source_workflow_names = []
        destination_workflow_names = []
        workflow_copy_errors = []

        for workflow in source_workflows.json():
            # get workflow name and add to source workflow names list
            source_workflow_name = workflow['name']
            source_workflow_namespace = workflow['namespace']
            source_workflow_names.append(source_workflow_name)

            # get full source workflow configuration (detailed config with inputs, oututs, etc) for single workflow
            source_workflow_config = fapi.get_workspace_config(source_workspace_namespace, source_workspace_name, source_workflow_namespace, source_workflow_name)

            # create a workflow based on source workflow config returned above
            response = fapi.create_workspace_config(destination_workspace_namespace, destination_workspace_name, source_workflow_config.json())
            status_code = response.status_code

            # if copy failed (409 = already exists, does not count as failure)
            if status_code not in [201, 409]:
                workflow_copy_errors.append(response.text)

            # if copy successful, append workflow name to destination workflow list
            destination_workflow_names.append(source_workflow_name)

        # check if workflows in source and destination workspaces match
        if source_workflow_names.sort() != destination_workflow_names.sort():
            missing_workflows = list(set(source_workflow_names) - set(destination_workflow_names))
            print(f"WARNING: Failed to copy the following workflows to {destination_workspace_namespace}/{destination_workspace_name}: {missing_workflows}. Check output file for details.")
            return False, workflow_copy_errors

        print(f"Successfully copied all workflows from {source_workspace_namespace}/{source_workspace_namespace} to {destination_workspace_namespace}/{destination_workspace_namespace}")
        return True, destination_workflow_names

    except Exception as error:
        print(f"WARNING: Workflow copying failed due to: {error}")
        return False, error


def find_and_replace(attr, value, replace_this, with_this):
    """Replace an attribute in a data table row."""
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
    """Update data model tables with new destination workspace bucket file paths."""
    # update workspace entities
    print(f"Starting update of data tables in workspace: {workspace_name}")

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
                    print('   ' + str(attr['attributeName']) + ' : ' + str(attr['addUpdateAttribute']))


def copy_workspace_entities_sushma(destination_workspace_namespace, destination_workspace_name, source_workspace_namespace, source_workspace_name, destination_workspace_bucket):
    """Copy workspace data tables to destination workspace."""

    source_etypes = fapi.list_entity_types(source_workspace_namespace, source_workspace_name)
    if source_etypes.status_code != 200:  # getting list of data tables fails
        message = f"Failed to retrieve list of data tables (entity types) from: {source_workspace_namespace}/{source_workspace_name}. API error: {source_etypes.text}."
        print(message)
        return False, message
    source_set_etypes = [s for s in list(source_etypes.json().keys()) if s.endswith("_set")]
    source_single_etypes = [s for s in list(source_etypes.json().keys()) if not s.endswith("_set")]

    # for each table that is not a set
    for etype in source_single_etypes:
        # get entity names for etype
        entities = fapi.get_entities(source_workspace_namespace, source_workspace_name, etype)
        if entities.status_code != 200:  # getting an etype's entities fails
            message = f"Failed to retrieve entities (row names) for {etype}. API error: {entities.text}"
            print(message)
            return False, message

        entity_names = [ent["name"] for ent in entities.json()]
        # copy single etype (with entities) to destination workspace
        copy_response = fapi.copy_entities(source_workspace_namespace, source_workspace_name, destination_workspace_namespace, destination_workspace_name, etype, entity_names, link_existing_entities=True)
        if copy_response.status_code not in [201, 409]:  # if copying table with entities fails
            message = f"Failed to copy {etype} with entities({entity_names}) to {destination_workspace_namespace}/{destination_workspace_name}. API error: {copy_response.text}."
            print(message)
            return False, message

    for set_etype in source_set_etypes:
        # get entity names for etype
        set_entities = fapi.get_entities(source_workspace_namespace, source_workspace_name, set_etype)
        if set_entities.status_code != 200:  # getting a set etype's entities fails
            message = f"Failed to retrieve entities (row names) for {set_etype}. API error: {set_entities.text}"
            print(message)
            return False, message

        set_entity_names = [ent["name"] for ent in set_entities.json()]
        # copy single etype (with entities) to destination workspace
        set_copy_response = fapi.copy_entities(source_workspace_namespace, source_workspace_name, destination_workspace_namespace, destination_workspace_name, set_etype, set_entity_names, link_existing_entities=True)
        if set_copy_response.status_code not in [201, 409]:  # if copying set table with entities fails
            message = f"Failed to copy {set_etype} with entities({set_entity_names}) to {destination_workspace_namespace}/{destination_workspace_name}. API error: {set_copy_response.text}."
            print(message)
            return False, message

    print(f"Successfully copied data tables to {destination_workspace_namespace}/{destination_workspace_name}: {list(source_etypes.json().keys())}")
    # get original workpace bucket id
    get_bucket_success, get_bucket_message = get_workspace_bucket(source_workspace_name, source_workspace_namespace)
    # TODO: handle if getting workspace bucket fails
    source_bucket = json.loads(get_bucket_message)["workspace"]["bucketName"]
    destination_bucket = destination_workspace_bucket.replace("gs://", "")

    # update bucket links in the destination workspace so that it matches the path structure of what the WDL generates when it migrates data
    # gs://new_bucket_id/original_bucket_id/[original data structure]
    update_entities(destination_workspace_name, destination_workspace_namespace, replace_this=source_bucket, with_this=f"{destination_bucket}/{source_bucket}")

    print(f"Successfully updated data tables with new bucket paths data tables in {destination_workspace_namespace}/{destination_workspace_name}.")
    return True, list(source_etypes.json().keys())


def copy_workspace_entities(destination_workspace_namespace, destination_workspace_name, source_workspace_namespace, source_workspace_name, destination_workspace_bucket):
    """Copy workspace data tables to destination workspace."""

    # get data attributes and copy non-set data table to workspace
    data_table_name_list = []
    try:
        response = fapi.get_entities_with_type(source_workspace_namespace, source_workspace_name)
        source_entities = response.json()
        ent_type_before = None
        ent_names = []
        set_list = {}
        for ent in source_entities:
            print(ent)
            ent_name = ent['name']
            ent_type = ent['entityType']
            if ent == source_entities[-1] or (ent_type_before and ent_type != ent_type_before):
                if ent == source_entities[-1]:
                    ent_names.append(ent_name)
                    ent_type_before = ent_type
                if "_set" in ent_type_before:
                    set_list[ent_type_before] = ent_names
                else:
                    fapi.copy_entities(source_workspace_namespace, source_workspace_name, destination_workspace_namespace, destination_workspace_name, ent_type_before, ent_names, link_existing_entities=True)
                    # print(f"Successfully copied {ent_type_before} data table over to: {destination_workspace_namespace}/{destination_workspace_name}.")
                    data_table_name_list.append(ent_type_before)
                ent_names = []
            ent_names.append(ent_name)
            ent_type_before = ent_type

        # copy set type data tables to workspace
        for etype, enames in set_list.items():
            fapi.copy_entities(source_workspace_namespace, source_workspace_name, destination_workspace_namespace, destination_workspace_name, etype, enames, link_existing_entities=True)
            # print(f"Successfully copied {etype} data table over to: {destination_workspace_namespace}/{destination_workspace_name}")
            data_table_name_list.append(etype)

        # Check if data tables match
        destination_entities = fapi.get_entities_with_type(destination_workspace_namespace, destination_workspace_name).json()

        if source_entities != destination_entities:
            print(f"Error: Data Tables don't match")
            return False, f"Error: Not all source data tables were copied over to the destination workspace."

        # Get original workpace bucket
        get_bucket_success, get_bucket_message = get_workspace_bucket(source_workspace_name, source_workspace_namespace)
        # TODO: handle if getting workspace bucket fails
        source_bucket = json.loads(get_bucket_message)["workspace"]["bucketName"]
        destination_bucket = destination_workspace_bucket.replace("gs://", "")

        # update bucket links in the destination workspace so that it matches the path structure of what the WDL generates when it migrates data
        # gs://new_bucket_id/original_bucket_id/[original data structure]
        update_entities(destination_workspace_name, destination_workspace_namespace, replace_this=source_bucket, with_this=f"{destination_bucket}/{source_bucket}")
        print(f"Successfully updated data tables with new bucket paths.")
    except Exception as error:
        return False, error

    return True, data_table_name_list


def setup_single_workspace(workspace):
    """Create one workspace and set ACLs."""
    # initialize workspace dictionary with default values assuming failure
    workspace_dict = {"source_workspace_name": "NA", "source_workspace_namespace": "NA",
                      "source_workspace_bucket": "Incomplete",
                      "destination_workspace_name": "NA", "destination_workspace_namespace": "NA",
                      "workspace_link": "Incomplete", "destination_workspace_bucket": "Incomplete",
                      "workspace_creation_error": "NA",
                      "workspace_ACLs": "Incomplete", "workspace_ACLs_error": "NA",
                      "workspace_tags": "Incomplete", "workspace_tags_error": "NA",
                      "copy_data_tables": "Incomplete", "copy_data_tables_error": "NA",
                      "copy_workflows": "Incomplete", "copy_workflows_error": "NA",
                      "source_object_details_file": "Incomplete", "source_object_details_file_error": "NA",
                      "final_workspace_status": "Failed"}

    # workspace creation
    # capture original workspace details
    source_workspace_name = workspace["source_workspace_name"]
    source_workspace_namespace = workspace["source_workspace_namespace"]
    workspace_dict["source_workspace_name"] = source_workspace_name
    workspace_dict["source_workspace_namespace"] = source_workspace_namespace

    # get the source workspace bucket
    get_source_bucket_success, get_source_bucket_message = get_workspace_bucket(source_workspace_name, source_workspace_namespace)

    if not get_source_bucket_success:
        workspace_dict["source__workspace_bucket"] = get_source_bucket_message
        return workspace_dict

    source_bucket_id = "gs://" + json.loads(get_source_bucket_message)["workspace"]["bucketName"]
    workspace_dict["source_workspace_bucket"] = source_bucket_id

    # capture new workspace details
    destination_workspace_name = workspace["destination_workspace_name"]
    destination_workspace_namespace = workspace["destination_workspace_namespace"]
    workspace_dict["destination_workspace_name"] = destination_workspace_name
    workspace_dict["destination_workspace_namespace"] = destination_workspace_namespace

    # get original workspace authorization domain
    get_ad_success, get_ad_message = get_workspace_authorization_domain(source_workspace_name, source_workspace_namespace)

    if not get_ad_success:
        return workspace_dict

    # create workspace (pass in auth domain response.text)
    create_ws_success, create_ws_message = create_workspace(destination_workspace_name, get_ad_message, destination_workspace_namespace)

    workspace_dict["workspace_creation_error"] = create_ws_message

    if not create_ws_success:
        return workspace_dict

    # ws creation success
    workspace_dict["workspace_link"] = (f"https://app.terra.bio/#workspaces/{destination_workspace_namespace}/{destination_workspace_name}").replace(" ", "%20")

    # get the newly created workspace bucket
    get_destination_bucket_success, get_destination_bucket_message = get_workspace_bucket(destination_workspace_name, destination_workspace_namespace)

    if not get_destination_bucket_success:
        workspace_dict["destination_workspace_bucket"] = get_destination_bucket_message
        return workspace_dict

    destination_bucket_id = "gs://" + json.loads(get_destination_bucket_message)["workspace"]["bucketName"]
    workspace_dict["destination_workspace_bucket"] = destination_bucket_id

    # get original workspace ACLs json - not including auth domain
    get_workspace_members_success, workspace_members_message = get_workspace_members(source_workspace_name, source_workspace_namespace)

    # if original workspace ACLs could not be retrieved - stop workspace setup
    if not get_workspace_members_success:
        workspace_dict["workspace_ACLs_error"] = workspace_members_message
        return workspace_dict

    # add ACLs to workspace if workspace creation success
    add_member_success, add_member_message = add_members_to_workspace(destination_workspace_name, workspace_members_message, destination_workspace_namespace)

    if not add_member_success:
        workspace_dict["workspace_ACLs_error"] = add_member_message
        return workspace_dict

    # adding ACLs to workspace success
    workspace_dict["workspace_ACLs"] = add_member_message  # update dict with ACL emails

    # add tags from original workspace to new workspace
    get_tags_success, get_tags_message = get_workspace_tags(source_workspace_name, source_workspace_namespace)

    if not get_tags_success:  # if get tags fails
        workspace_dict["workspace_tags_error"] = get_tags_message
        return workspace_dict

    add_tags_success, add_tags_message = add_tags_to_workspace(destination_workspace_name, get_tags_message, destination_workspace_namespace)

    if not add_tags_success:  # if add tags fails
        workspace_dict["workspace_tags_error"] = add_tags_message
        return workspace_dict

    print(f"Successfully updated {destination_workspace_namespace}/{destination_workspace_namespace} with the following tags: {add_tags_message}")
    workspace_dict["workspace_tags"] = add_tags_message

    # copy over workflows from source workspace to destination workspace
    copy_workflow_success, copy_workflow_message = copy_workspace_workflows(destination_workspace_namespace, destination_workspace_name, source_workspace_namespace, source_workspace_name)

    if not copy_workflow_success:  # if copy workflow fails
        workspace_dict["copy_workflows_error"] = copy_workflow_message
        return workspace_dict
    workspace_dict["copy_workflows"] = f"Workflows copied: {copy_workflow_message}"

    # copy over data tables from source workspace to destination workspace
    copy_data_table_success, copy_data_table_message = copy_workspace_entities_sushma(destination_workspace_namespace, destination_workspace_name, source_workspace_namespace, source_workspace_name, destination_bucket_id)

    if not copy_data_table_success:  # if copy workflow fails
        workspace_dict["copy_data_tables_error"] = copy_data_table_message
        return workspace_dict
    workspace_dict["copy_data_tables"] = f"Data tables copied: {copy_data_table_message}"

    # query big query tables to create, export, and get source_details.txt file uri
    make_table_success, make_table_message = get_source_objects_file.create_bucket_inventory_table(source_bucket_id)

    if not make_table_success:  # if the query results could not be put into table - don't proceed to export
        workspace_dict["source_object_details_file_error"] = make_table_message
        return workspace_dict

    # export table to GCS
    get_source_obj_success, get_source_obj_message = get_source_objects_file.export_bucket_inventory_table(source_bucket_id)

    if not get_source_obj_success:  # if source_details.txt could not be created
        workspace_dict["source_object_details_file_error"] = get_source_obj_message
        return workspace_dict
    workspace_dict["source_object_details_file"] = get_source_obj_message

    workspace_dict["final_workspace_status"] = "Success"  # final workspace setup step

    return workspace_dict


def migrate_workspaces(tsv):
    """Create and set up migrated workspaces."""
    # read full tsv into dataframe
    setup_info_df = pd.read_csv(tsv, sep="\t")

    # create df for output tsv file
    col_names = ["source_workspace_name", "source_workspace_namespace", "source_workspace_bucket",
                 "destination_workspace_name", "destination_workspace_namespace", "destination_workspace_bucket",
                 "workspace_link", "workspace_creation_error",
                 "workspace_ACLs", "workspace_ACLs_error",
                 "workspace_tags", "workspace_tags_error",
                 "copy_data_tables", "copy_data_tables_error",
                 "copy_workflows", "copy_workflows_error",
                 "source_object_details_file", "source_object_details_file_error",
                 "final_workspace_status"]

    all_row_df = pd.DataFrame(columns=col_names)

    # per row in tsv/df
    for index, row in setup_info_df.iterrows():

        # Create Workspace
        migration_data = setup_single_workspace(row)

        # Create output tsv
        migration_data_df = all_row_df.append(migration_data, ignore_index=True)

    # Create the report
    write_output_report(migration_data_df)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Set-up Van Allen Lab workspaces.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with original and new workspace details.')

    args = parser.parse_args()

    # call to create and set up workspaces
    migrate_workspaces(args.tsv)
