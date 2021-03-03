"""Put/update Dataset attributes to workspaces parsed from input tsv file.

Usage:
    > python3 post_workspace_attributes.py -t TSV_FILE [-p BILLING-PROJECT]"""

import argparse
import pandas as pd

from utils import add_library_metadata_to_workspace, \
    publish_workspace_to_data_library, \
    write_output_report


def setup_single_data_delivery_workspace(request, project="anvil-datastorage"):
    """Update workspace with Dataset Attributes and publish to Data Library."""

    workspace_dict = {"input_workspace_name": "NA",
                      "input_project_name": "NA",
                      "workspace_link": "NA",
                      "dataset_attributes_status": "NA",
                      "publish_workspace_status": "Incomplete",
                      "final_workspace_status": "Failed"}

    # update the workspace's dataset attributes per request in list of requests
    workspace_name = request["name"]
    workspace_dict["input_workspace_name"] = workspace_name
    workspace_dict["input_project_name"] = project
    workspace_dict["workspace_link"] = (f"https://app.terra.bio/#workspaces/{project}/{workspace_name}").replace(" ", "%20")

    # post each request (a single workspace's dataset attributes) to workspace
    post_attrs_success, post_attrs_response = add_library_metadata_to_workspace(request, workspace_name, project)

    if not post_attrs_success:  # if posting dataset attributes does not succeed
        workspace_dict["dataset_attributes_status"] = post_attrs_response
        return workspace_dict

    workspace_dict["post_dataset_attributes_status"] = "Success"

    # if posting dataset attributes succeeds, publish workspace
    publish_ws_success, publish_ws_response = publish_workspace_to_data_library(workspace_name, project)

    if not publish_ws_success:
        workspace_dict["publish_workspace_status"] = publish_ws_response
        return workspace_dict

    workspace_dict["publish_workspace_status"] = "Success"
    return workspace_dict


def setup_data_delivery_workspaces(tsv):
    """Create array of json requests per row in tsv file."""

    # read input tsv into dataframe, workspace name = index and edit dataframe
    all_workspaces = pd.read_csv(tsv, sep="\t", index_col="name")
    # remove columns that have ".itemsType" in col name - col values are AttributeValue
    all_workspaces_modified = all_workspaces[all_workspaces.columns.drop(list(all_workspaces.filter(regex='.itemsType')))]
    # remove the ".items" string from column names
    all_workspaces_modified.columns = all_workspaces_modified.columns.str.replace(".items", "")

    # create df for OUTPUT tsv file
    col_names = ["input_workspace_name", "input_project_name",
                 "workspace_link",
                 "post_dataset_attributes_status", "publish_workspace_error",
                 "final_workspace_status"]
    published_workspace_statuses = pd.DataFrame(columns=col_names)

    # make json request for each workspace and append json request to list of requests
    row_requests = []
    for workspace in all_workspaces_modified.index:
        row_json_request = all_workspaces_modified.loc[workspace].to_json()
        row_requests.append(row_json_request)

    for request in row_requests:
        row_dict = setup_single_data_delivery_workspace(request)

        # append single workspace's dictionary of statuses to output dataframe
        published_workspace_statuses = published_workspace_statuses.append(row_dict, ignore_index=True)

    # convert all workspaces' dataframe into csv report
    write_output_report(published_workspace_statuses)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Post dataset attributes to workspace and publish workspace to Data Library.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with attributes to post to workspaces.')
    parser.add_argument('-p', '--workspace_project', type=str, default="anvil-datastorage", help='Workspace Project/Namespace')

    args = parser.parse_args()

    # call to set up data delivery workspaces with dataset/library metadata and publish workspace to Data Library
    setup_data_delivery_workspaces(args.tsv, args.workspace_project)
