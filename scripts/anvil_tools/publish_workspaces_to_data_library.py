"""Put/update Dataset attributes to workspaces parsed from input tsv file.

Usage:
    > python3 publish_workspaces_to_data_library.py -t TSV_FILE [-p BILLING-PROJECT]"""

import argparse
import pandas as pd

from utils import publish_workspace_to_data_library, \
    write_output_report


def publish_single_workspace(workspace_info, project="anvil-datastorage"):
    """Parse workspace information and call API to publish to Data Library."""

    # create dictionary to populate for output tsv file
    workspace_dict = {"input_workspace_name": "NA",
                      "input_workspace_project": "NA",
                      "workspace_link": "NA",
                      "publish_workspace_error": "NA",
                      "final_workspace_status": "Failed"}

    workspace_name = workspace_info["workspace_name"]
    workspace_dict["input_workspace_name"] = workspace_name
    workspace_dict["input_workspace_project"] = project

    # call api to publish workspace and get response
    success, response = publish_workspace_to_data_library(workspace_name, project)

    if not success:                                         # publish fail
        workspace_dict["publish_workspace_error"] = response
        return workspace_dict

    workspace_dict["workspace_link"] = (f"https://portal.firecloud.org/#workspaces/{project}/{workspace_name}").replace(" ", "%20")
    workspace_dict["publish_workspace_status"] = "Success"  # publish success
    return workspace_dict


def setup_workspaces_for_publication(tsv, project="anvil-datastorage"):
    """Publish workspace, capture success/fail, and report details in output tsv file."""

    # read input tsv into dataframe
    to_publish_workspaces = pd.read_csv(tsv, sep="\t")

    # create df for output tsv file
    col_names = ["input_workspace_name", "input_workspace_project",
                 "workspace_link",
                 "publish_workspace_error", "final_workspace_status"]

    published_workspace_statuses = pd.DataFrame(columns=col_names)

    # per row in input tsv/df
    for index, row in to_publish_workspaces.iterrows():
        # publish the single workspace
        row_dict = publish_single_workspace(row, project)

        # append single workspace's dictionary of statuses to output dataframe
        published_workspace_statuses = published_workspace_statuses.append(row_dict, ignore_index=True)

    # convert all workspaces' dataframe into csv report
    write_output_report(published_workspace_statuses)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Publish workspaces to Dataset Library.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with attributes to post to workspaces.')
    parser.add_argument('-p', '--workspace_project', type=str, default="anvil-datastorage", help='Workspace Project/Namespace')

    args = parser.parse_args()

    # call to create request body PER row and make API call to update attributes
    setup_workspaces_for_publication(args.tsv, args.workspace_project)
