"""Get Google Storage bucket ID for a given workspace and workspace namespace.
Usage:
    > python3 get_workspace_bucket.py -t TSV_FILE """

import argparse
import pandas as pd

from utils import get_access_token, get_workpace_bucket, \
                  write_dataframe_to_file


def get_workspace_buckets(tsv):
    """Parse workspace information from input tsv and return bucket id."""

    workspace_info = pd.read_csv(tsv, sep="\t")
    updated_ws_info = pd.DataFrame(columns=["workspace_name", "workspace_project", "bucket_id"])

    for index, workspace in workspace_info.iterrows():
        # get workspace details
        workspace_name = workspace["workspace_name"]
        workspace_project = workspace["workspace_project"]

        get_bucket_message = get_workpace_bucket(workspace_name, workspace_project)

        # in case of success or failure, report the message
        workspace["bucket_id"] = get_bucket_message

        updated_ws_info = updated_ws_info.append(workspace, ignore_index=True)
    
    output_filename = tsv.split(".tsv")[0] + "_with_bucket_id.tsv"
    write_dataframe_to_file(updated_ws_info, output_filename)
    return updated_ws_info


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Get workspace bucket.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with workspace name and workspace project columns.')

    args = parser.parse_args()

    # call to create and set up external data delivery workspaces
    updated_ws_info = get_workspace_buckets(args.tsv)