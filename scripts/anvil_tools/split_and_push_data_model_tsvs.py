"""Split single data model load tsv file by workspace_name (and workspace_project) into individual load tsv files and push to respective Terra workspace.

Usage:
    > python3 split_and_push_data_model_tsvs.py -t TSV_FILE [-a ARRAY_COLUMNS_FILE]"""

import argparse
import pandas as pd
import batch_usert_entities_standard


def split_and_push_workspace_entities(tsv, array_column_names=None):
    """Create individual request body per workspace and user/group listed in tsv file and push rquest/data table to workspace."""

    # read full tsv into dataframe
    tsv_all = pd.read_csv(tsv, sep="\t")

    # start the array attribute column list as empty - fill in if user provides a file with list of column names of array type attributes
    array_attr_cols = []
    if array_column_names:
        # read array column names into list
        with open(array_column_names, "r") as f:
            array_attr_cols = [line.strip() for line in f]

    # get unique list of tuples where each tuple = (workspace_name, workspace_project)
    # length of list = number of workspaces for which tsvs will be generated and pushed via FISS
    all_workspaces = list(tsv_all[['workspace_name', 'workspace_project']].drop_duplicates().to_records(index=False))

    # for each tuple in list (workspace, project)
    for workspace in all_workspaces:
        workspace_name = workspace[0]
        workspace_project = workspace[1]

        # get rows that match the combination of workspace_name and workspace_project, drop the workspace identifying columns
        workspace_tsv = tsv_all.loc[(tsv_all['workspace_name'] == workspace_name) & (tsv_all['workspace_project'] == workspace_project)] \
            .drop(['workspace_name', 'workspace_project'], axis=1)

        print(f"Starting entity updates to {workspace_project}/{workspace_name}:")
        print(f"Creating json request.")
        json_request = batch_usert_entities_standard.create_upsert_request(workspace_tsv, array_attr_cols)

        print(f"Uploading json request.")
        batch_usert_entities_standard.call_rawls_batch_upsert(workspace_name, workspace_project, json_request)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Set-up AnVIL external data delivery workspaces.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file with workspace name and auth domains to create.')
    parser.add_argument('-a', '--array_columns', required=False, type=str, help='new line delimited file with array type column names in provided tsv.')

    args = parser.parse_args()

    if args.array_columns:
        split_and_push_workspace_entities(args.tsv, args.array_columns)
    else:
        split_and_push_workspace_entities(args.tsv)

