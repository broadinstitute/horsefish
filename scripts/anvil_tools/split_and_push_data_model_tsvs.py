"""Split single data model load tsv file by workspace_name (and workspace_project) into individual load tsv files and push to respective Terra workspace.

Usage:
    > python3 split_and_push_data_model_tsvs.py -t TSV_FILE [-a ARRAY_COLUMNS_FILE]"""

import argparse
import pandas as pd
import batch_upsert_entities_standard


def split_and_push_workspace_entities(tsv, array_column_names=None, json_output=None):
    """Create individual request body per unique project/workspace noted in tsv file and push request/data table to workspace."""

    # read full tsv into dataframe - contains all rows for all data across all workspaces
    tsv_all = pd.read_csv(tsv, sep="\t")

    # start the array attribute column list as empty - fill in if user provides a file with list of column names of array type attributes
    array_attr_cols = []
    if array_column_names:
        # read array column names into list - strip out \n
        with open(array_column_names, "r") as f:
            array_attr_cols = [line.strip() for line in f]

    # get unique list of tuples where each tuple = (workspace_name, workspace_project)
    # length of list = number of workspaces for which tsvs will be generated and pushed via FISS
    all_workspaces = list(tsv_all[['workspace_name', 'workspace_project']].drop_duplicates().to_records(index=False))

    # for each tuple in list (workspace, project)
    for workspace in all_workspaces:
        workspace_name = workspace[0]
        workspace_project = workspace[1]

        # get rows that match the combination of workspace_name and workspace_project, drop the workspace identifying columns (name + project)
        workspace_tsv = tsv_all.loc[(tsv_all['workspace_name'] == workspace_name) & (tsv_all['workspace_project'] == workspace_project)] \
            .drop(['workspace_name', 'workspace_project'], axis=1)

        # pass in workspace subsetted tsv to convert the df into properly formatted batchUpsert request
        print(f"Starting entity updates to {workspace_project}/{workspace_name}:")
        print(f"Creating json request.")
        json_request = batch_upsert_entities_standard.create_upsert_request(workspace_tsv, array_attr_cols)

        # if local file containing final json request is requested, write json file
        if json_output:
            # write out a json of the request body
            print(f"Creating json request file locally.")
            filename_prefix = f"{workspace_project}_{workspace_name}"
            batch_upsert_entities_standard.write_request_json(json_request, filename_prefix)

        # pass batchUpsert formatted request to FISS to make API call
        print(f"Uploading json request.")
        batch_upsert_entities_standard.call_rawls_batch_upsert(workspace_name, workspace_project, json_request)

        


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Split single data model tsv and push individual tables using workspace/project column information.')

    parser.add_argument('-t', '--tsv', required=True, type=str, help='tsv file in Terra data model load file format with workspace name and workspace project columns.')
    parser.add_argument('-a', '--array_columns', required=False, type=str, help='new line delimited file with array type column names/attributes in provided tsv.')
    parser.add_argument('-j', '--json_output', required=False, action='store_true', help='set parameter if a local copy/file of the final json request is needed.')

    args = parser.parse_args()
    # if a local copy of the final json request for API is needed

    # if both array type columns/attributes
    if args.json_output and args.array_columns:
        split_and_push_workspace_entities(tsv=args.tsv, array_column_names=args.array_columns, json_output=args.json_output)
    # if only array type columns/attributes
    elif args.array_columns:
        split_and_push_workspace_entities(tsv=args.tsv, array_column_names=args.array_columns)
    # if only json request output file
    elif args.json_output:
        split_and_push_workspace_entities(tsv=args.tsv, json_output=args.json_output)
    # if neither array type columns/attributes nor json request output file
    else:
        split_and_push_workspace_entities(tsv=args.tsv)
