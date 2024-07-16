"""This script extracts metadata from a TDR snapshot and exports its tables to the
data model of a defined destination Terra workspace.
It automatically shares snapshot with Terra workspace readers, writers, and owners.
It resolves DRS uris to gs paths.
"""


import argparse
import os
import numpy as np
from pprint import pprint
from tqdm import tqdm

from tdr_utils import get_snapshot_access_info, get_all_attributes, resolve_drs, share_snapshot
from terra_utils import upload_tsv_to_terra, get_workspace_groups, create_set, get_workspace_AD_list


def share_snapshot_with_workspace_groups(snapshot_id, workspace_project, workspace_name):

    # try to get workspace AD
    workspace_AD_list = get_workspace_AD_list(workspace_project, workspace_name)

    # if workspace has an AD, use that
    if workspace_AD_list:
        print(f'found following workspace auth domains:')
        print(workspace_AD_list)
        for workspace_AD in workspace_AD_list:
            share_snapshot(snapshot_id, workspace_AD, 'reader')
    else:  # no AD, so get and share with workspace access groups
        workspace_groups = get_workspace_groups(workspace_project, workspace_name)

        workspace_permissions_to_share = ['owner', 'project-owner', 'reader', 'writer']

        for permissions_level, group_email in workspace_groups.items():
            if permissions_level in workspace_permissions_to_share:
                # grant the group reader level access to the snapshot
                share_snapshot(snapshot_id, group_email, 'reader')


def export_snapshot_to_data_model(snapshot_id, workspace_name, workspace_project, make_set=True, verbose=True):
    snapshot_access_info = get_snapshot_access_info(snapshot_id)

    snapshot_name = snapshot_access_info['name']
    # create a dict of tables in the snapshot with their fq paths
    snapshot_tables_dict = dict()
    for table in snapshot_access_info['accessInformation']['bigQuery']['tables']:
        snapshot_tables_dict[table['name']] = table['qualifiedName']

    if verbose:
        print(f"Found {len(snapshot_tables_dict)} table(s) in snapshot {snapshot_name}")

    for table_name, fq_snapshot_table in snapshot_tables_dict.items():
        if verbose:
            print(f"Importing table {table_name} to Terra. \nRetrieving snapshot data.")

        # use the snapshot project to query BQ
        bq_project = fq_snapshot_table.split('.')[0]
        df_snapshot = get_all_attributes(fq_snapshot_table, bq_project, datarepo_row_id_list=None)

        # reformat dataframe
        ent_id_col_name = f"entity:{table_name}_id"
        ent_id_col = df_snapshot.pop(f"{table_name}_id")
        df_snapshot.insert(0, ent_id_col_name, ent_id_col)

        # reformat any array columns from stupid numpy.ndarray to list
        print("reformatting columns. this could take a while.")
        for col in df_snapshot.columns:
            print(f"processing {col}")
            existing_data = df_snapshot[col]
            updated_data = []
            for value in tqdm(existing_data):
                if isinstance(value, np.ndarray):
                    # this is an array field. convert to a string so the tsv is written properly
                    stringified_list = '["' + '","'.join([resolve_drs(item) for item in value]) + '"]'
                    updated_data.append(stringified_list)
                else:
                    updated_data.append(resolve_drs(value))
            df_snapshot[col] = updated_data

        # save df as tsv
        output_filename = f"tmp/snapshot_{snapshot_name}_terra_datamodel_upload.tsv"
        df_snapshot.to_csv(output_filename, sep="\t", index=False, quotechar="'")

        # upload tsv to data model
        upload_result = upload_tsv_to_terra(output_filename, workspace_project, workspace_name)
        pprint(upload_result)

        if make_set:
            # OPTIONALLY create a set from the imported entities
            set_name = snapshot_name
            entity_list = df_snapshot[ent_id_col_name]

            if len(entity_list) == 1:
                print(f"hmm, you probably don't mean to create a set of one entity, so we're not going to do that.")
            else:
                create_set(set_name, entity_list, workspace_project, workspace_name, root_etype=table_name)

    # share snapshot with workspace readers
    # TODO only share if the snapshot contains files, otherwise this achieves nothing
    share_snapshot_with_workspace_groups(snapshot_id, workspace_project, workspace_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--snapshot_id', '-s', type=str, help='TDR snapshot id (uuid) of snapshot to import')
    parser.add_argument('--workspace_name', '-w', type=str, help='destination Terra workspace name')
    parser.add_argument('--workspace_project', '-p', type=str, help='destination Terra project name')

    parser.add_argument('--share_only', action='store_true', help='share snapshot with all workspace groups (does NOT export snapshot to data model)')
    parser.add_argument('--make_set', action='store_true', help='create a set from the contents of the imported snapshot')

    parser.add_argument('--verbose', '-v', action='store_true', help='print progress text')

    args = parser.parse_args()

    if not os.path.isdir("tmp"):
        os.mkdir("tmp")

    if args.share_only:
        share_snapshot_with_workspace_groups(args.snapshot_id, args.workspace_project, args.workspace_name)
    else:
        export_snapshot_to_data_model(args.snapshot_id, args.workspace_name, args.workspace_project, args.make_set, args.verbose)

    os.rmdir("tmp")