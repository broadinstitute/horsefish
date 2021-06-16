import argparse
import csv
from firecloud import api as fapi
from io import StringIO
import json
import re

GUID_PATTERN = re.compile(r'^[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}$', re.IGNORECASE)


def update_entities_to_compact_identifier(workspace, project, single_etypes_list, dry_run):
    """Update Data Model entity attributes to DRS 1.1 Compact Identifier."""

    for etype in single_etypes_list:
        print(f'Starting TCGA DRS updates for entity: {etype}')

        # get entity table response for API call
        res_etype = fapi.get_entities_tsv(project, workspace, etype, model="flexible")

        # save current/original data model tsv files for provenance
        print(f'Saving original {etype} TSV.')
        original_tsv_name = f"original_{etype}_{project}-{workspace}_table.tsv"
        with open(original_tsv_name, "w") as f:
            f.write(res_etype.text)

        # read entity table response into dictionary to perform DRS URL updates
        dict_etype = list(csv.DictReader(StringIO(res_etype.text), delimiter='\t'))

        # create empty list to add updated rows and list to capture list of columns that were modified
        drs_dict_table = []
        modified_cols = set()
        # for "row" (each row is [list] of column:values)
        for row in dict_etype:
            drs_row = row.copy()
            # for each column in row
            for col in row:
                # check if the col values are dataguids.org URLs and parse out guid
                if row[col].startswith("drs://dataguids.org"):
                    guid = row[col].split("/")[3]
                    # only modify col if guid is valid and exists
                    if guid and GUID_PATTERN.match(guid):
                        drs_url = "drs://dg.4DFC:" + guid
                        drs_row[col] = drs_url
                        modified_cols.add(col)

            # append new "row" with updated drs values to new list
            drs_dict_table.append(drs_row)

        # save new/drs updated data model tsv files for provenance
        print(f'Saving updated {etype} TSV.')
        updated_tsv_name = f"updated_{etype}_{project}-{workspace}_table.tsv"
        tsv_headers = drs_dict_table[0].keys()

        with open(updated_tsv_name, 'w') as outfile:
            # get keys from OrderedDictionary and write rows, separate with tabs
            writer = csv.DictWriter(outfile, tsv_headers, delimiter="\t")
            writer.writeheader()
            writer.writerows(drs_dict_table)

        # list of the columns that are scoped to be updated if re-run with dry_run = False
        modified_cols = list(modified_cols)
        if dry_run:
            print(f"Columns in the {etype} table that *will be* be updated when notebook is re-run with `dry_run = False`:")
            if not modified_cols:
                print("\t" * 4 + f"No columns to update in the {etype} table." + "\n\n")
            else:
                print('\n'.join(['\t' * 4 + c for c in modified_cols]))
                print(f"To view in detail what will be updated, inspect the {updated_tsv_name} file." + "\n\n")
        else:
            # upload newly created tsv file containing drs urls
            print(f"Starting update of the {etype} table with compact DRS identifiers (drs://df.4DFC:GUID).")

            res_update = fapi.upload_entities_tsv(project, workspace, updated_tsv_name, model="flexible")
            if res_update.status_code != 200:
                print(f"Could not update existing {etype} table. Error message: {res_update.text}")

            print(f"Finished uploading TCGA DRS updated .tsv for entity: {etype}" + "\n")


def get_single_entity_types(workspace, project):
    """Get a list of all non-set entity types in given workspace."""

    # API call to get all entity types in workspace (type set and non-set)
    res_etypes = fapi.list_entity_types(project, workspace)
    dict_all_etypes = json.loads(res_etypes.text)

    # get non-set entities and add to list
    # "set" entities do not need to be updated because they only reference the unique ID of each single entity
    # the unique ID of any single entity is not modified so sets should remain the same
    single_etypes_list = []
    single_etypes_list = [key for key in dict_all_etypes.keys() if not key.endswith("_set")]

    print(f"List of entity types that will be updated, if applicable:")
    print('\n'.join(['\t' * 7 + c for c in single_etypes_list]))

    return single_etypes_list


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Update workspace data models with DRS v1.1 compact identifiers.')

    parser.add_argument('-w', '--workspace_name', required=True, type=str, help='Name of Terra workspace.')
    parser.add_argument('-p', '--terra_project', required=True, type=str, help='Name of Terra project.')
    parser.add_argument('-d', '--dry_run', action='store_true', help='Returns updates that will be made if script is re-run without flag.')

    args = parser.parse_args()

    # get a list of all single entity types
    single_etypes_list = get_single_entity_types(args.workspace_name, args.terra_project)
    # call to create and set up workspaces
    update_entities_to_compact_identifier(args.workspace_name, args.terra_project, single_etypes_list, args.dry_run)
