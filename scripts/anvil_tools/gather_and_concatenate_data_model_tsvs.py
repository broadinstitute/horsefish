"""Gather a single entity's data model metadata by workspace_name (and workspace_project) into a single load tsv file format and report in excel document.

Usage:
    > python3 gather_and_concatenate_data_model_tsvs.py -x EXCEL_FILE -e ENTITY_TABLE_NAME"""

import argparse
from firecloud import api as fapi
from openpyxl import load_workbook
import pandas as pd


def gather_and_concatenate_data_model_tsvs(input_file, entity_name):
    """Get data table tsv files from list of workspaces and concatenate results into a single excel report."""

    # read full excel sheet into dataframe - all rows of workspace project and workspace names
    workspace_info = pd.read_excel(input_file, sheet_name="Sheet1", index_col=None)

    # instantiate empty list to hold all entity information from all workspaces
    all_workspace_entities = []
    failed_workspaces = []
    # for each workspace_name, workspace_project pair
    for index, workspace in workspace_info.iterrows():
        # get workspace details
        workspace_name = workspace["workspace_name"]
        workspace_project = workspace["workspace_project"]

        # get a response with all attributes for each row in entity table
        entities = fapi.get_entities(workspace_project, workspace_name, entity_name)

        # if get entities call fails, add workspace details to dictionary
        # skip to next workspace
        if entities.status_code != 200:
            print(f"{entity_name} table in {workspace_project}/{workspace_name} does not exist or user does not have workspace access.")
            failed_workspaces.append({"workspace_project": workspace_project, "workspace_name": workspace_name})
            continue

        # for each row in entity table, re-format nested response json
        for entity in entities.json():
            entity_attributes = entity["attributes"]  # [{attr name: attr value}] for each row
            entity_id = entity["name"]         # name of entity

            # insert entity_id, workspace_project, and workspace_name into list of dictionaries
            entity_attributes[f"entity:{entity_name}_id"] = entity_id
            entity_attributes["workspace_project"] = workspace_project
            entity_attributes["workspace_name"] = workspace_name

            # add entity informatioon (dictionary) to list
            all_workspace_entities.append(entity_attributes)

        print(f"{entity_name} table in {workspace_project}/{workspace_name} successfully gathered.")

    # successful entity dictionaries -> df - dict per row (entity) for each entity table in all workspaces
    succeeded_data = pd.DataFrame(all_workspace_entities)
    # failed workspaces -> df
    failed_data = pd.DataFrame(failed_workspaces)

    # reorder dataframe entity:table_name column is first
    ent_id_col = succeeded_data.pop(f"entity:{entity_name}_id")
    succeeded_data.insert(0, ent_id_col.name, ent_id_col)

    # # write final dataframes to excel file - separate sheets for success and failed data
    output_filename = input_file.split("/")[-1].split(".")[0] + "_final.xlsx"
    writer = pd.ExcelWriter(output_filename, engine="openpyxl")

    succeeded_data.to_excel(writer, sheet_name="concatenated_entity_table", index=None)
    failed_data.to_excel(writer, sheet_name="failed_workspaces", index=None)
    writer.save()

    # if any failures, print warning message.
    if len(failed_workspaces) > 0:
        print(f"Warning: Completed gather and concatenate with the exception of some workspace/s. Please examine details in {output_filename}.")
        return
    # else print success message
    print(f"Successfully completed gather and concatenate for all workspaces. Results can be found in {output_filename}.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Gather multiple data model tables using workspace/project column information and concatenate into single excel report.')

    parser.add_argument('-x', '--excel', required=True, type=str, help='excel (.xlsx) file with workspace name and workspace project columns.')
    parser.add_argument('-e', '--entity_name', required=True, type=str, help='name of data table to pull from each workspace.')

    args = parser.parse_args()

    gather_and_concatenate_data_model_tsvs(args.excel, args.entity_name)
