"""Parse user supplied input to create data tables in Terra workspaces for the appropriate type/format of dataset.

Usage:
    > python3 make_dataset_data_tables.py -d DATASET_TYPE -x EXCEL_FILE -w TERRA_WORKSPACE_NAME -p TERRA_WORKSPACE_PROJECT """
import argparse
from firecloud import api as fapi
import json
import pandas as pd
from pandas_schema import*
from pandas_schema.validation import*
from validate import DATA_TABLE_VALIDATE_AND_FORMAT_SCHEMA as schema_validator


def upload_dataset_table_to_workspace(dataset_tables_dict, workspace_name, workspace_project):
    """Upload each of the tables in the chosen dataset to given workspace."""
    
    # for each table in dictionary
    for table in dataset_tables_dict:
        table_df = dataset_tables_dict[table]
        output_filename = f"{table}_table_load_file.tsv"
        # write out tsv file per table
        table_df.to_csv(output_filename, sep="\t")

        response = fapi.upload_entities_tsv(workspace_project, workspace_name, output_filename, model='flexible')
        status_code = response.status_code

        # TODO: do we want it to fail entirely as long as a single table fails to upload
        # if not success
        if status_code != 200:
            print(f"Warning: Table with name {table} has failed to upload. Please see errors: {response.text}")
            return

        # if success
        print(f"Table with name {table} has uploaded to {workspace_project}/{workspace_name}.")


def create_dataset_tables_dictionary(validated_dataset, dataset_name, dataset_table_names, schema_dict):
    """For a given dataset, subset/separate input columns into individual table dataframes."""

    # instantiate empty dictionary to capture {table_name: table_metadata_dataframe}
    dataset_tables_dict = {}

    # create a subset df for each table defined in the chosen dataset
    for table in dataset_table_names:
        # get subset dataframe for specific table for given dataset name
        table_df = dataset_metadata[schema_dict[dataset_name][table]]

        # TODO: edit the value = dataframe for a table to have the entity:table_name_id format

        # add KVP {table name : table_df} entry to all tables dictionary
        dataset_tables_dict[table] = table_df

    print(f"Dataset has been partitioned into the following individual tables: {dataset_table_names}")
    return dataset_tables_dict


def validate_dataset(dataset):
    """Validate the full dataset metadata with Schema validator."""

    # capture errors from the validation against the pre-defined validations per column
    errors = schema_validator.validate(dataset, columns=schema_validator.get_column_names())
    # get indices of the failed rows
    errors_index_rows = [e.row for e in errors]

    # if any errors, print error message and exit
    if errors_index_rows:
        print(f"Warning: Dataset validation errors found. Please retry after correcting errors listed in validation_errors.csv.")
        pd.DataFrame({'validation_error':errors}, index=False).to_csv('validation_errors.csv')
        return

    # TODO: determine how to handle drops of failed rows/columns to get cleaned data
    # errors_index_rows = [e.row for e in errors]
    # data_clean = dataset_metadata_df.drop(index=errors_index_rows, axis=1)
    # data_clean.to_csv('clean_data.csv')
    print(f"Dataset has been validated.")
    return dataset


def organize_dataset_metadata(dataset_name, excel, schema_json):
    """Create dataframe determined by user supplied dataset name and schema."""

    print(f"Loading schema for selected dataset: {dataset_name}")
    # read schema into dictionary
    with open(schema_json) as schema:
        schema_dict = json.load(schema)

    # get list of tables for a given dataset name
    dataset_table_names = list(schema_dict[dataset_name].keys())

    # get all cols (attributes) from all tables for dataset
    # returns nested list of lists with column names
    nested_dataset_cols = [schema_dict[dataset_name][table] for table in dataset_table_names]
    # unnest for flat list of columns to parse from excel file
    expected_dataset_cols = [col for table_cols in nested_dataset_cols for col in table_cols]

    try:
        dataset_metadata_df = pd.read_excel(excel, sheet_name="Sheet1", skiprows=2, usecols=expected_dataset_cols, index_col=None)
        
    except ValueError as e:
        # if the value error is about missing columns in input file
        if len(e.args) > 0 and e.args[0].startswith("Usecols do not match columns, columns expected but not found"):
            missing_cols = e.args[0].split(":")[1]
            print(f"Input excel file has the following missing columns that are required: {missing_cols}")
        else:
            print(e)
        return

    print(f"{len(dataset_table_names)} tables will be created for {dataset_name}: {dataset_table_names}")
    return(dataset_metadata_df, dataset_table_names, schema_dict)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create and optionally upload Terra dataset specific data tables to a Terra workspace.')

    parser.add_argument('-d', '--dataset', required=True, type=str, help='Dataset type to apply validations and data table structure. ex. proteomics, atac-seq, transcriptomics, singe-cell')
    parser.add_argument('-x', '--excel', required=True, type=str, help='CFoS data excel file (.xlsx) - based on CFoS data intake template file.')
    parser.add_argument('-j', '--schema', required=True, type=str, help='Dataset schema file - defines per dataset tables and associated attributes.')
    parser.add_argument('-p', '--project', required=True, type=str, help='Terra workspace project (namespace).')
    parser.add_argument('-w', '--workspace', required=True, type=str, help='Terra workspace name.')
    parser.add_argument('-u', '--upload', required=False, action='store_true', help='Set parameter to upload data table files to workspace. default = no upload')

    args = parser.parse_args()

    dataset_metadata, dataset_table_names, schema_dict = organize_dataset_metadata(args.dataset, args.excel, args.schema)
    validated_dataset = validate_dataset(dataset_metadata)
    dataset_tables_dict = create_dataset_tables_dictionary(validated_dataset, args.dataset, dataset_table_names, schema_dict)
    upload_dataset_table_to_workspace(dataset_tables_dict, args.workspace, args.project)
# python3 make_dataset_data_tables.py -d dataset1 -x test_data/CFoS_Template.xlsx -j dataset_tables_schema.json -p project -w workspace