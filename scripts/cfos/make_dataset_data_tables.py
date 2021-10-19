"""Parse user supplied input to create data tables in Terra workspaces for the appropriate type/format of dataset.

Usage:
    > python3 make_dataset_data_tables.py -d DATASET_TYPE -x EXCEL_FILE -w TERRA_WORKSPACE_NAME -p TERRA_WORKSPACE_PROJECT """
import argparse
import json
import pandas as pd
from pandas_schema import*
from pandas_schema.validation import*
from validate import DATA_TABLE_VALIDATE_AND_FORMAT_SCHEMA as schema_validator

def validate_and_format_dataset_tables(table_metadata, table_name):
    """Validate and/or format a dataset's data tables with defined validations/formats."""

    # validate the data frame for given table
    # validated_table = validate_and_format_table(table_df, table)
    # write validated data table to tsv file in data table upload format
    print(table_name)
    errors = validator.validate(table_metadata)
    print(errors)
    errors_index_rows = [e.row for e in errors]
    print(errors_index_rows)
    data_clean = table_metadata.drop(index=errors_index_rows)
    print(data_clean)

    pd.DataFrame({'col':errors}).to_csv('errors.csv')
    data_clean.to_csv('clean_data.csv')
    exit(1)

def create_dataset_tables_dictionary(dataset_metadata, dataset_name, dataset_table_names, schema_dict):
    """For a given dataset, subset/separate input columns into individual table dataframes."""

    # instantiate empty dictionary to capture {table_name: table_metadata_dataframe}
    dataset_tables_dict = {}

    # create a subset df for each table defined for given dataset and send for pertinent validation
    for table in dataset_table_names:
        # get subset dataframe for specific table for given dataset name
        table_df = dataset_metadata[schema_dict[dataset_name][table]]
        # add KVP {table name : table_df} entry to all tables dictionary
        dataset_tables_dict[table] = table_df

    # dictionary of table and table dataframes
    # print(dataset_tables_dict)
    for key in dataset_tables_dict:
        table_name = key
        table_df = dataset_tables_dict[key]
        validate_and_format_dataset_tables(table_df, table_name)
    exit(1)
        # validate_and_format_dataset_tables(dataset, table_name)


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
    # print(f"Dataset metadata dataframe: {dataset_metadata_df}")

    errors = schema_validator.validate(dataset_metadata_df, columns=schema_validator.get_column_names())
    print(errors)
    errors_index_rows = [e.row for e in errors]
    print(errors_index_rows)
    exit(1)
    data_clean = table_metadata.drop(index=errors_index_rows)
    print(data_clean)

    pd.DataFrame({'col':errors}).to_csv('errors.csv')
    data_clean.to_csv('clean_data.csv')
    exit(1)
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
    dataset_tables_dict = create_dataset_tables_dictionary(dataset_metadata, args.dataset, dataset_table_names, schema_dict)
    validate_and_format_dataset_tables(table_metadata, table_name)

# python3 make_dataset_data_tables.py -d dataset1 -x test_data/CFoS_Template.xlsx -j dataset_tables_schema.json -p project -w workspace