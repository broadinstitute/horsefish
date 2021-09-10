"""Parse user supplied input to create data tables in Terra workspaces for the appropriate type/format of dataset.

Usage:
    > python3 make_dataset_data_tables.py -d DATASET_TYPE -t TSV_FILE -w TERRA_WORKSPACE_NAME -p TERRA_WORKSPACE_PROJECT """
import argparse
import json
import pandas as pd
from pandas_schema import*
from pandas_schema.validation import*

DATA_TABLE_VALIDATE_AND_FORMAT_SCHEMA = ps.Schema([Column("hasDonorAge", [isDtypeValidation("int")]),
                                                   Column("column_name", [validator()]),
                                                   

])

# {
#     "hasDonorAge": check_is_integer,
#     "has_phenotypic_sex": "format_gender_string"
# }


def validate_and_format_dataset_tables(table_metadata, table_name):
    """Validate and/or format a dataset's data tables with defined validations/formats."""

    function = check_is_integer

    function()

    # validate the data frame for given table
    # validated_table = validate_and_format_table(table_df, table)
    # write validated data table to tsv file in data table upload format


def create_dataset_tables_dictionary(dataset_metadata, dataset_name, dataset_tables, schema_dict):
    """For a given dataset's metadata, subset metadata into individual table dataframes."""

    # instantiate empty dictionary to capture {table_name: table_metadata_dataframe}
    dataset_tables_dict = {}

    # create a subset df for each table defined for given dataset and send for pertinent validation
    for table in dataset_tables:
        # get subset dataframe for specific table for given dataset name
        table_df = dataset_metadata[schema_dict[dataset_name][table]]
        # add KVP {table name : table_df} entry to all tables dictionary
        dataset_tables_dict[table] = table_df

    # dictionary of table and table dataframes
    return dataset_tables_dict


def organize_dataset_metadata(dataset_name, tsv, schema_json):
    """Create dataframe determined by user supplied dataset name and schema."""

    print("Loading schema json.")
    # read schema into dictionary
    with open(schema_json) as schema:
        schema_dict = json.load(schema)

    # get list of tables for a given dataset name
    dataset_tables = list(schema_dict[dataset_name].keys())
    print(f"{len(dataset_tables)} tables will be created for {dataset_name}: {dataset_tables}")

    # get all cols (attributes) from all tables for dataset
    # returns nested list of lists with column names
    nested_dataset_cols = [schema_dict[dataset_name][table] for table in dataset_tables]
    # unnest for flat list of columns to parse from tsv file
    dataset_cols = [col for table_cols in nested_dataset_cols for col in table_cols]

    # TODO: validation. if schema json columns do not match columns in given tsv file, report error
    # from template: sheet1, skip non-column header rows, ignore first empty column
    dataset_metadata_df = pd.read_excel(tsv, sheet_name="Sheet1", skiprows=2, usecols=dataset_cols, index_col=None)

    print(f"Dataset metadata dataframe: {dataset_metadata_df}")
    return(dataset_metadata_df, dataset_tables, schema_dict)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create and optionally upload Terra dataset specific data tables to a Terra workspace.')

    parser.add_argument('-d', '--dataset', required=True, type=str, help='Dataset type to apply validations and data table structure. ex. proteomics, atac-seq, transcriptomics, singe-cell')
    parser.add_argument('-t', '--tsv', required=True, type=str, help='CFoS data file - based on CFoS data intake template file.')
    parser.add_argument('-j', '--schema', required=True, type=str, help='Dataset schema file - defines per dataset tables and associated attributes.')
    parser.add_argument('-p', '--project', required=True, type=str, help='Terra workspace project (namespace).')
    parser.add_argument('-w', '--workspace', required=True, type=str, help='Terra workspace name.')
    parser.add_argument('-u', '--upload', required=False, action='store_true', help='Set parameter to upload data table files to workspace. default = no upload')

    args = parser.parse_args()

    dataset_metadata, dataset_tables, schema_dict = organize_dataset_metadata(args.dataset, args.tsv, args.schema)
    dataset_tables_dict = create_dataset_tables_dictionary(dataset_metadata, args.dataset, dataset_tables, schema_dict)
