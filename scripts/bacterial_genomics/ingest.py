# imports and environment variables
from ingest_to_tdr import call_ingest_dataset
from data_models import ingest_tables

import argparse
import copy 
import pandas as pd
from firecloud import api as fapi


# DEVELOPER: update this field anytime you make a new docker image and update changelog
docker_version = "1.0.0-beta"


def df_to_col_dicts_chunked(df, col_groupings):
    """
    Efficiently creates multiple dictionaries from a large DataFrame,
    using chunking for memory management. Each dictionary contains specific
    column combinations you define. Makes it so you only have to iterate through
    a DF once for every subset, where a subset is considered a table with specific
    columns. 

    Args:
        df (pandas.DataFrame): The DataFrame to process.
        col_groupings (dict): A dictionary specifying column combinations.
            Keys are desired names for the dictionaries, and values are lists
            containing column names for each dictionary.

    Returns:
        dict: A dictionary where keys are the provided names (from col_groupings)
              and values are dictionaries containing the selected columns.
              {< Table name > : [{column -> value}, … , {column -> value}]}
    """
    # Initialize a dictionary where { < table name > : []}
    col_dicts= {column_name: [] for column_name in col_groupings.keys()}
    for chunk in df:
        # Efficiently select columns for each grouping using list comprehension
        group_cols = select_grouped_columns(chunk, col_groupings)
        for name, group in group_cols.items():
            remove_row_nan = group.dropna()
            if name == "Sample":
                remove_row_nan.rename(columns={"entity:sample_id": "sample_id" }, inplace=True)
            col_dicts[name].extend(remove_row_nan.to_dict(orient='records'))
    return col_dicts


def select_grouped_columns(chunk, col_groupings):
    """
    Selects specific columns from a DataFrame chunk based on grouping definitions.

    Args:
        chunk (pandas.DataFrame): A chunk of the DataFrame being processed.
        col_groupings (dict): A dictionary specifying column combinations.
            Keys are desired names for the dictionaries, and values are lists
            containing column names for each dictionary.

    Returns:
        dict: A dictionary where keys are names from col_groupings and values are
              DataFrames containing only the selected columns for each group.
    """

    group_cols = {}
    for name, cols in col_groupings.items():
        # Regular loop to select columns for each group
        # Select columns using list of column names
        group_cols[name] = chunk[cols]
    return group_cols


def ingest_to_tdr(tsv_path, dataset_id):
    """
    Ingest dataset, given by a tsv path, to TDR

    Args:
        tsv_path (string): The path to the tsv 
        dataset_id (string): The id of the target TDR workspace

    """

    df = pd.read_csv(tsv_path, header=0,
                     iterator=True, sep='\t', chunksize=2)
    result_dicts = df_to_col_dicts_chunked(df, ingest_tables)
    for table_name, table_data in result_dicts.items():
        # Only ingest tables that have data in them
        if table_data:
            call_ingest_dataset(table_data, table_name, dataset_id)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest into TDR")

    parser.add_argument('-f', '--tsv', required=True, type=str,
                        help='tsv file of files to ingest to TDR')
    parser.add_argument('-d', '--dataset_id', required=True, type=str,
                        help='id of TDR dataset for destination of outputs')

    args = parser.parse_args()
    ingest_to_tdr(args.tsv, args.dataset_id)
