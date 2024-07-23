# imports and environment variables
from ingest_to_tdr import call_ingest_dataset
from data_models import isolates_instance, plate_swipes_instance


import argparse
import copy 
import pandas as pd
from firecloud import api as fapi


# DEVELOPER: update this field anytime you make a new docker image and update changelog
docker_version = "1.0.29-beta"

# Acceptable instance types
instance_types = ["isolate", "plate_swipe"]



def df_to_col_dicts_chunked(df, instance_type_metadata):
    """
    Efficiently creates multiple dictionaries from a large DataFrame,
    using chunking for memory management. Each dictionary contains specific
    column combinations you define. Makes it so you only have to iterate through
    a DF once for every subset, where a subset is considered a table with specific
    columns. 

    Args:
        df (pandas.DataFrame): The DataFrame to process.
        instance_type_metadata (dict): A dictionary specifying column combinations.
            Keys are desired names for the dictionaries, and values are lists
            containing column names for each dictionary.

    Returns:
        dict: A dictionary where keys are the provided names (from instance_type_metadata[models])
              and values are dictionaries containing the selected columns.
              {< Table name > : [{column -> value}, … , {column -> value}]}
    """
    tables = instance_type_metadata["tables"]

    # Initialize a dictionary where { < table name > : []}
    results= {table["name"]: [] for table in tables}
    print(f"results: {results}") 
    for chunk in df:
        for table in tables:
            table_name = table["name"]
            column_names = table["columns"]
            print(f"Column names are: {column_names}")
            table_data = chunk[column_names]
            if "rename" in table:
                table_data = update_columns_names(table_data.copy(), table["rename"]) 
            table_data.fillna("", inplace=True)
            print(f"dataframe after NA replaced with empty values are: {table_data}")
            # print(f"the dictionary for {table_name} is: {col_dicts[table_name]}")
            results[table_name].extend(table_data.to_dict(orient='records'))
    return results


def update_columns_names(df, new_names):
    """
    Updates first column name in df

    Args:
        df (pandas.DataFrame): The DataFrame being processed.
        new_names (dict{string:string}): Dictionary to map old names to new names
            {'col1': 'New Column 1'}

    Returns:
        df ( pandas.DataFrame ): Original DF with header value updated
    """

    df.rename(columns=new_names, inplace=True) 
    return df

# def select_grouped_columns(df, col_groupings):
#     """
#     Selects specific columns from a DataFrame chunk based on grouping definitions.

#     Args:
#         df (pandas.DataFrame): The DataFrame being processed.
#         col_groupings (dict): A dictionary specifying column combinations.
#             Keys are desired names for the dictionaries, and values are lists
#             containing column names for each dictionary.

#     Returns:
#         dict ({ string:pandas.DataFrame } ): A dictionary where keys are the table name and values are
#               DataFrames containing only the selected columns for each table as defined
#               in data_models.py. { < table name >: < Table Data> }
#     """

#     group_cols = {}
#     for name, cols in col_groupings.items():
#         # Regular loop to select columns for each group
#         # Select columns using list of column names
#         group_cols[name] = df[cols]
#     return group_cols


def ingest_to_tdr(tsv_path, dataset_id, instance_type):
    """
    Ingest dataset, given by a tsv path, to TDR

    Args:
        tsv_path (string): The path to the tsv 
        dataset_id (string): The id of the target TDR workspace
        entity_name (string): Name of datamodel that defines table

    """
    if instance_type not in instance_types:
        raise NameError(f"instance_type must be one of these options {instance_types}")

    if instance_type == "plate_swipe":
        instance_type_metadata = plate_swipes_instance
    if instance_type == "isolate":
        instance_type_metadata = isolates_instance

    df = pd.read_csv(tsv_path, header=0,
                     iterator=True, sep='\t', chunksize=100)
    result = df_to_col_dicts_chunked(df, instance_type_metadata)
    print(f"All tables and data to be ingested looks like: {result}")
    # for table_name, table_data in result_dicts.items():
    #     # print(f"{table_name} data that will be ingested is: {table_data}")
    #     # Only ingest tables that have data in them
    #     if table_data:
    #         call_ingest_dataset(table_data, table_name, dataset_id)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest into TDR")

    parser.add_argument('-f', '--tsv', required=True, type=str,
                        help='tsv file of files to ingest to TDR')
    parser.add_argument('-d', '--dataset_id', required=True, type=str,
                        help='id of TDR dataset for destination of outputs')
    parser.add_argument('-i', '--instance_type', required=True, choices= instance_types)

    args = parser.parse_args()
    ingest_to_tdr(args.tsv, args.dataset_id, args.instance_type)
