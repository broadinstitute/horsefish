# imports and environment variables
from ingest_to_tdr import call_ingest_dataset
from data_models import tables, plate_swipes_model, isolate_swipe_model
import traceback


import argparse
import copy 
import pandas as pd
from firecloud import api as fapi


# DEVELOPER: update this field anytime you make a new docker image and update changelog
docker_version = "1.0.09-alpha"

# Acceptable data models types
instance_types = ["plate_swipe", "isolate"]
boolean_values = ['true', 'false']



def df_to_col_dicts_chunked(df, instance_type):
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

    # Initialize a dictionary where { < table name > : []}
    results= {table["name"]: [] for table in tables}
    # print(f"results: {results}") 
    for chunk in df:
        for table in tables:
            new_df = chunk.dropna(axis=1, how='all').copy()
            new_df.fillna("")
            # print(new_df)
            table_name = table["name"]
            table_column_names = table["columns"]
            # print(type(table_column_names))
            primary_key = table["pk"]
            chunk_columns = new_df.columns.tolist()
            column_names = set(chunk_columns) & set(table_column_names)

            table_data = new_df[list(column_names)] 
            table_data.fillna("''")
            if "rename" in table:
                table_data = update_columns_names(table_data.copy(), table["rename"]) 
            table_data.drop_duplicates(subset = [primary_key], keep = 'first', inplace = True)
            # print(f"{table_data}")
            # print(f"dataframe after NA replaced with empty values are: {table_data}")
            # print(f"the dictionary for {table_name} is: {col_dicts[table_name]}")
            # print(table_data.to_dict(orient='records'))
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

def ingest_to_tdr(tsv_path, dataset_id, instance_type, debug='false'):
    """
    Ingest dataset, given by a tsv path, to TDR

    Args:
        tsv_path (string): The path to the tsv 
        dataset_id (string): The id of the target TDR workspace
        entity_name (string): Name of datamodel that defines table

    """
    if debug.lower() not in boolean_values:
        raise NameError(f" Acceptable debug values are  {boolean_values} \n")
    if instance_type not in instance_types:
        raise NameError(f"instance_type must be one of these options {instance_types} \n")
    if instance_type == "plate_swipe":    
        tables.append(plate_swipes_model)
    elif instance_type == "isolate":
        tables.append(isolate_swipe_model)
    df = pd.read_csv(tsv_path, header=0,
                     iterator=True, sep='\t', chunksize=100,  na_values=['NA', 'N/A'], dtype=str, keep_default_na=False)
    result = df_to_col_dicts_chunked(df, instance_type)
    # print(f"All tables and data to be ingested looks like: {result}")
    for table_name, table_data in result.items():
        print(f"Starting process to ingest table {table_name} for {instance_type} \n")
        # Only ingest tables that have data in them
        if table_data:
            # print(table_data)
            call_ingest_dataset(table_data, table_name, dataset_id, debug=debug.lower())


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest into TDR")

    parser.add_argument('-f', '--tsv', required=True, type=str,
                        help='tsv file of files to ingest to TDR')
    parser.add_argument('-d', '--dataset_id', required=True, type=str,
                        help='id of TDR dataset for destination of outputs')
    parser.add_argument('-i', '--instance_type', required=True, choices= instance_types)
    parser.add_argument('-v', '--verbose', type=str, required=False, default='false', help='Add debugging to stdout')

    args = parser.parse_args()
    try:
        ingest_to_tdr(args.tsv, args.dataset_id, args.instance_type, args.verbose)
    except Exception as e:
        print("An unexpected error occurred:", e)
        traceback.print_exc()
        
