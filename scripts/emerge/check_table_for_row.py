# imports and environment variables
import argparse
import json
import pandas as pd
import pytz
import requests
import time

from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage as gcs
from oauth2client.client import GoogleCredentials


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def write_file(dataframe):
    """Write file."""

    output_filename = f"search_result.tsv"

    print(f"Writing query result to tsv file.")
    dataframe.to_csv(output_filename, sep='\t')

    return output_filename


def run_query(query, google_project="emerge-production"):
    """Performs a BQ lookup of a desired attribute in a specified snapshot or dataset table."""
    
    # create BQ connection
    bq = bigquery.Client(google_project)
    
    print(f"Executing query.")
    executed_query = bq.query(query)
    result = executed_query.result()
    
    df_result = result.to_dataframe()
    if df_result.empty:
        raise ValueError("Query resulted in no rows found.")

    return df_result


def get_dataset_access_info(dataset_id):
    """"Get dataset access details from retrieveDataset API given a datasetID."""
    
    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}?include=ACCESS_INFORMATION"
    
    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}
    
    response = requests.get(uri, headers=headers)
    status_code = response.status_code
    
    if status_code != 200:
        return response.text
    
    print(f"Successfully retrieved access information for dataset with datasetID {dataset_id}.")
    return json.loads(response.text)


def get_fq_table(entity_id, table_name):
    """Given a datset or snapshot id, table name, and entity type {dataset,snapshot}, retrieve its fully qualified BQ table name"""
    
    access_info = get_dataset_access_info(entity_id)
    tables = access_info['accessInformation']['bigQuery']['tables']

    # pull out desired table
    table_fq = None  # fq = fully qualified name, i.e. project.dataset.table
    for table_info in tables:
        if table_info['name'] == table_name:
            table_fq = table_info['qualifiedName'] 

    return table_fq


def create_query(dataset_id, table_name, attribute_name, attribute_value):
    """Create query to get back the ingested row."""

    fq_table_name = get_fq_table(dataset_id, table_name)
    query = f"""SELECT * FROM `{fq_table_name}`
                WHERE {attribute_name} = '{attribute_value}'"""
    
    return query


if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='Check TDR dataset table for presence of a row.')

    parser.add_argument('-d', '--dataset_id', required=True, type=str, help='id of TDR dataset for destination of outputs')
    parser.add_argument('-t', '--table_name', required=True, type=str, help='name of target table in TDR dataset')
    parser.add_argument('-a', '--attribute_name', required=True, type=str, help='name of target table in TDR dataset')
    parser.add_argument('-v', '--attribute_value', required=True, type=str, help='name of target table in TDR dataset')

    args = parser.parse_args()

    query = create_query(args.dataset_id, args.table_name, args.attribute_name, args.attribute_value)
    result_df = run_query(query)
    write_file(result_df)
    