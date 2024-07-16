"""This script soft deletes a list of rows from a TDR dataset."""

import argparse
import json
import requests
from pprint import pprint

from tdr_utils import wait_for_job_status_and_result
from utils import get_headers


def call_soft_delete_rows(rows_to_soft_delete, dataset_id, table):
    """Soft delete rows from a given table in a dataset."""

    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/deletes"

    soft_delete_data = json.dumps(
      {
        "deleteType": "soft",
        "specType": "jsonArray",
        "tables": [
          {
            "jsonArraySpec":
            {
              "rowIds": rows_to_soft_delete
            },
            "tableName": table
          }
        ]
      }
    )

    response = requests.post(uri, headers=get_headers('post'), data=soft_delete_data)
    status_code = response.status_code

    if status_code != 202:
        return response.text

    print(f"Successfully submitted job to soft delete rows in datasetID {dataset_id}.")
    return response.json()


def soft_delete_rows(rows_to_delete, table, dataset_id, staging_bucket, verbose=True):
    pprint(f"preparing to soft delete {len(rows_to_delete)} rows from dataset {dataset_id}, table {table}")

    # soft delete old rows
    soft_delete_result = call_soft_delete_rows(rows_to_delete, dataset_id, table)
    soft_delete_job_id = soft_delete_result['id']

    _, response = wait_for_job_status_and_result(soft_delete_job_id, wait_sec=5)
    if verbose:
        pprint(response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--dataset_id', '-d', type=str, help='TDR dataset id (uuid) where data should be ingested')
    parser.add_argument('--table', '-t', type=str, default='sample', help='TDR table to delete the rows from')

    # one of the following two inputs is required
    parser.add_argument('--rows_to_delete', '-r', nargs='+', help="space separated list of datarepo row ids to delete, e.g. `-r row1 row2 row3`")
    parser.add_argument('--rows_to_delete_file', '-f', help="path to file containing newline-delimited list of datarepo row ids to delete")

    parser.add_argument('--verbose', '-v', action='store_true', help='print progress text')

    args = parser.parse_args()

    # check and prepare rows to delete
    if (args.rows_to_delete is None) and (args.rows_to_delete_file is None):
        print("No rows to delete were specified. Please specify either --rows_to_delete (-r) or --rows_to_delete_file (-f).")
        exit(1)
    elif args.rows_to_delete and args.rows_to_delete_file:
        print("You specified both rows_to_delete and rows_to_delete_file; please use only one.")
        exit(1)
    elif args.rows_to_delete_file:
        with open(args.rows_to_delete_file, 'r') as infile:
            rows_to_delete_raw = infile.readlines()
        rows_to_delete = [row.rstrip('\n') for row in rows_to_delete_raw]
    else:
        rows_to_delete = args.rows_to_delete

    soft_delete_rows(rows_to_delete, args.table, args.dataset_id, args.verbose)