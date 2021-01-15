"""Getting the list of workspaces in a project from the warehouse and saving as txt file."""
from google.cloud import bigquery
import argparse


def call_bigquery(bq, env_query, verbose=False):
    """Call BigQuery table."""
    # Starting job
    query = bq.query(env_query)
    if verbose:
        print("Starting job.")
        print(env_query)

    # Gathering results
    results = query.result()
    if verbose:
        print("Job finished.")

    # Reading RowIterator object into pandas df
    results_df = results.to_dataframe()
    if verbose:
        print(results_df)
    return results_df


def create_workspaces_list(terra_project, verbose):
    """Creating a txt file that contains the list of workspaces in a project."""
    # Constructing a BigQuery client object
    bq = bigquery.Client("broad-dsde-prod-analytics-dev")

    # Assigning query for getting all the workspaces in a terra project
    query_get_workspaces_list = f"SELECT name FROM `broad-dsde-prod-analytics-dev.warehouse.rawls_workspaces` WHERE namespace =  '{terra_project}'"

    # Getting workspaces list nested
    workspaces_list_nested = call_bigquery(bq, query_get_workspaces_list, verbose)

    # Getting workspaces list unnested
    workspaces_list = [item[0] for item in workspaces_list_nested.values]

    # Creating Json File with workspace list
    with open(f'{terra_project}_workspaces_list.txt', 'w') as f:
        f.write(str(workspaces_list))


if __name__ == "__main__":

    # Optional Verbose and terra_project args
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--verbose', "-v", action="store_true", help='Verbose')
    parser.add_argument('--terra_project', "-tp", type=str, default="anvil-datastorage", help='Terra Project/Namespace')
    args = parser.parse_args()

    # Assigning verbose variable
    verbose = args.verbose

    # Assigning terra_project variable
    terra_project = args.terra_project

    # Creating a txt file that contains the list of workspaces in a project
    create_workspaces_list(terra_project, verbose)
