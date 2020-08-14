# from googlecloud_bigquery_class import BigQuery
from google.cloud import bigquery
import pandas as import pd

def call_bigquery(env_query, project_name):
    """Call BigQuery table."""
    # Construct a BigQuery client object.
    bq = bigquery.Client(project=project_name)

    # Start job
    query = bq.query(env_query)
    print("Starting job.")

    # Gather results
    results = query.result()
    print("Job finished.")

    # read RowIterator object into pandas df
    results_df = results.to_dataframe()
    print(results_df)
    return results_df


def get_tables(schema):
    """Get a list of the tables in warehouse_dev."""

    dev_tables = list(schema['table_name']) # type list
    print(dev_tables)
    return dev_tables


# 2. Extract out the names of the tables that we want to look at. Column name = "table_name"

# filters
# limit results to date range: "yesterday" to "six months from yesterday"
# dev warehouse query
# dev_dataset = "broad-dsde-prod-analytics-dev:warehouse_dev"
# get a list of the tables in the dataset


# prod warehouse query
# get results
# prod_dataset = "broad-dsde-prod-analytics-dev:warehouse"

if __name__ == "__main__":
    # hard-coded project name for warehouse tables
    project = "broad-dsde-prod-analytics-dev"

    query_get_dev_schema = "SELECT * FROM broad-dsde-prod-analytics-dev.warehouse_dev.INFORMATION_SCHEMA.TABLES"
    dev_table_schema = call_bigquery(query_get_dev_schema, project)

    get_tables(dev_table_schema)
    
    # call_bigquery(dev_query, project)
    # call_bigquery(prod_query, project)

# compare prod and dev results
    # SELECT * LIMIT 1 and compare column counts, column names, etc.
    # Compare # rows returned within the timeframe defined above.
        # SELECT COUNT(1) to get number of rows in column #1.