"""WILL FILL OUT."""
# from googlecloud_bigquery_class import BigQuery
from google.cloud import bigquery
# import pandas as pd


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
    dev_tables = list(schema['table_name'])  # type list
    dev_tables.remove("rawls_entity")
    dev_tables.remove("rawls_entity_attribute")
    dev_tables.remove("cromwell_metadata")
    print(f"List of dev tables in {dev_tables}")
    return dev_tables


def get_newrows(table_names):
    """Get the list of new row in dev."""
    project = "broad-dsde-prod-analytics-dev"
    for table_name in table_names:
        print(table_name)
        query_get_dev_schema = f"""WITH
        DevData AS (
            SELECT
                allDevData AS data,
                FARM_FINGERPRINT(FORMAT("%T", allDevData)) AS allRows
            FROM
                `broad-dsde-prod-analytics-dev.warehouse_dev.{table_name}` AS allDevData
        ),
        RawData AS (
            SELECT
                allRawData AS data,
                FARM_FINGERPRINT(FORMAT("%T", allRawData)) AS allRows
            FROM
                `broad-dsde-prod-analytics-dev.warehouse.{table_name}` AS allRawData
        )
        SELECT
            IF(DevData.allRows IS NULL,"Not in dev","Not in Warehouse") AS Change,
            IF(DevData.allRows IS NULL,RawData.data,DevData.data).*
        FROM
            DevData
        FULL OUTER JOIN RawData
            ON DevData.allRows = RawData.allRows
        WHERE
            RawData.allRows IS NULL"""

        dev_dataset_schema = call_bigquery(query_get_dev_schema, project)

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
    dev_dataset_schema = call_bigquery(query_get_dev_schema, project)

    dev_table_list = get_tables(dev_dataset_schema)
    get_newrows(dev_table_list)

    
    # call_bigquery(dev_query, project)
    # call_bigquery(prod_query, project)

# compare prod and dev results
    # SELECT * LIMIT 1 and compare column counts, column names, etc.
    # Compare # rows returned within the timeframe defined above.
        # SELECT COUNT(1) to get number of rows in column #1.