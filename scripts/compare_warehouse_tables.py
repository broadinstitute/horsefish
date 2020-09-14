"""Compare the dev_warehouse with the prod warehouse to make sure that changes to code did not break functionality."""
from datetime import datetime, timedelta
from google.cloud import bigquery
from tabulate import tabulate
import pandas as pd
import argparse
import warnings

# TODO Setup For Running The Code:
# 1. Make sure to update warehouse_dev:
#   A. Go to https://fcprod-jenkins.dsp-techops.broadinstitute.org
#   B. Go to the Data Warehouse tab (if applicable)
#   C. Click the WIP-data-warehouse-python-development job
#   D. Check if code is pulling from */master branch (Info found in Configure Section, Under Source Code Management -> Branches to build)
#   E. In the WIP-data-warehouse-python-development job, press Build Now
# 2. Run "gcloud auth login" with firecloud.org email
# 3. Run "gcloud auth application-default login" with firecloud.org email
# 4. Run "gcloud auth application-default set-quota-project broad-dsde-dev"


def call_bigquery(bq, env_query, project_name, verbose=False):
    """Call BigQuery table."""
    # Start job
    query = bq.query(env_query)
    if verbose:
        print("Starting job.")

    # Gather results
    results = query.result()
    if verbose:
        print("Job finished.")

    # read RowIterator object into pandas df
    results_df = results.to_dataframe()
    if verbose:
        print(results_df)
    return results_df


def get_tables(schema, verbose=False):
    """Get a list of the tables in warehouse_dev."""
    # Getting the list of tables
    dev_tables = list(schema['table_name'])

    # Remove tables not in Prod or too long to parse
    dev_tables.remove("rawls_entity")
    dev_tables.remove("rawls_entity_attribute")
    dev_tables.remove("cromwell_metadata")
    if verbose:
        print(f"List of dev tables in {dev_tables}")
    return dev_tables


def get_newrows(bq, project, table_names, tables_with_timestamps, verbose):
    """Getting the table and information of new rows in dev and prod."""
    # Creating the master list of the new rows in dev and prod for the table
    results_newrows = []

    # Getting today date
    today = datetime.today()

    # initializing the time count variable that Loops through the dates: today, yesterday, 1 month and six months
    time_count = 0

    # Initializing the Boolean has_timestamp that checks if the table has a timestamp
    has_timestamp = False

    # Initializing the more_info variable that will output extra information of the tables
    more_info = ""

    # Looping through all the table names for columns and rows match, and extra rows in Dev and prod
    for table_name in table_names:
        row_result = []
        time_iteration = True
        not_matched = False
        not_matched_info = ""
        newrows = [table_name]
        while time_iteration:
            # Checking if table has a timestamps and (if applicable) looping through today, yesterday, one month ago, and six month ago
            if table_name in tables_with_timestamps:
                if time_count == 0:
                    has_timestamp = True
                    not_matched = False
                    not_matched_info += ""
                    timestamp_DevData = f" WHERE Date(allDevData.{tables_with_timestamps[table_name]}) = '{today.date()}'"
                    timestamp_ProdData = f" WHERE Date(allProdData.{tables_with_timestamps[table_name]}) = '{today.date()}'"
                if time_count == 1:
                    one_day = today - timedelta(days=1)
                    timestamp_DevData = f" WHERE Date(allDevData.{tables_with_timestamps[table_name]}) = '{one_day.date()}'"
                    timestamp_ProdData = f" WHERE Date(allProdData.{tables_with_timestamps[table_name]}) = '{one_day.date()}'"
                    row_result = extra_rows
                if time_count == 2:
                    one_month = today - timedelta(days=31)
                    timestamp_DevData = f" WHERE Date(allDevData.{tables_with_timestamps[table_name]}) = '{one_month.date()}'"
                    timestamp_ProdData = f" WHERE Date(allProdData.{tables_with_timestamps[table_name]}) = '{one_month.date()}'"
                    if extra_rows["in_Prod_not_Dev"].values[0] is not None and extra_rows["in_Dev_not_Prod"].values[0] is not None:
                        not_matched = True
                        not_matched_info += " yesterday :"
                if time_count == 3:
                    six_month = today - timedelta(days=186)
                    timestamp_DevData = f" WHERE Date(allDevData.{tables_with_timestamps[table_name]}) = '{six_month.date()}'"
                    timestamp_ProdData = f" WHERE Date(allProdData.{tables_with_timestamps[table_name]}) = '{six_month.date()}'"
                    if extra_rows["in_Prod_not_Dev"].values[0] is not None and extra_rows["in_Dev_not_Prod"].values[0] is not None:
                        not_matched = True
                        not_matched_info += " One Month :"
                time_count += 1
            else:
                timestamp_DevData = ""
                timestamp_ProdData = ""
                time_iteration = False
                has_timestamp = False

            # Initializing query
            extra_rows_query = f"""WITH
            DevData AS (
                SELECT
                    FARM_FINGERPRINT(FORMAT("%T", allDevData)) AS allRows
                FROM
                    `broad-dsde-prod-analytics-dev.warehouse_dev.{table_name}` AS allDevData
                {timestamp_DevData}
            ),
            ProdData AS (
                SELECT
                    FARM_FINGERPRINT(FORMAT("%T", allProdData)) AS allRows
                FROM
                    `broad-dsde-prod-analytics-dev.warehouse.{table_name}` AS allProdData
                {timestamp_ProdData}
            )
            SELECT
                Sum(IF(ProdData.allRows IS NULL,1,0)) AS in_Dev_not_Prod,
                Sum(IF(DevData.allRows IS NULL,1,0)) AS in_Prod_not_Dev
            FROM
                DevData
            FULL OUTER JOIN ProdData
                ON DevData.allRows = ProdData.allRows
            WHERE
                ProdData.allRows IS NULL OR DevData.allRows IS NULL"""

            # Calling big query to get In_Dev_Only and In_Prod_Only extra rows count
            extra_rows = call_bigquery(bq, extra_rows_query, project)

            if time_count == 4:
                if extra_rows["in_Prod_not_Dev"].values[0] is not None and extra_rows["in_Dev_not_Prod"].values[0] is not None:
                    not_matched = True
                    not_matched_info += " Six Month :"
                time_iteration = False
                timestamp_DevData = ""
                timestamp_ProdData = ""
                time_count = -1

        # Assigning row relust for tablw without a timestamp
        if not has_timestamp:
            row_result = extra_rows

        # Getting Dev and Prod table colunms
        dev_table = bq.get_table(f"warehouse_dev.{table_name}")
        prod_table = bq.get_table(f"warehouse.{table_name}")
        prod_table_columns = ["{0}".format(prod_schema.name) for prod_schema in prod_table.schema]
        dev_table_columns = ["{0}".format(dev_schema.name) for dev_schema in dev_table.schema]

        # Getting In_Dev_Only and In_Prod_Only count from query
        dev_table_rows = int(row_result["in_Dev_not_Prod"])
        prod_table_rows = int(row_result["in_Prod_not_Dev"])

        # Adding Columns_Match? boolean and percentage (if not 100%) to the newrow row
        if prod_table_columns == dev_table_columns:
            newrows.append("True")
        else:
            newrows.append("False")
            more_info += f"{table_name} table columns Not Matched on " + (", ".join(list(list(set(prod_table_columns) - set(dev_table_columns)) + list(set(dev_table_columns) - set(prod_table_columns))))) + " \n"

        # Adding Rows_Match boolean and percentage (if not 100%) to the newrow row
        if dev_table_rows is None:
            newrows.append("True")
        else:
            match_percent = int(100 - (((dev_table_rows + prod_table_rows) / dev_table.num_rows) * 100))
            if match_percent > 85:
                newrows.append("True (" + str(match_percent) + "%)")
            else:
                newrows.append("False")
                more_info += f"{table_name} table rows Not Matched, only (" + str(match_percent) + "%) the same \n"

        # Adding information about today, yesterday, one month ago, and six month ago matchs to more info
        if has_timestamp and not_matched:
            more_info += f"{table_name} table didn't 100% match: " + not_matched_info + " \n"
        elif has_timestamp:
            more_info += f"{table_name} table match 100% yesterday, one month, six month \n"

        # Adding In_Dev_Only count to the newrow row
        newrows.append(dev_table_rows)

        # Adding In_Prod_Only count to the newrow row
        newrows.append(prod_table_rows)

        # Adding newrow row to the master table list
        results_newrows.append(newrows)

    # Creating the table of new rows in dev and prod from results_newrows
    newrows_df = pd.DataFrame(results_newrows, columns=["Table_Name", "Columns_Match?", "Rows_Match?", "In_Dev_Only", "In_Prod_Only"])
    
    # Returning the table and information of new rows in dev and prod.
    return newrows_df, more_info


if __name__ == "__main__":

    # Optional Verbose Option
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--verbose', "-v", type=str, default=False, help='Verbose')
    args = parser.parse_args()
    verbose = args.verbose

    if not verbose:
        # Ignore all warning outputs
        warnings.filterwarnings("ignore")

    # Hard-coded project name for warehouse tables
    project = "broad-dsde-prod-analytics-dev"

    # Construct a BigQuery client object.
    bq = bigquery.Client(project)

    # Getting all the warehouse tables
    query_get_dev_schema = "SELECT * FROM broad-dsde-prod-analytics-dev.warehouse_dev.INFORMATION_SCHEMA.TABLES"
    dev_dataset_schema = call_bigquery(bq, query_get_dev_schema, project, verbose)

    # Getting all the warehouse tables names
    dev_table_list = get_tables(dev_dataset_schema, verbose)

    # Dict with the warehouse tables that has a date value 
    tables_with_timestamps = {"terra_nps": "timestamp",
                                "rawls_workspaces": "last_modified",
                                "rawls_access_logs": "timestamp",
                                "rawls_submissions": "DATE_SUBMITTED",
                                "leo_access_logs": "timestamp",
                                "rawls_workflows": "STATUS_LAST_CHANGED",
                                "orchestration_access_logs": "timestamp"}

    # Getting the table and information of new rows in dev and prod, and printing it out
    newrows_table, more_info = get_newrows(bq, project, dev_table_list, tables_with_timestamps, verbose)
    print(tabulate(newrows_table, headers='keys', tablefmt='pretty'))
    print("More Information:")
    print(more_info)

