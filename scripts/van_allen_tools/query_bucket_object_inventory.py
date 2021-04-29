"""Query Big Query dataset/table with bucket object inventory to get file of paths and sizes of each object.

Usage:
    > python3 query_bucket_object_inventory.py -b SOURCE_BUCKET_NAME

Notes:
    ALL BQ resources must be in the same LOCATION (US) and need to have WRITE access to the DATASET to make TABLES."""
import argparse
import google.auth

from google.cloud import bigquery


# information of the destination table to be exported to a bucket as csv
GCP_PROJECT = "vanallen-gcp-nih"
DATASET_NAME = "gcs_inventory_loader"
INVENTORY_TABLE_NAME = "object_metadata"
EXPORT_TABLE_NAME = "export_bucket_obj_metadata"
SOURCE_DETAILS_CSV_BUCKET = "bigquery-billing-exports"


def bq_setup(project=GCP_PROJECT):
    """Set up credentials and BQ client."""
    credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

    # make clients
    bqclient = bigquery.Client(credentials=credentials,
                               project=project)

    return bqclient


def export_bucket_inventory_table(source_bucket_name):
    """Export query results table to GCS storage location as csv file."""

    # set up bq clients
    bqclient = bq_setup(GCP_PROJECT)

    source_details_filename = f"{source_bucket_name}_source_details.csv"

    destination_uri = f"gs://{SOURCE_DETAILS_CSV_BUCKET}/bucket_inventory_files/{source_details_filename}"
    dataset_name = bigquery.DatasetReference(GCP_PROJECT, DATASET_NAME)
    export_table_name = dataset_name.table(EXPORT_TABLE_NAME)

    # API request, destination format CSV (default)
    extract_job = bqclient.extract_table(export_table_name,
                                         destination_uri,
                                         location="US")
    # wait for job to complete
    job_status = extract_job.result()

    if job_status.errors is not None:
        print(f"WARNING: Failed to extract source_details.txt file for {source_bucket_name}.")
        return False, job_status.errors

    print(f"Successfully exported bucket {source_bucket_name}'s inventory details to {destination_uri}.")
    return True, destination_uri


def create_bucket_inventory_table(source_bucket_name):
    """Query BQ table for bucket metadata and export results (.csv) to GCS storage destination."""

    # set up bq clients
    bqclient = bq_setup(GCP_PROJECT)

    # overwrite tmp table that is used to export contents per bucket to gcs csv file
    job_config = bigquery.QueryJobConfig(destination=f"{GCP_PROJECT}.{DATASET_NAME}.{EXPORT_TABLE_NAME}",
                                         use_legacy_sql=False,
                                         allow_large_results=True,
                                         write_disposition="WRITE_TRUNCATE")

    # query the pre-created inventory table with all bucket inventories
    query_string = f'''WITH single_bucket_inventory AS (
                        SELECT size AS size_bytes, CONCAT("gs://", bucket, "/", name) AS path,
                        FROM `{GCP_PROJECT}`.{DATASET_NAME}.{INVENTORY_TABLE_NAME}
                        WHERE bucket = "{source_bucket_name}"
                        GROUP BY bucket, name, size -- make sure that we don't capture possible duplicate rows
                        ORDER BY size DESC
                        )
                    -- remove all log files
                    SELECT *
                        FROM single_bucket_inventory
                        WHERE path NOT LIKE "%stderr"
                        AND path NOT LIKE "%stdout"
                        AND path NOT LIKE "%script"
                        AND path NOT LIKE "%rc"
                        AND path NOT LIKE "%output"
                        AND path NOT LIKE "%gcs_delocalization.sh"
                        AND path NOT LIKE "%gcs_localization.sh"
                        AND path NOT LIKE "%gcs_transfer.sh"'''

    print(f"Query for single bucket inventory: {query_string}")

    # API request - start query, pass in extra configuration
    query_job = bqclient.query(query_string, job_config=job_config)

    # wait for the job to complete
    job_results = query_job.result()

    if query_job.errors is not None:
        print(f"WARNING: Failed to extract source_details.txt file for {source_bucket_name}.")
        return False, query_job.errors

    print(f"Successfully loaded query results to the table {GCP_PROJECT}.{DATASET_NAME}.{EXPORT_TABLE_NAME}.")
    return True, None


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Query BQ table with GCS bucket object metadata inventory.')

    parser.add_argument('-b', '--source_workspace_bucket', required=True, type=str, help='Source workspace bucket used to query inventory BQ table.')
    parser.add_argument('-o', '--output_file_bucket', type=str, default=SOURCE_DETAILS_CSV_BUCKET, help='Bucket to export BQ tables as csv files.')
    parser.add_argument('-p', '--gcp_project', type=str, default=GCP_PROJECT, help='Google project used for BigQuery.')
    parser.add_argument('-d', '--bq_dataset', type=str, default=DATASET_NAME, help='Destination dataset name in BigQuery project.')
    parser.add_argument('-t', '--bq_inventory_table', type=str, default=INVENTORY_TABLE_NAME, help='Destination table name in BigQuery project.')

    args = parser.parse_args()

    # handle bucket input as gs://bucket or simply bucket
    if args.source_workspace_bucket.startswith("gs://"):
        source_bucket_name = args.source_workspace_bucket.split("/")[2]
    else:
        source_bucket_name = args.source_workspace_bucket

    # call to create and set up workspaces
    table_success, table_message = create_bucket_inventory_table(source_bucket_name)
    if table_success:
        export_bucket_inventory_table(source_bucket_name)
