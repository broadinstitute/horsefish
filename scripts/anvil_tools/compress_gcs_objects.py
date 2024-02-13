import argparse
import google.auth

from google.cloud import bigquery


def bq_setup(gcp_project):
    """Set up credentials and BQ client."""
    credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

    # make client
    bqclient = bigquery.Client(credentials=credentials,
                               project=gcp_project)

    return bqclient


def get_uncompressed_objects(gcp_project, bq_dataset, bq_dataset_table, tar_object):
    """Query to return uncompressed object paths given a "grouping" term."""

    # create bq client
    bqclient = bq_setup(gcp_project)

    query_string = f'''SELECT  src_object
                       FROM `{gcp_project}`.{bq_dataset}.{bq_dataset_table}
                       WHERE tar_object = "{tar_object}"
                    '''

    print(f"Querying for uncompressed objects using query: {query_string}")

    # API request - start query, pass in extra configuration
    query_job = bqclient.query(query_string)
    
    # wait for the job to complete
    job_results = query_job.result()

    if query_job.errors is not None:
        print(f"WARNING: Failed to extract uncompressed objects for tar_object: {tar_object}.\n\n")
        return

    uncompressed_objects_df = job_results.to_dataframe()
    outfile_name = "uncompressed_objects_to_compress.tsv"
    uncompressed_objects_df.to_csv(outfile_name, header=False, index=False)
    print(f"Successfully generated {outfile_name} containing list of uncompressed objects belonging to final: {tar_object}\n\n")

    return uncompressed_objects_df


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Query BQ table with GCS bucket object metadata inventory.')

    parser.add_argument('-g', '--gcp_project', type=str, help='Google project used for BigQuery.')
    parser.add_argument('-d', '--bq_dataset', type=str, help='Dataset name in BigQuery project.')
    parser.add_argument('-t', '--bq_dataset_table', type=str, help='Table name in BigQuery dataset.')
    parser.add_argument('-z', '--tar_object', type=str, help='Final tar.gz GCS url.')

    args = parser.parse_args()

    uncompressed_objects_df = get_uncompressed_objects(args.gcp_project, args.bq_dataset, args.bq_dataset_table, args.tar_object)