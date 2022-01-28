import argparse
import datetime
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage as gcs
from oauth2client.client import GoogleCredentials
from pprint import pprint
from time import sleep


# define some utils functions
def get_access_token():
    """Get access token."""
    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)
    return credentials.get_access_token().access_token

def get_headers(request_type='get'):
    headers = {"Authorization": "Bearer " + get_access_token(),
                "accept": "application/json"}
    if request_type == 'post':
        headers["Content-Type"] = "application/json"
    return headers

def write_file_to_bucket(filename, bucket):
    dir = "tdr"
    control_file_destination = f"{bucket}/{dir}"
    storage_client = gcs.Client()
    dest_bucket = storage_client.get_bucket(bucket)
    blob = dest_bucket.blob(f"{dir}/{filename}")
    blob.upload_from_filename(filename)
    control_file_full_path = f"gs://{bucket}/{dir}/{filename}"
    print(f"Successfully copied {loading_json_filename} to {control_file_full_path}.")
    return control_file_full_path

def wait_for_job_status_and_result(job_id, wait_sec=10):
    # first check job status
    uri = f"https://data.terra.bio/api/repository/v1/jobs/{job_id}"

    headers = get_headers()
    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    while status_code == 202:
        print(f"job running. checking again in {wait_sec} seconds")
        sleep(wait_sec)
        response = requests.get(uri, headers=headers)
        status_code = response.status_code

    if status_code != 200:
        print(f"error retrieving status for job_id {job_id}")
        return "internal error", response.text

    job_status = response.json()['job_status']
    print(f'job_id {job_id} has status {job_status}')
    # if job status = done, check job result
    if job_status in ['succeeded', 'failed']:
        result_uri = uri + "/result"
        print(f'retrieving job result from {result_uri}')
        response = requests.get(result_uri, headers=get_headers())

    return job_status, response.json()


def main(dataset_id, bucket, target_table, outputs_json, sample_id, gcp_project_for_query):
    # clean up bucket prefix
    bucket = bucket.replace("gs://","")

    # read workflow outputs from file
    print(f"reading data from outputs_json file {outputs_json}")
    with open(outputs_json, "r") as infile:
        outputs_to_load = json.load(infile)

    # recode any paths (files) for TDR ingest
    print("recoding paths for TDR ingest")
    for k in outputs_to_load.keys():
        v = outputs_to_load[k]
        if v is not None and "gs://" in v:
            outputs_to_load[k] = {
                "sourcePath": v,
                "targetPath": v.replace("gs://", "/")
            }

    # get BQ access info for TDR dataset
    print("retrieving BQ access info for TDR dataset")
    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}?include=ACCESS_INFORMATION"
    response = requests.get(uri, headers=get_headers())
    tables = response.json()['accessInformation']['bigQuery']['tables']
    dataset_table_fq = None  # fq = fully qualified name, i.e. project.dataset.table
    for table_info in tables:
        if table_info['name'] == target_table:
            dataset_table_fq = table_info['qualifiedName']

    # retrieve data for this sample
    print(f"retrieving data for sample_id {sample_id} from {dataset_table_fq}")
    bq = bigquery.Client(gcp_project_for_query)
    query = f"SELECT * FROM \`{dataset_table_fq}\` WHERE sample_id = '{sample_id}'"
    print("using query:" + query)

    executed_query = bq.query(query)
    results = executed_query.result()

    # this avoids the pyarrow error that arises if we use `df_result = result.to_dataframe()`
    df = results.to_dataframe_iterable()
    reader = next(df)
    df_result = pd.DataFrame(reader)

    # break if there's more than one row in TDR for this sample
    print(f"retrieved {len(df_result)} samples matching sample_id {sample_id}")
    assert(len(df_result) == 1)

    # format to a dictionary
    print("formatting results to dictionary")
    input_data_list = []
    for row_id in df_result.index:
        row_dict = {}
        for col in df_result.columns:
            if isinstance(df_result[col][row_id], pd._libs.tslibs.nattype.NaTType):
                value = None
            elif isinstance(df_result[col][row_id], pd._libs.tslibs.timestamps.Timestamp):
                print(f'processing timestamp. value pre-formatting: {df_result[col][row_id]}')
                formatted_timestamp = df_result[col][row_id].strftime('%Y-%m-%dT%H:%M:%S')
                print(f'value post-formatting: {formatted_timestamp}')
                value = formatted_timestamp
            else:
                value = df_result[col][row_id]
            if value is not None:  # don't include empty values
                row_dict[col] = value
        input_data_list.append(row_dict)

    sample_data_dict = input_data_list[0]

    # update original sample data with workflow outputs
    sample_data_dict.update(outputs_to_load)
    # remove and store datarepo_row_id
    old_datarepo_row_id = sample_data_dict.pop('datarepo_row_id')
    # update version_timestamp field
    new_version_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    sample_data_dict['version_timestamp'] = new_version_timestamp

    # write update json to disk and upload to staging bucket
    loading_json_filename = f"{sample_id}_{new_version_timestamp}_recoded_ingestDataset.json"
    with open(loading_json_filename, 'w') as outfile:
        outfile.write(json.dumps(sample_data_dict))
        outfile.write("\n")
    load_file_full_path = write_file_to_bucket(loading_json_filename, bucket)

    # ingest data to TDR
    load_json = json.dumps({"format": "json",
                        "path": load_file_full_path,
                        "table": target_table,
                        "resolve_existing_files": True,
                        })
    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/ingest"
    response = requests.post(uri, headers=get_headers('post'), data=load_json)
    load_job_id = response.json()['id']
    job_status, job_info = wait_for_job_status_and_result(load_job_id)
    if job_status != "succeeded":
        print(f"job status {job_status}:")
        print(job_info)

    # soft delete old row
    print("beginning soft delete")
    soft_delete_data = json.dumps({
            "deleteType": "soft", 
            "specType": "jsonArray",
            "tables": [
            {"jsonArraySpec": {"rowIds": [old_datarepo_row_id]},
                "tableName": target_table}
            ]})
    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/deletes"
    response = requests.post(uri, headers=get_headers('post'), data=soft_delete_data)

    print("probing soft delete job status")
    if "id" not in response.json():
        pprint(response.text)
    else:
        sd_job_id = response.json()['id']

    job_status, job_info = wait_for_job_status_and_result(sd_job_id)
    if job_status != "succeeded":
        print(f"job status {job_status}:")
        print(job_info)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest workflow outputs to TDR')
    parser.add_argument('-d', '--dataset_id', required=True,
        help='UUID of destination TDR dataset')
    parser.add_argument('-b', '--bucket', required=True,
        help='GCS bucket to use for TDR API json payloads')
    parser.add_argument('-t', '--target_table', required=True,
        help='name of destination table in the TDR dataset')
    parser.add_argument('-o', '--outputs_json', required=True,
        help='path to a json file defining the outputs to be loaded to TDR')
    parser.add_argument('-s', '--sample_id', required=True,
        help='the sample_id of the sample to be ingested')
    parser.add_argument('-p', '--gcp_project_for_query', required=True,
        help='GCP project to use for querying TDR/BQ dataset')
        
    args = parser.parse_args()

    main(args.dataset_id, args.bucket, args.target_table, args.outputs_json, args.sample_id, args.gcp_project_for_query)
