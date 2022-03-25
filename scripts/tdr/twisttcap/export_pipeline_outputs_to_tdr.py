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
    storage_client = gcs.Client()
    dest_bucket = storage_client.get_bucket(bucket)
    blob = dest_bucket.blob(f"{dir}/{filename}")
    blob.upload_from_filename(filename)
    control_file_full_path = f"gs://{bucket}/{dir}/{filename}"
    print(f"Successfully copied {filename} to {control_file_full_path}.")
    return control_file_full_path

def configure_path_json(v):
    tdr_path = v.replace("gs://","/")

    return { 
        "sourcePath": v,
        "targetPath": tdr_path
    }

def configure_list(v_list):
    v_list_recoded = []

    # check if any of the list values are non-string types
    for v in v_list:
        if isinstance(v, str) and v.startswith("gs://"):
            # update json for loading files
            v_list_recoded.append(configure_path_json(v))
        else:
            # don't change it
            v_list_recoded.append(v)
    
    return v_list_recoded

def recode_json_with_filepaths(json_object):
    """Takes a dict, transforms files for upload as needed for TDR ingest, returns updated dict."""
    for k in json_object.keys():
        v = json_object[k]
        if v is None:
            # nothing needed
            continue
        
        if isinstance(v, str) and v.startswith("gs://"):
            # update json for loading files
            json_object[k] = configure_path_json(v)
        elif isinstance(v, str) and v.startswith("[") and v.endswith("]"):  # if value is an array
            v_list = json.loads(v)  # convert <str> to <list>
            json_object[k] = configure_list(v_list)
        elif isinstance(v, list):
            json_object[k] = configure_list(v)

    return json_object

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


def clean_bucket_path(bucket_input):
    """Removes gs:// prefix and trailing /"""
    return bucket_input.replace("gs://", "").rstrip("/")


def get_fq_bq_table(dataset_id, target_table):
    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}?include=ACCESS_INFORMATION"
    response = requests.get(uri, headers=get_headers())
    tables = response.json()['accessInformation']['bigQuery']['tables']
    dataset_table_fq = None  # fq = fully qualified name, i.e. project.dataset.table
    for table_info in tables:
        if table_info['name'] == target_table:
            dataset_table_fq = table_info['qualifiedName']

    if not dataset_table_fq:
        # no table found
        error_msg = f"No table named {target_table} was found in dataset {dataset_id}"
        raise ValueError(error_msg)

    return dataset_table_fq


def get_existing_data(dataset_table_fq, primary_key_field, primary_key_value):
    gcp_project = dataset_table_fq.split('.')[0]
    bq = bigquery.Client(gcp_project)
    query = f"SELECT * FROM `{dataset_table_fq}` WHERE {primary_key_field} = '{primary_key_value}'"
    print("using query:" + query)

    executed_query = bq.query(query)
    result = executed_query.result()

    # this avoids the pyarrow error that arises if we use `df_result = result.to_dataframe()`
    df = result.to_dataframe_iterable()
    reader = next(df)
    df_result = pd.DataFrame(reader)

    # break if there's more than one row in TDR for this sample
    n_rows = len(df_result)
    print(f"retrieved {n_rows} rows matching {primary_key_field} `{primary_key_value}`")
    assert(n_rows == 1)

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

    # we have already asserted that there is only one row
    return input_data_list[0]


def main(dataset_id, bucket_input, target_table, outputs_json, pk_field, pk_value):
    # clean up bucket string
    bucket = clean_bucket_path(bucket_input)

    # read workflow outputs from file
    print(f"reading data from outputs_json file {outputs_json}")
    with open(outputs_json, "r") as infile:
        outputs_dict = json.load(infile)

    # recode any paths (files) for TDR ingest
    print("recoding paths for TDR ingest")
    outputs_to_add = recode_json_with_filepaths(outputs_dict)

    # get BQ access info for TDR dataset
    print("retrieving BQ access info for TDR dataset")
    dataset_table_fq = get_fq_bq_table(dataset_id, target_table)

    # retrieve data for this row
    print(f"retrieving data for {pk_field} {pk_value} from {dataset_table_fq}")
    row_data = get_existing_data(dataset_table_fq, pk_field, pk_value)

    # update original row data with workflow outputs
    row_data.update(outputs_to_add)
    # remove and store datarepo_row_id
    old_datarepo_row_id = row_data.pop('datarepo_row_id')
    print(f"Replacing data from row with datarepo_row_id {old_datarepo_row_id}")

    # update version_timestamp field
    new_version_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    row_data['version_timestamp'] = new_version_timestamp

    # write update json to disk and upload to staging bucket
    loading_json_filename = f"{pk_value}_{new_version_timestamp}_recoded_ingestDataset.json"
    with open(loading_json_filename, 'w') as outfile:
        outfile.write(json.dumps(row_data))
        outfile.write("\n")
    load_file_full_path = write_file_to_bucket(loading_json_filename, bucket)

    # ingest data to TDR
    load_json = json.dumps({"format": "json",
                        "path": load_file_full_path,
                        "table": target_table,
                        "resolve_existing_files": True,
                        "updateStrategy": "replace"
                        })
    uri = f"https://data.terra.bio/api/repository/v1/datasets/{dataset_id}/ingest"
    response = requests.post(uri, headers=get_headers('post'), data=load_json)
    load_job_id = response.json()['id']
    job_status, job_info = wait_for_job_status_and_result(load_job_id)
    if job_status != "succeeded":
        print(f"job status {job_status}:")
        message = job_info["message"]
        detail = job_info["errorDetail"]
        error_msg = f"{message}: {detail}"
        raise ValueError(error_msg)


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
    parser.add_argument('-k', '--primary_key_field', required=True,
        help='the name of the table\'s primary_key field')
    parser.add_argument('-v', '--primary_key_value', required=True,
        help='the primary key value for the row to update')
        
    args = parser.parse_args()

    main(args.dataset_id,
         args.bucket,
         args.target_table,
         args.outputs_json,
         args.primary_key_field,
         args.primary_key_value)
