"""Common functions used for TDR scripts."""

import datetime
import json
import pandas as pd

from google.cloud import storage as gcs
from oauth2client.client import GoogleCredentials


def open_config_file(config_file_path):
    with open(config_file_path) as json_data_file:
        config_data = json.load(json_data_file)
    return config_data


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


def get_current_timestamp_str():
    """Returns a formatted timestamp string for the current time in UTC, formatted like 'YYYY-MM-DDTHH:MM:SS' (this is the timestamp format requested by BQ)"""
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')


def convert_df_to_json(df):
    """Converts a Pandas dataframe containing BQ results to a list of dicts"""
    output_list = []

    for row_id in df.index:
        row_dict = {}
        for col in df.columns:
            if isinstance(df[col][row_id], pd._libs.tslibs.nattype.NaTType):
                value = None
            else:
                value = df[col][row_id]
            if value is not None:  # don't include empty values
                row_dict[col] = value
        output_list.append(row_dict)

    return output_list


def write_file_to_bucket(file_name, bucket, dir='control_files'):

    control_file_destination = f"{bucket}/{dir}"

    storage_client = gcs.Client()
    dest_bucket = storage_client.get_bucket(bucket)

    blob = dest_bucket.blob(f"control_files/{file_name}")
    blob.upload_from_filename(file_name)

    print(f"Successfully copied {file_name} to {control_file_destination}.")

    return f"gs://{bucket}/{dir}/{file_name}"