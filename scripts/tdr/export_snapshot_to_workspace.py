# imports and environment variables
import argparse
import json
import pandas as pd
import pytz
import requests
from time import sleep

from datetime import datetime
from google.cloud import storage as gcs
from oauth2client.client import GoogleCredentials


def get_access_token():
    """Get access token."""

    scopes = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    credentials = GoogleCredentials.get_application_default()
    credentials = credentials.create_scoped(scopes)

    return credentials.get_access_token().access_token


def wait_for_job_status_and_result(job_id, wait_sec=10):
    """Check job status and return job result."""
    
    # check job status
    uri = f"https://data.terra.bio/api/repository/v1/jobs/{job_id}"

    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json"}

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    while status_code == 202:
        print(f"job running. checking again in {wait_sec} seconds")
        sleep(wait_sec)
        response = requests.get(uri, headers=headers)
        status_code = response.status_code

    if status_code != 200:
        return response.text

    job_status = response.json()['job_status']
    print(f'job_id {job_id} has status {job_status}')
    # if job status = done, check job result
    if job_status in ['succeeded', 'failed']:
        print('retrieving job result')
        response = requests.get(uri + "/result", headers=headers)
        status_code = response.status_code

    return response.json()


def export_snapshot_to_workspace(project, workspace, manifest_url):
    """Export snapshot-by-copy to a workspace."""

    uri = f"https://api.firecloud.org/api/workspaces/{project}/{workspace}/importJob"

    data = {"filetype": "tdrexport",
            "url": {manifest_url},
            "options": {"tdrSyncPermissions": "true"}
            }
    
    # need to get job id of the final export etc
    # error handling


def call_export_snapshot(snapshot_id, use_gs_paths):
    """Call exportSnapshot for job_id."""

    # call exportSnapshot to get manifest url
    uri = f"https://data.terra.bio/api/repository/v1/snapshots/{snapshot_id}/export?exportGsPaths={use_gs_paths}&validatePrimaryKeyUniqueness=true"

    headers = {"Authorization": "Bearer " + get_access_token(),
               "accept": "application/json",
               "Content-Type": "application/json"}

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    # job fails to start
    if status_code != 202:
        raise ValueError(response.text)

    # job successfully starts
    # get job_id
    job_id = response.json()["id"]
    return job_id


def get_snapshot_manifest_url(snapshot_id, use_gs_paths):
    """Get manifest url for snapshot."""

    job_id = call_export_snapshot(snapshot_id, use_gs_paths)
    job_response = wait_for_job_status_and_result(job_id)

    manifest_url = job_response["format"]["parquet"]["manifest"]
    
    return manifest_url
    

if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='Push Arrays.wdl outputs to TDR dataset.')

    parser.add_argument('-p', '--project', required=True, type=str, help='terra workspace project_id/namespace')
    parser.add_argument('-w', '--workspace', required=True, type=str, help='terra workspace name')
    parser.add_argument('-s', '--snapshot_id', required=True, type=str, help='tdr snapshot uuid')
    parser.add_argument('-g', '--use_gs_paths', required=False, default=False, type=bool, help='convert file paths to gs paths')

    args = parser.parse_args()

    manifest_url = get_snapshot_manifest_url(args.snapshot_id, args.use_gs_paths)
    export_snapshot_to_workspace(args.project, args.workspace, manifest_url)