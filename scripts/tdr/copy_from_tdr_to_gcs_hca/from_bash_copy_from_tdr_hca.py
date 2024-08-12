import os
import sys
import csv
import logging
import re
import requests
import google.auth
import google.auth.transport.requests
import subprocess

STAGING_AREA_BUCKETS = {
        "EBI": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
        "UCSC": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
        "LANTERN": "gs://broad-dsp-monster-hca-prod-lantern",
        "LATTICE": "gs://broad-dsp-monster-hca-prod-lattice/staging",
        "TEST": "gs://broad-dsp-monster-hca-prod-ebi-storage/broad_test_dataset"
}

# TODO change prints to logging


def setup_cli_logging_format() -> None:
    logging.basicConfig(level=logging.INFO, format='%(message)s', stream=sys.stdout)


def validate_input(csv_path: str):
    """
    input should be a manifest csv of those projects that need data copied back
    format is <institution>,<project_id>
    """
    if not os.path.isfile(csv_path):
        logging.debug(f"{csv_path} not found")
        sys.exit(1)

    if not csv_path.endswith('.csv'):
        logging.debug(f"{csv_path} is not a csv file")
        sys.exit(1)

    else:
        return csv_path


def find_project_id_in_str(s: str) -> str:
    """
    The selected function find_project_id_in_str(s: str) -> str:
    is used to extract a valid UUID (Universally Unique Identifier) from a given string s.
    :param s:
    :return:
    Attribution:
    https://github.com/DataBiosphere/hca-ingest/blob/main/orchestration/hca_orchestration/support/matchers.py
    """
    uuid_matcher = re.compile('[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}', re.I)
    project_ids = uuid_matcher.findall(s)

    if len(project_ids) != 1:
        raise Exception(f"Found more than one or zero project UUIDs in {s}")

    return str(project_ids[0])


def _sanitize_staging_gs_path(path: str) -> str:
    return path.strip().strip("/")


def _parse_csv(csv_path:str):
    """
    Parses the csv file and returns a list of staging areas
    :param csv_path:
    :return:
    Attribution:
    https://github.com/DataBiosphere/hca-ingest/blob/main/orchestration/hca_manage/manifest.py
    """
    staging_gs_paths = set()
    project_ids = set()
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                logging.debug("Empty path detected, skipping")
                continue

            assert len(row) == 2
            institution = row[0]
            project_id = find_project_id_in_str(row[1])

            project_ids.add(project_id)

            if institution not in STAGING_AREA_BUCKETS:
                raise Exception(f"Unknown institution {institution} found. "
                                f"Make sure the institution is in the list of staging area buckets and is in all caps")

            institution_bucket = STAGING_AREA_BUCKETS[institution]
            path = institution_bucket + "/" + project_id

            # sanitize and dedupe
            path = _sanitize_staging_gs_path(path)
            assert path.startswith("gs://"), "Staging area path must start with gs:// scheme"
            staging_gs_path = path

            staging_gs_paths.add(staging_gs_path)

        return staging_gs_paths, project_ids


def _get_target_snapshot_ids(project_ids: set[str]) -> set[str]:
    """
    This function gets the target snapshot name filters for the given project ids
    :param project_ids:
    :return:
    """
    target_snapshots = set()
    for project in project_ids:
        target_snapshot = f"hca_prod_{project.replace('-', '')}"
        target_snapshots.add(target_snapshot)
    return target_snapshots


# TODO: make this work in a Docker image
def get_access_token():
    creds, project = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    access_token = creds.token
    return access_token


def _get_latest_snapshots(target_snapshots: set[str], access_token: str):
    latest_snapshots = []
    for snapshot_name in target_snapshots:
        snapshot_response = requests.get(
            f'https://data.terra.bio/api/repository/v1/snapshots?offset=0&limit=10&sort=created_date&direction=desc&filter={snapshot_name}',
            headers={'accept': 'application/json', 'Authorization': f'Bearer {access_token}'}
        )
        snapshot_response.raise_for_status()
        latest_snapshot_id = snapshot_response.json()['items'][0]['id']
        latest_snapshots.append(latest_snapshot_id)
    return latest_snapshots


# for each snapshot get access url and add to a list of access urls for that snapshot
def get_access_urls(latest_snapshot_ids: list[str], access_token: str):
    for snapshot in latest_snapshot_ids:
        files_response = requests.get(
            f'https://data.terra.bio/api/repository/v1/snapshots/{snapshot}/files?offset=0',
            headers={'accept': 'application/json', 'Authorization': f'Bearer {access_token}'}
        )
        files_response.raise_for_status()

        # Extract file details from the JSON file and add them to a list
        list_of_access_urls = []
        data = files_response.json()
        for item in data:
            list_of_access_urls.append(item['fileDetail']['accessUrl'])
        return list_of_access_urls


def copy_tdr_to_staging(access_urls: list[str], staging_gs_paths: set[str]):
    for staging_dir in staging_gs_paths:
        staging_data_dir = staging_dir + '/data/'
        logging.info(f'staging_data_dir is {staging_data_dir}')
        # using gsutil as output is cleaner & faster
        output = subprocess.run(['gsutil', 'ls', staging_data_dir], capture_output=True)
        stdout = output.stdout.strip()
        files = stdout.decode('utf-8').split('\n')
        if len(files) > 1:
            logging.error(f"Staging area {staging_data_dir} is not empty")
            logging.info(f"files in staging area are: {files}")
            continue
    else:
        logging.info(f"Staging area {staging_data_dir} is empty - copying files now")
        for access_url in access_urls:
            try:
                # strip the filename from the access url because gcp is not a file system - it's all objects
                filename = access_url.split('/')[-1]
                print(f"Copying {access_url} to {staging_data_dir}{filename}")
                subprocess.run(['gcloud', 'storage', 'cp', access_url, staging_data_dir + filename])
            except Exception as e:
                logging.error(f"Error copying {access_url} to {staging_data_dir}{filename}: {e}")


def main():
    """Parse command-line arguments and run specified tool.

     Note: Does not take explicit input arguments, but uses sys.argv inputs
     from the command line.

    """
    setup_cli_logging_format()
    access_token = get_access_token()

    # read in the manifest and parse out the staging gs paths and project ids
    csv_path = sys.argv[1]
    validate_input(csv_path)
    staging_gs_paths = _parse_csv(csv_path)[0]
    logging.info(f"staging_gs_paths are {staging_gs_paths}")
    project_ids = _parse_csv(csv_path)[1]
    logging.info(f"project_ids are {project_ids}")

    # get the target snapshot ids, based on standard HCA ingest naming conventions
    target_snapshots = _get_target_snapshot_ids(project_ids)
    logging.info(f"target snapshot ids are {target_snapshots}")

    # get the latest snapshot ids for each target snapshot
    latest_snapshot_ids = _get_latest_snapshots(target_snapshots, access_token)
    logging.info(f"latest_snapshots_ids are {latest_snapshot_ids}")

    # get the access urls for each file in the snapshot
    access_urls = get_access_urls(latest_snapshot_ids, access_token)
    print(f"access_urls are {access_urls}")

    # copy the files from the TDR project bucket to the staging area bucket
    copy_tdr_to_staging(access_urls, staging_gs_paths)


if __name__ == '__main__':
    sys.exit(main())
