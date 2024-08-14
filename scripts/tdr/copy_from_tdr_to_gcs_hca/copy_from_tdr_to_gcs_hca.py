# Description: This script copies data from Terra Data Repository (TDR) HCA project buckets to HCA staging area buckets.
# It is based on the bash script get_snapshot_files_and_transfer.sh, written by Samantha Velasquez.

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
    tuple_list = []
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                logging.debug("Empty path detected, skipping")
                continue

            assert len(row) == 2
            institution = row[0]
            project_id = find_project_id_in_str(row[1])

            if institution not in STAGING_AREA_BUCKETS:
                raise Exception(f"Unknown institution {institution} found. "
                                f"Make sure the institution is in the list of staging area buckets and is in all caps")

            institution_bucket = STAGING_AREA_BUCKETS[institution]
            path = institution_bucket + "/" + project_id

            # sanitize and dedupe
            path = _sanitize_staging_gs_path(path)
            assert path.startswith("gs://"), "Staging area path must start with gs:// scheme"
            staging_gs_path = path

            tuple_list.append((staging_gs_path, project_id))

        return tuple_list


def get_access_token():
    creds, project = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    access_token = creds.token
    return access_token


def get_latest_snapshot(target_snapshot: str, access_token: str):
    snapshot_response = requests.get(
        f'https://data.terra.bio/api/repository/v1/snapshots?offset=0&limit=10&sort=created_date&direction=desc&filter='
        f'{target_snapshot}',
        headers={'accept': 'application/json', 'Authorization': f'Bearer {access_token}'}
    )
    snapshot_response.raise_for_status()
    latest_snapshot_id = snapshot_response.json()['items'][0]['id']
    return latest_snapshot_id


# for each snapshot get access url and add to a list of access urls for that snapshot
def get_access_urls(snapshot: str, access_token: str):
    list_of_access_urls = []
    # for snapshot in latest_snapshot_ids:
    logging.info(f"getting access urls for snapshot {snapshot}")
    files_response = requests.get(
        f'https://data.terra.bio/api/repository/v1/snapshots/{snapshot}/files?offset=0',
        headers={'accept': 'application/json', 'Authorization': f'Bearer {access_token}'}
    )
    files_response.raise_for_status()

    # Extract file details from the JSON file and add them to a list
    data = files_response.json()
    for item in data:
        list_of_access_urls.append(item['fileDetail']['accessUrl'])
    return list_of_access_urls


def check_staging_is_empty(staging_gs_paths: set[str]):
    nonempty_staging_areas = []
    for staging_dir in staging_gs_paths:
        staging_data_dir = staging_dir + '/data/'
        logging.info(f'checking contents of staging_data dir: {staging_data_dir}')
        # using gsutil as output is cleaner & faster
        output = subprocess.run(['gsutil', 'ls', staging_data_dir], capture_output=True)
        stdout = output.stdout.strip()
        files = stdout.decode('utf-8').split('\n')
        if len(files) > 1:
            logging.error(f"Staging area {staging_data_dir} is not empty")
            logging.info(f"files in staging area are: {files}")
            nonempty_staging_areas.append(staging_data_dir)
        else:
            logging.info(f"Staging area {staging_data_dir} is empty")

    if len(nonempty_staging_areas) > 0:
        logging.error("One or more staging areas are not empty. Exiting.")
        logging.info(f"Non-empty staging areas are: {nonempty_staging_areas}")
        sys.exit(1)


def copy_tdr_to_staging(tuple_list: list[tuple[str, str]], access_token: str):
    for project_id in set([x[1] for x in tuple_list]):
        target_snapshot = f"hca_prod_{project_id.replace('-', '')}"
        latest_snapshot_id = get_latest_snapshot(target_snapshot, access_token)
        logging.info(f'latest snapshot id for project {project_id} is {latest_snapshot_id}')
        access_urls = get_access_urls(latest_snapshot_id, access_token)
        num_access_urls = len(access_urls)
        staging_gs_path = [x[0] for x in tuple_list if x[1] == project_id][0]
        staging_data_dir = staging_gs_path + '/data/'
        logging.info(f'Copying {num_access_urls} files from snapshot {latest_snapshot_id} to staging area {staging_data_dir}')
        for access_url in access_urls:
            # strip the filename from the access url because gcp is not a file system - it's all objects
            filename = access_url.split('/')[-1]
            logging.info(f'access_url for snapshot {latest_snapshot_id} is {access_url}')
            try:
                subprocess.run(['gcloud', 'storage', 'cp', access_url, staging_data_dir + filename])
            except Exception as e:
                logging.error(f'Error copying {access_url} to {staging_gs_path}{filename}: {e}')
                continue
        # visual summary of number of files copied
        files_copied = subprocess.run(['gsutil', 'ls', staging_data_dir],
                                      capture_output=True).stdout.decode('utf-8').split('\n')
        # gsutil outputs the dir and a blank line, so we need to remove the blank line and the dir to count files
        files_in_dir = [x.split('/')[-1] for x in files_copied if x and x.split('/')[-1]]
        number_files_copied = len(files_in_dir)
        logging.info(f'{number_files_copied} out of {num_access_urls} files copied to {staging_data_dir}')


def main():
    """Parse command-line arguments and run specified tool.

     Note: Does not take explicit input arguments, but uses sys.argv inputs
     from the command line.

    """
    setup_cli_logging_format()
    access_token = get_access_token()

    # read in the manifest and get a tuple list of staging gs paths and project ids
    csv_path = sys.argv[1]
    validate_input(csv_path)
    tuple_list = _parse_csv(csv_path)
    logging.info(f"staging_gs_paths and project id tuple list is {tuple_list}")

    # staging dir is the first element in each tuple
    staging_gs_paths = set([x[0] for x in tuple_list])

    # check if the staging area is empty
    check_staging_is_empty(staging_gs_paths)
    # copy the files from the TDR project bucket to the staging area bucket
    copy_tdr_to_staging(tuple_list, access_token)


if __name__ == '__main__':
    sys.exit(main())
