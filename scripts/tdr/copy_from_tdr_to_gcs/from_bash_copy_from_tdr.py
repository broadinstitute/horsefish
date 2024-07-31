import os
import sys
import csv
import logging
import json
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

def validate_input(csv_path:str):
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
    is used to extract a UUID (Universally Unique Identifier) from a given string s.
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

def _sanitize_gs_path(path: str) -> str:
    return path.strip().strip("/")

def _parse_csv(csv_path:str):
    """
    Parses the csv file and returns a list of staging areas
    :param csv_path:
    :return:
    Attribution:
    https://github.com/DataBiosphere/hca-ingest/blob/main/orchestration/hca_manage/manifest.py
    """
    gs_paths = set()
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

            gs_path = None
            if institution not in STAGING_AREA_BUCKETS:
                raise Exception(f"Unknown institution {institution} found")

            institution_bucket = STAGING_AREA_BUCKETS[institution]
            path = institution_bucket + "/" + project_id

            # sanitize and dedupe
            path = _sanitize_gs_path(path)
            assert path.startswith("gs://"), "Staging area path must start with gs:// scheme"
            gs_path = path

            gs_paths.add(gs_path)

        # print(f"These are the parsed gs_paths {gs_paths}")
        # print(f"These are the parsed project_ids {project_ids}")
        return gs_paths, project_ids


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


def _get_latest_snapshot(target_snapshots: set[str], access_token: str):
    for snapshot_name in target_snapshots:
        response = requests.get(f'https://data.terra.bio/api/repository/v1/snapshots?sort=createdDate,'
                                f'desc&limit=1&filter={snapshot_name}',
                                headers={'accept': 'application/json', 'Authorization': f'Bearer {access_token}'})
        response.raise_for_status()
        with open(f"response_{snapshot_name}.json", 'w') as outfile:
            json.dump(response.json(), outfile)

# TODO 8/1/24 - got a 400 for invalid filter - yay!
# need to add note that you need to gcloud auth to run this.
#
#     # Extract file details from the JSON file and append them to a text file
#     with open(f"response_{snapshot}.json", 'r') as json_file:
#         data = json.load(json_file)
#         with open("list_of_access_urls.txt", 'a') as outfile:
#             for item in data:
#                 outfile.write(item['fileDetail']['accessUrl'] + '\n')
#
#
# # Read the list of files from list_of_filepaths.txt and copy them using gcloud storage cp
# with open("list_of_access_urls.txt", 'r') as file:
#     access_urls = file.read().splitlines()

# # TODO
# # copy command will look something like\
# # gcloud storage cp gs://datarepo-4bcb4408-bucket/2e2aac27-3bf5-4a89-b466-e563cf99aef2/07a78be1-c75f-4463-a1a4-d4f7f9771ca5/SRR3562314_2.fastq.gz gs://broad-dsp-monster-hca-prod-ebi-storage/broad_test_dataset/07e5ebc0-1386-4a33-8ce4-3007705adad8/data/.
# # Also need to construct the staging/data gs:// path from the manifest.csv
# # "EBI": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
# # "UCSC": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
# # "LANTERN": "gs://broad-dsp-monster-hca-prod-lantern",
# #  "LATTICE": "gs://broad-dsp-monster-hca-prod-lattice/staging",
# for access_url in access_urls:
#     subprocess.run(['gcloud storage', 'cp', access_url, "<INSERT STAGING /DATA url>"])


def main():
    """Parse command-line arguments and run specified tool.

     Note: Does not take explicit input arguments, but uses sys.argv inputs
     from the command line.

    """
    setup_cli_logging_format()
    access_token = get_access_token()
    csv_path = sys.argv[1]
    validate_input(csv_path)
    gs_paths = _parse_csv(csv_path)[0]
    print(f"gs_paths are {gs_paths}")
    project_ids = _parse_csv(csv_path)[1]
    print(f"project_ids are {project_ids}")
    target_snapshots = _get_target_snapshot_ids(project_ids)
    print(f"target snapshot ids are {target_snapshots}")
    tdr_data_path = _get_latest_snapshot(target_snapshots, access_token)


if __name__ == '__main__':
    sys.exit(main())
