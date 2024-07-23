import os
import sys
import csv
import json
import requests
import subprocess
from google.auth import compute_engine
from google.auth.transport.requests import Request

STAGING_AREA_BUCKETS = {
    "prod": {
        "EBI": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
        "UCSC": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
        "LANTERN": "gs://broad-dsp-monster-hca-prod-lantern",
        "LATTICE": "gs://broad-dsp-monster-hca-prod-lattice/staging",
        "TEST": "gs://broad-dsp-monster-hca-prod-ebi-storage/broad_test_dataset"
    }
}

# input should be a manifest csv of those projects that need data copied back
# Check if n csv_path is provided as an argument
if len(sys.argv) != 2:
    print("Usage: python3 copy_from_tdr.py <csv_path>")
    sys.exit(1)

csv_path = sys.argv[1]

# Check if the file exists
if not os.path.isfile(csv_path):
    print(f"{csv_path} not found")
    sys.exit(1)

# Check if the file is a csv
if not csv_path.endswith('.csv'):
    print(f"{csv_path} is not a csv file")
    sys.exit(1)

def _parse_csv(csv_path:str):
    keys = set()
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                logging.debug("Empty path detected, skipping")
                continue

            assert len(row) == 2
            institution = row[0]
            project_id = find_project_id_in_str(row[1])

            key = None
            if project_id_only:
                project_id = row[1]
                key = project_id
            else:

                if institution not in STAGING_AREA_BUCKETS[env]:
                    raise Exception(f"Unknown institution {institution} found")

                institution_bucket = STAGING_AREA_BUCKETS[env][institution]
                path = institution_bucket + "/" + project_id

                # sanitize and dedupe
                path = _sanitize_gs_path(path)
                assert path.startswith("gs://"), "Staging area path must start with gs:// scheme"
                key = path

            if include_release_tag:
                key = key + f",{release_tag}"
            keys.add(key)

    chunked_paths = chunked(keys, MAX_STAGING_AREAS_PER_PARTITION_SET)
    return [chunk for chunk in chunked_paths]

# Read the file line by line
with open csv_path, 'r') as file:
    lines = file.read().splitlines()
    #

print(lines)

# TODO create the list of snapshot IDs from the list of UUIDs
# use manifest.csv to get the UUIDs that need data copied back
# for each UUID, construct the snapshot name - which will be latest snapshot with a dataset id like
# "hca_prod_<uuid-without-dashes>*"
# use those snapshot ids to get the latest snapshot id for each dataset
# this then becomes the snapshot list

snapshots = []
for line in lines:

# TODO - is this needed? or can we just run locally as Monster members?
# Get access token
# credentials = compute_engine.Credentials()
# credentials.refresh(Request())
# access_token = credentials.token

# for snapshot in lines:
#     # Make request to the API with the current snapshot
#     response = requests.get(f"https://data.terra.bio/api/repository/v1/snapshots/{snapshot}/files?offset=0&limit=10000",
#                             headers={'accept': 'application/json', 'Authorization': f'Bearer {access_token}'})
#
#     # Write the response to a JSON file
#     with open(f"response_{snapshot}.json", 'w') as outfile:
#         json.dump(response.json(), outfile)
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
