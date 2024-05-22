import os
import sys
import json
import requests
import subprocess
from google.auth import compute_engine
from google.auth.transport.requests import Request

# input should be a manifest csv of those projects that need data copied back
# Check if a filename is provided as an argument
if len(sys.argv) != 2:
    print("Usage: python3 script.py <filename>")
    sys.exit(1)

filename = sys.argv[1]

# Check if the file exists
if not os.path.isfile(filename):
    print(f"File {filename} not found")
    sys.exit(1)

# Read the file line by line
with open(filename, 'r') as file:
    lines = file.read().splitlines()

# TODO create the list of snapshot IDs from the list of UUIDs
# use manifest.csv to get the UUIDs that need data copied back
# for each UUID, construct the snapshot name - which will be latest snapshot with a dataset id like
# "hca_prod_<uuid-without-dashes>*"
# use those dataset ids to get the latest snapshot id for each dataset
# this then becomes the lines list

# TODO - is this needed? or can we just run locally as Monster members?
# Get access token
credentials = compute_engine.Credentials()
credentials.refresh(Request())
access_token = credentials.token

for snapshot in lines:
    # Make request to the API with the current snapshot
    response = requests.get(f"https://data.terra.bio/api/repository/v1/snapshots/{snapshot}/files?offset=0&limit=10000",
                            headers={'accept': 'application/json', 'Authorization': f'Bearer {access_token}'})

    # Write the response to a JSON file
    with open(f"response_{snapshot}.json", 'w') as outfile:
        json.dump(response.json(), outfile)

    # Extract file details from the JSON file and append them to a text file
    with open(f"response_{snapshot}.json", 'r') as json_file:
        data = json.load(json_file)
        with open("list_of_access_urls.txt", 'a') as outfile:
            for item in data:
                outfile.write(item['fileDetail']['accessUrl'] + '\n')


# Read the list of files from list_of_filepaths.txt and copy them using gcloud storage cp
with open("list_of_access_urls.txt", 'r') as file:
    access_urls = file.read().splitlines()

# TODO
# copy command will look something like\
# gcloud storage cp gs://datarepo-4bcb4408-bucket/2e2aac27-3bf5-4a89-b466-e563cf99aef2/07a78be1-c75f-4463-a1a4-d4f7f9771ca5/SRR3562314_2.fastq.gz gs://broad-dsp-monster-hca-prod-ebi-storage/broad_test_dataset/07e5ebc0-1386-4a33-8ce4-3007705adad8/data/.
# Also need to construct the staging/data gs:// path from the manifest.csv
# "EBI": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
# "UCSC": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
# "LANTERN": "gs://broad-dsp-monster-hca-prod-lantern",
#  "LATTICE": "gs://broad-dsp-monster-hca-prod-lattice/staging",
for access_url in access_urls:
    subprocess.run(['gcloud storage', 'cp', access_url, "<INSERT STAGING /DATA url>"])