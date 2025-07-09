# Copy from TDR to GCS

This was originally a bash script written by Samantha Velasquez\
[scripts/get_snapshot_files_and_transfer.sh](scripts/get_snapshot_files_and_transfer.sh) \
which was written to copy files from a TDR snapshot to an Azure bucket. \
Bobbie then translated to python using CoPilot and it ballooned from there. \
[src/copy_from_tdr_to_gcs_hca.py](src/copy_from_tdr_to_gcs_hca.py) \
The bash script is now just here for posterity as it previously only lived in Slack. 
It has not been tested in the Docker image created for the Python script.

## Project Structure

```
copy_from_tdr_to_gcs_hca/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── .gitignore                   # Git ignore patterns
├── src/                         # Source code
│   ├── copy_from_tdr_to_gcs_hca.py
│   └── compare_files_in_tdr_to_files_in_staging.py
├── config/                      # Configuration files
│   ├── manifests/              # CSV manifest files - add yours here to run
│   │   └── dcpTEST_manifest.csv
│   └── docker/                 # Docker configuration
│       ├── Dockerfile
│       └── docker-compose.yaml
├── scripts/                     # Utility scripts
│   └── get_snapshot_files_and_transfer.sh
├── runs/                        # Run outputs (gitignored)
│   └── <timestamped-runs>/     # Individual run results
├── tests/                       # Test files
│   ├── test_copy_from_tdr_to_gcs_hca.py
│   ├── smoke_test.py
│   ├── pytest.ini
│   ├── run_tests.sh
│   └── test-requirements.txt
└── .docs/                        # context for agents
    └── STREAMING_OPTIMIZATION_CHANGELOG.md
```

## Running the Script

**IMPORTANT**\
You will need to be in either the [Monster Group](https://groups.google.com/a/broadinstitute.org/g/monster) 
or the [Field Eng group](https://groups.google.com/a/broadinstitute.org/g/dsp-fieldeng) to run this script.

You will want to clone the whole horsefish repo, if you have not done so already.

### Manifest Files

You will need a manifest file to run the script.\
The format of this manifest is identical to the one used for [HCA ingest](https://docs.google.com/document/d/1NQCDlvLgmkkveD4twX5KGv6SZUl8yaIBgz_l1EcrHdA/edit#heading=h.cg8d8o5kklql).
Sample manifests are provided in the `config/manifests/` directory.\
(Note that test manifests require you to first load the data into TDR to use them - see the HCA ingest Ops manual linked above).\
It's probably easiest to copy out the rows from the original ingest manifest into a new manifest, 
then save that file in the `config/manifests/` directory.

### Docker Setup

If you are not already logged in to gcloud/docker, you will need to do so before running the Docker compose command.\
`gcloud auth application-default login` \
`gcloud auth configure-docker us-east4-docker.pkg.dev`

To start up the run/dev Docker compose env \
`docker compose -f docker-compose.yaml run app bash`\
This will pull the latest image from Artifact Registry, start up the container, and mount the project dir, 
so changes in your local project dir will be reflected in the container.

### Authentication

Next you will want to authenticate with gcloud using your Broad credentials.\
`gcloud auth login`\
`gcloud config set project dsp-fieldeng-dev`* \
`gcloud auth application-default login` \
If you are not in dsp-fieldeng-dev contact Field Eng to get access.

### Running the Script

Then run the script using the following command syntax:\
`python3 src/copy_from_tdr_to_gcs_hca.py config/manifests/<manifest_file> --staging-env <staging_env> --dry-run --allow-override`

Where `<staging_env>` is either `prod` or `dev` (refers to staging bucket environment only - data is always sourced from production TDR).

All output files are automatically organized into timestamped directories in `runs/`.

#### File Comparison

If you are notified that there are files in the staging area (IE it is non-empty), reach out to the wranglers to \
determine if the files should be deleted or can be left in the staging area. \
It may be helpful to provide them with a diff of the files in TDR vs staging using: \
`python3 src/compare_files_in_tdr_to_files_in_staging.py runs/<run-dir>/<basename>_access_urls_filenames_sorted.txt runs/<run-dir>/<basename>_nonempty_staging_areas.txt`

Or prompt an agent with: \
"Compare these two files and tell me report back as to which files are not in both
for instance, SRR6373869_10hr_MissingLibrary_1_H7LFLBCXY_bamtofastq_S1_L002_R3_001.fastq.gz is in both files" & provide the access urls and nonempty (staging) files as input.

Run the script again with the appropriate response to the prompt. \
Once you have the list of files (`runs/<run-dir>/<basename>_all_access_urls_by_bucket.txt`), \
verify that those are the files the wranglers want copied to GCS. \
If so, run the script again with the `--dry-run` flag removed. \
If you want to run without the file validation use the `--skip-integrity-check` flag.

Contact Field Eng for any issues that arise. \
_*or the monster hca prod project - mystical-slate-284720_

## Output Files

The script generates timestamped output files based on the input CSV manifest filename. If the manifest is named `HCADevRefresh_May2025_manifest.csv`, all output files will have the prefix `HCADevRefresh_May2025-MMDDYY-HHMM` (e.g., `HCADevRefresh_May2025-062425-1400`).

### Generated Files

- **`{basename}_copy_tdr_to_gcs_hca.log`** - Complete log of all operations
- **`{basename}_failed_access_urls.txt`** - List of URLs that failed to copy (if any failures occur)
- **`{basename}_integrity_verification_failed.txt`** - List of files that failed integrity verification (if any failures occur)
- **`{basename}_all_access_urls_by_bucket.txt`** - All access URLs sorted and grouped by bucket
- **`{basename}_nonempty_staging_areas.txt`** - Report of staging areas that were not empty (if any found)
- **`{basename}_access_urls.txt`** - Raw access URLs from TDR API
- **`{basename}_access_urls_filenames_sorted.txt`** - Sorted list of filenames to be copied

## Testing

The script includes a comprehensive pytest test suite that covers unit tests for individual functions and integration tests for main workflows.

### Running Tests

From the /tests dir - install test dependencies:
```bash
pip install -r test-requirements.txt
```

Run all tests:
```bash
pytest
```

Run tests with coverage:
```bash
pytest --cov=copy_from_tdr_to_gcs_hca --cov-report=html
```

Run specific test categories:
```bash
# Unit tests only
pytest -m unit

# Integration tests only  
pytest -m integration

# Run tests in parallel
pytest -n auto
```

### Test Structure

- `test_copy_from_tdr_to_gcs_hca.py` - Main test file containing:
  - Unit tests for individual functions
  - Integration tests for workflows
  - Mock-based testing for external dependencies
  - Edge case and error condition testing

The tests mock external dependencies like:
- Google Cloud authentication
- HTTP requests to Terra Data Repository
- Subprocess calls (gsutil, gcloud)
- File system operations

## Building the Docker Image
The image builds with the GitHub Action "Main Validation and Release" `../.github/workflows/build-and-push_docker_copy_from_tdr_to_gcs_hca_main.yaml` 
and `../.github/workflows/build-and-push_docker_copy_from_tdr_to_gcs_hca_dev.yaml` \
tags = `us-east4-docker.pkg.dev/$GCP_PROJECT_ID/$GCP_REPOSITORY/copy_from_tdr_to_gcs_hca:$GITHUB_SHA`, 
`us-east4-docker.pkg.dev/$GCP_PROJECT_ID/$GCP_REPOSITORY/copy_from_tdr_to_gcs_hca:latest`

### To manually build and run locally
`docker build -t us-east4-docker.pkg.dev/dsp-fieldeng-dev/horsefish/copy_from_tdr_to_gcs_hca:<new_version> .` \
`docker run --rm -it us-east4-docker.pkg.dev/dsp-fieldeng-dev/horsefish/copy_from_tdr_to_gcs_hca:<new_version>`

### To build and push to Artifact Registry
- make sure you are logged in to gcloud and that application default credentials are set \
`gcloud auth login` \
`gcloud config set project dsp-fieldeng-dev` \
`gcloud auth application-default login`
- set the <new_version> before building and pushing \
`docker push us-east4-docker.pkg.dev/dsp-fieldeng-dev/horsefish/copy_from_tdr_to_gcs_hca:<new_version>`


## Possible improvements*
- update the script with conditional logic to accept a snapshot ID and destination instead
- update the script check lower case institution against lower case institution keys - see ~line 86
- Might want to be able to specify the file type to copy or to exclude.
- update with retry logic, if that becomes an issue
- add config yaml rather than setting batch sizes and base_urls...etc in the script itself
- add a progress bar
- if we run this a bunch, a summary report might be nice, rather than individual files.

*this is likely to be used only rarely and mostly by the author, as a stop gap until partial updates have been implemented.
As such, we are attempting to keep this as light as possible, so as not to introduce unnecessary complexity.

