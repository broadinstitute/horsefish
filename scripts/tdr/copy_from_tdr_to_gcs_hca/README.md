# Copy from TDR to GCS
This was originally a bash script written by Samantha Velasquez\
[get_snapshot_files_and_transfer.sh](get_snapshot_files_and_transfer.sh) \
which was written to copy files from a TDR snapshot to an Azure bucket. \
Bobbie then translated to python using CoPilot and it ballooned from there. \
[copy_from_tdr_to_gcs.py](copy_from_tdr_to_gcs.py) \
The bash script is now just here for posterity as it previously only lived in Slack. 
It has not been tested in the Docker image created for the Python script.

## Running the Script
**IMPPORTANT**\
You will need to be in either the [Monster Group](https://groups.google.com/a/broadinstitute.org/g/monster) 
or the [Field Eng group](https://groups.google.com/a/broadinstitute.org/g/dsp-fieldeng) to run this script.

You will want to clone the whole horsefish repo, if you have not done so already.

You will also need a manifest file to run the script.\
The format of this manifest is identical to the one used for [HCA ingest](https://docs.google.com/document/d/1NQCDlvLgmkkveD4twX5KGv6SZUl8yaIBgz_l1EcrHdA/edit#heading=h.cg8d8o5kklql).
A sample manifest is provided in the project directory - dcpTEST_manifest.csv.\
(Note that this is a test manifest and you will have to first load the data into TDR to use it - see the HCA ingest Ops manual linked above).\
It's probably easiest to copy out the rows from the original ingest manifest into a new manifest, 
then move that file into this project directory, so that it is picked up by compose.

If you are not already logged in to gcloud/docker, you will need to do so before running the Docker compose command.\
`gcloud auth application-default login` \
`gcloud auth configure-docker us-east4-docker.pkg.dev`

To start up the run/dev Docker compose env \
`docker compose run app bash`\
This will pull the latest image from Artifact Registry, start up the container, and mount the project dir, 
so changes in your local project dir will be reflected in the container.

Next you will want to authenticate with gcloud using your Broad credentials.\
`gcloud auth login`\
`gcloud config set project dsp-fieldeng-dev`* \
`gcloud auth application-default login` \
If you are not in dsp-fieldeng-dev contact Field Eng to get access. \
Then run the script using the following command syntax:\
`python3 copy_from_tdr_to_gcs_hca.py <manifest_file> --env <env> --dry-run --allow-override'` \
If you are notified that there are files in the staging area (IE it is non-empty), reach out to the wranglers to \
determine if the files should be deleted or can be left in the staging area. \
Run the script again with the appropriate response to the prompt. \
Once you have the list of files (access_urls_filenames_sorted.txt, in your local project directory), \
verify that those are the files the wranglers want copied to GCS. \
If so, run the script again with the `--dry-run` flag removed. \
If you want to run without the file validation use the `--skip-integrity-check` flag.

Contact Field Eng for any issues that arise. \
_*or the monster hca prod project - mystical-slate-284720_

## Testing

The script includes a comprehensive pytest test suite that covers unit tests for individual functions and integration tests for main workflows.

### Running Tests

Install test dependencies:
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

