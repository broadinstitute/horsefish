# imports and environment variables
import json
import pytz
import csv
import requests
from argparse import ArgumentParser, Namespace
import logging
import time
import backoff
import httplib2
from datetime import datetime, timedelta
from typing import Union, Any
from oauth2client.client import GoogleCredentials
import sys

# DEVELOPER: update this field anytime you make a new docker image and update changelog
version = "1.0"

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

RP_TO_DATASET_ID = {
    "RP-2720": "dbfdcd34-2937-4781-96c2-5bf0c22fddec",
    "RP-2856": "d21a6291-3a5e-45c5-9ede-33b127142b79",
    "RP-3026": "667bf107-fb59-4649-803b-8e302630eef9",
    "RP-2065": "4aadfeb1-734d-4c72-ac9b-ac6d513d4d7f",
    "RP-2643": "550debf9-7df1-4049-ba9b-14573a6cd4dc",
}

KEYS_THAT_ARE_STRING = [
    "sample_id",
    "collaborator_participant_id",
    "collaborator_sample_id",
]
ONE_DAY_IN_SECONDS = 86400
MAX_BACKOFF_TIME = 600
MAX_RETRIES = 1
INGEST_CHECK_INTERVAL = 120


class Token:
    def __init__(self):
        self.cloud = 'gcp'
        self.expiry = None
        self.token_string = None
        self.credentials = GoogleCredentials.get_application_default()
        self.credentials = self.credentials.create_scoped(
            [
                "https://www.googleapis.com/auth/userinfo.profile",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/devstorage.full_control"
            ]
        )

    def get_gcp_token(self) -> str:
        # Refresh token if it has not been set or if it is expired or close to expiry
        if not self.token_string or not self.expiry or self.expiry < datetime.now(pytz.UTC) + timedelta(minutes=5):
            http = httplib2.Http()
            self.credentials.refresh(http)
            self.token_string = self.credentials.get_access_token().access_token
            # Set expiry to use UTC since google uses that timezone
            self.expiry = self.credentials.token_expiry.replace(tzinfo=pytz.UTC)
            # Convert expiry time to EST for logging
            est_expiry = self.expiry.astimezone(pytz.timezone('US/Eastern'))
            logging.info(f"New token expires at {est_expiry} EST")
        return self.token_string


class Terra:
    ACCEPTABLE_STATUS_CODES = [200, 202]
    TDR_LINK = "https://data.terra.bio/api/repository/v1"
    TERRA_LINK = "https://api.firecloud.org/api"
    LEONARDO_LINK = "https://leonardo.dsde-prod.broadinstitute.org/api"
    WORKSPACE_LINK = "https://workspace.dsde-prod.broadinstitute.org/api/workspaces/v1"
    GET = 'get'
    POST = 'post'

    def __init__(self, max_retries: int, max_backoff_time: int):
        self.max_retries = max_retries
        self.max_backoff_time = max_backoff_time
        self.token = Token()

    def _create_headers(self, content_type: str = None) -> dict:
        """Create headers for API calls."""
        headers = {"Authorization": f"Bearer {self.token.get_gcp_token()}", "accept": "application/json"}
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def _check_return_code(self, response: requests.Response) -> None:
        """Check if status code is acceptable."""
        if response.status_code not in self.ACCEPTABLE_STATUS_CODES:
            raise ValueError(
                f"Status code {response.status_code} is not acceptable: {response.text}")

    @staticmethod
    def _create_backoff_decorator(max_tries: int, factor: int, max_time: int) -> Any:
        """Create backoff decorator so we can pass in max_tries."""
        return backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=max_tries,
            factor=factor,
            max_time=max_time
        )

    def _run_request(self, uri: str, method: str, headers: dict, data: Any = None,
                     factor: int = 15) -> requests.Response:
        """Run request."""
        # Create a custom backoff decorator with the provided parameters
        backoff_decorator = self._create_backoff_decorator(
            max_tries=self.max_retries,
            factor=factor,
            max_time=self.max_backoff_time
        )

        # Apply the backoff decorator to the actual request execution
        @backoff_decorator
        def make_request() -> requests.Response:
            if method == self.GET:
                response = requests.get(uri, headers=headers)
            elif method == self.POST:
                response = requests.post(uri, headers=headers, data=data)
            else:
                raise ValueError(f"Method {method} is not supported")
            response.raise_for_status()  # Raise an exception for non-200 status codes
            return response
        return make_request()

    def get_job_result(self, job_id: str) -> dict:
        """retrieveJobResult"""
        uri = f"{self.TDR_LINK}/jobs/{job_id}/result"
        response = self._run_request(uri=uri, method=self.GET, headers=self._create_headers())
        return json.loads(response.text)

    def get_job_status(self, job_id: str) -> requests.Response:
        """retrieveJobStatus"""
        # first check job status - retrieveJob
        uri = f"{self.TDR_LINK}/jobs/{job_id}"
        response = self._run_request(uri=uri, method=self.GET, headers=self._create_headers())
        return response

    def ingest_dataset(self, dataset_id: str, data: json) -> dict:
        """Load data into TDR with ingestDataset."""
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/ingest"
        response = self._run_request(
            uri=uri,
            method=self.POST,
            headers=self._create_headers(content_type="application/json"),
            data=data
        )
        return json.loads(response.text)

    def _yield_data_set_metrics(self, dataset_id: str, target_table_name: str, query_limit: int = 1000) -> Any:
        """Yield all entity metrics from dataset."""
        search_request = {
            "offset": 0,
            "limit": query_limit,
            "sort": "datarepo_row_id"
        }
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/data/{target_table_name}"
        while True:
            batch_number = int((search_request["offset"] / query_limit)) + 1
            response = self._run_request(
                uri=uri,
                method=self.POST,
                headers=self._create_headers(content_type="application/json"),
                data=json.dumps(search_request)
            )
            if not response or not response.json()["result"]:
                break
            logging.info(
                f"Downloading batch {batch_number} of max {query_limit} records from {target_table_name} table in dataset {dataset_id}")
            for record in response.json()["result"]:
                yield record
            search_request["offset"] += query_limit

    def get_data_set_sample_ids(self, dataset_id: str, target_table_name: str, entity_id: str) -> list[str]:
        """Get existing ids from dataset."""
        data_set_metadata = self._yield_data_set_metrics(dataset_id=dataset_id, target_table_name=target_table_name)
        return [
            str(sample_dict[entity_id]) for sample_dict in data_set_metadata
        ]


class ReformatMetricsForIngest:
    def __init__(self, sample_metrics: list[dict]):
        self.sample_metrics = sample_metrics
        self.file_prefix = "gs://"

    def _format_relative_tdr_path(self, cloud_path: str) -> str:
        """Format cloud path to TDR path."""
        return '/' + '/'.join(cloud_path.split('/')[3:])

    def _check_and_format_file_path(self, column_value: str) -> Union[str, dict]:
        """Check if column value is a gs:// path and reformat to TDR's dataset relative path"""
        if isinstance(column_value, str):
            if column_value.startswith(self.file_prefix):
                return {
                    "sourcePath": column_value,
                    "targetPath": self._format_relative_tdr_path(column_value)
                }
        return column_value

    def _reformat_metric(self, row_dict: dict) -> dict:
        """Reformat metric for ingest."""
        reformatted_dict = {}
        for key, value in row_dict.items():
            # Ignore where there is no value
            if value:
                if key in KEYS_THAT_ARE_STRING:
                    reformatted_dict[key] = str(value)
                # If it is a list go through each item and recreate list
                elif isinstance(value, list):
                    reformatted_dict[key] = [
                        self._check_and_format_file_path(item) for item in value
                    ]
                else:
                    reformatted_dict[key] = self._check_and_format_file_path(value)
        # add in timestamp
        reformatted_dict['last_modified_date'] = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")
        return reformatted_dict

    def run(self) -> list[dict]:
        return [
            self._reformat_metric(row_dict) for row_dict in self.sample_metrics
        ]


class FilterOutSampleIdsAlreadyInDataset:
    def __init__(self, workspace_metrics: list[dict], dataset_id: str, terra: Terra, target_table_name: str, filter_entity_id: str):
        self.workspace_metrics = workspace_metrics
        self.terra = terra
        self.dataset_id = dataset_id
        self.target_table_name = target_table_name
        self.filter_entity_id = filter_entity_id

    def run(self) -> list[dict]:
        # Get all sample ids that already exist in dataset
        logging.info(f"Getting all sample ids that already exist in dataset {self.dataset_id}")

        data_set_sample_ids = self.terra.get_data_set_sample_ids(
            dataset_id=self.dataset_id,
            target_table_name=self.target_table_name,
            entity_id=self.filter_entity_id
        )
        # Filter out rows that already exist in dataset
        filtered_workspace_metrics = [
            row
            for row in self.workspace_metrics
            if str(row[filter_entity_id]) not in data_set_sample_ids
        ]
        logging.info("Checking this working")
        if len(filtered_workspace_metrics) < len(self.workspace_metrics):
            logging.info(f"Filtered out {len(self.workspace_metrics) - len(filtered_workspace_metrics)} rows that already exist in dataset")
            if filtered_workspace_metrics:
                return filtered_workspace_metrics
            else:
                logging.info("All rows filtered out as they all exist in dataset, nothing to ingest")
                sys.exit(0)
        else:
            logging.info("No rows were filtered out as they all do not exist in dataset")
            return filtered_workspace_metrics


class StartIngest:
    def __init__(self, terra: Terra, ingest_records: list[dict], target_table_name: str, dataset_id: str, load_tag: str, bulk_mode: bool, update_strategy: str):
        self.terra = terra
        self.ingest_records = ingest_records
        self.target_table_name = target_table_name
        self.dataset_id = dataset_id
        self.load_tag = load_tag
        self.bulk_mode = bulk_mode
        self.update_strategy = update_strategy

    def _create_ingest_dataset_request(self) -> Any:
        """Create the ingestDataset request body."""
        # https://support.terra.bio/hc/en-us/articles/23460453585819-How-to-ingest-and-update-TDR-data-with-APIs
        load_dict = {
            "format": "array",
            "records": self.ingest_records,
            "table": self.target_table_name,
            "resolve_existing_files": "true",
            "updateStrategy": self.update_strategy,
            "load_tag": self.load_tag,
            "bulkMode": "true" if self.bulk_mode else "false"
        }
        return json.dumps(load_dict)  # dict -> json

    def run(self) -> str:
        ingest_request = self._create_ingest_dataset_request()
        logging.info(f"Writing ingest request to {self.load_tag}.json")
        with open(f"{self.load_tag}.json", "w") as f:
            f.write(ingest_request)
        logging.info(f"Starting ingest to {self.dataset_id}")
        ingest_response = self.terra.ingest_dataset(
            dataset_id=self.dataset_id, data=ingest_request)
        return ingest_response["id"]


class MonitorIngest:
    def __init__(self, terra: Terra, ingest_id: str, check_interval: int):
        self.terra = terra
        self.ingest_id = ingest_id
        self.check_interval = check_interval

    def run(self) -> bool:
        """Monitor ingest until completion."""
        while True:
            ingest_response = self.terra.get_job_status(self.ingest_id)
            if ingest_response.status_code == 202:
                logging.info(f"Ingest {self.ingest_id} is still running")
                # Check every x seconds if ingest is still running
                time.sleep(self.check_interval)
            elif ingest_response.status_code == 200:
                response_json = json.loads(ingest_response.text)
                if response_json["job_status"] == "succeeded":
                    logging.info(f"Ingest {self.ingest_id} succeeded")
                    return True
                else:
                    logging.error(f"Ingest {self.ingest_id} failed")
                    job_result = self.terra.get_job_result(self.ingest_id)
                    raise ValueError(
                        f"Status code {ingest_response.status_code}: {response_json}\n{job_result}")
            else:
                logging.error(f"Ingest {self.ingest_id} failed")
                job_result = self.terra.get_job_result(self.ingest_id)
                raise ValueError(f"Status code {ingest_response.status_code}: {ingest_response.text}\n{job_result}")


def get_args() -> Namespace:
    parser = ArgumentParser(description='Move GCP data from Terra workspace to TDR.')
    parser.add_argument('-d', '--dataset_id', required=False, type=str,
                        help='id of TDR dataset for destination of outputs')
    parser.add_argument('-t', '--target_table_name', required=True, type=str,
                        help='name of target table in TDR dataset')
    parser.add_argument('--max_retries', default=MAX_RETRIES, type=int,
                        help=f'Max retries to have on all API calls. Defaults to {MAX_RETRIES}.')
    parser.add_argument('--max_backoff_time', default=MAX_BACKOFF_TIME, type=int,
                        help=f'Max backoff time for all API calls. Defaults to {MAX_BACKOFF_TIME}.')
    parser.add_argument('--waiting_time_to_poll', default=INGEST_CHECK_INTERVAL, type=int,
                        help=f'time to wait before polling ingest status again in seconds. Default is {INGEST_CHECK_INTERVAL}.')
    parser.add_argument('--batch_size', required=True, type=int, help="Ingest batch size is based on number of rows and not number of files")
    parser.add_argument('-u', '--update_strategy', required=True, choices=['replace', 'merge', 'append'], type=str,
                        help='which strategy to use for ingest')
    parser.add_argument('--bulk_mode', action="store_true",
                        help='use if you want to use bulkMode in ingest')
    parser.add_argument('--filter_entity_already_in_dataset', action="store_true",
                        help='use if you want to remove sample_ids already in dataset')
    parser.add_argument('--rp', help='research_project')
    parser.add_argument('--input_tsv', '-i', required=True, help='path to input tsv file')
    parser.add_argument('--filter_entity_id', action="store", default='sample_id',
                        help='use if you want to remove sample_ids already in dataset. Use if using filter_entity_already_in_dataset. default is sample_id')
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    # Initialize all args
    dataset_id, target_table_name = args.dataset_id, args.target_table_name
    batch_size, waiting_time_to_poll, filter_entity_id = args.batch_size, args.waiting_time_to_poll, args.filter_entity_id
    update_strategy, bulk_mode, rp = args.update_strategy, args.bulk_mode, args.rp
    filter_entity_already_in_dataset = args.filter_entity_already_in_dataset
    max_retries, max_backoff_time, input_tsv = args.max_retries, args.max_backoff_time, args.input_tsv

    # If dataset not provided used ones linked to RP
    if not dataset_id:
        dataset_id = RP_TO_DATASET_ID.get(rp)

    load_tag_prefix = f"{dataset_id}.{target_table_name}"

    # Read input tsv
    with open(input_tsv) as f:
        dict_reader = csv.DictReader(f, delimiter='\t')
        workspace_metrics = [row for row in dict_reader]
    logging.info(f"Read in {len(workspace_metrics)} rows from {input_tsv}")

    terra = Terra(
        max_retries=max_retries,
        max_backoff_time=max_backoff_time
    )

    if filter_entity_already_in_dataset:
        workspace_metrics = FilterOutSampleIdsAlreadyInDataset(
            workspace_metrics=workspace_metrics,
            dataset_id=dataset_id,
            terra=terra,
            target_table_name=target_table_name,
            filter_entity_id=filter_entity_id
        ).run()

    logging.info(f"Batching {len(workspace_metrics)} total rows into batches of {batch_size} for ingest")
    total_batches = len(workspace_metrics) // batch_size + 1
    for i in range(0, len(workspace_metrics), batch_size):
        batch_number = i // batch_size + 1
        logging.info(f"Starting ingest batch {batch_number} of {total_batches}")
        metrics_batch = workspace_metrics[i:i + batch_size]
        # Reformat metrics for ingest for paths
        reformatted_metrics = ReformatMetricsForIngest(
            sample_metrics=metrics_batch
        ).run()
        # Start actual ingest
        ingest_id = StartIngest(
            terra=terra,
            ingest_records=reformatted_metrics,
            target_table_name=target_table_name,
            dataset_id=dataset_id,
            load_tag=f"{load_tag_prefix}.batch_{batch_number}",
            bulk_mode=bulk_mode,
            update_strategy=update_strategy
        ).run()
        # monitor ingest until completion
        MonitorIngest(terra=terra, ingest_id=ingest_id,
                      check_interval=waiting_time_to_poll).run()
        logging.info(f"Completed batch ingest of {len(reformatted_metrics)} rows.")

