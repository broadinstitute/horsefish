from argparse import ArgumentParser, Namespace
import google.auth
import logging
from typing import Any
from google.cloud import bigquery
from pathlib import Path


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


class GetSampleInfo:
    def __init__(self, google_project: str):
        credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.google_project = google_project
        self.bqclient = bigquery.Client(credentials=credentials, project=google_project)

    def query_for_batch_job_info(self) -> Any:
        """Gets all batch job info from bigquery"""
        query_string = f"""SELECT a.job_id, a.input_path, s.status, s.timestamp
FROM `{self.google_project}.dragen_illumina.job_array` as a
join `{self.google_project}.dragen_illumina.tasks_status` as s on a.job_id = s.job_id"""

        logging.info(f"Querying for jobs status")

        # API request - start query, pass in extra configuration
        query_job = self.bqclient.query(query_string)

        # wait for the job to complete
        return query_job.result()

    def create_sample_dict(self, row: dict, sample_name: str) -> dict:
        """Create an initial sample dict"""
        return {
            'sample_name': sample_name,
            'latest_status': row['status'],
            'latest_timestamp': row['timestamp'],
            # Start a set for job ids
            'job_ids': {row['job_id']}
        }

    def create_full_samples_dicts(self, query_results: Any) -> dict:
        """Creates full dictionary where key is sample name and value is dict with all jobs info"""
        samples_dict = {}
        for row in query_results:
            # Get sample name from cram input
            sample_name = Path(row['input_path']).stem
            if sample_name not in samples_dict:
                # Create initial dict
                samples_dict[sample_name] = self.create_sample_dict(row, sample_name)
            else:
                sample_dict = samples_dict[sample_name]
                # Add to job ids set
                sample_dict['job_ids'].add(row['job_id'])
                # If entry has latest timestamp use this status
                if row['timestamp'] > sample_dict['latest_timestamp']:
                    sample_dict['latest_timestamp'] = row['timestamp']
                    sample_dict['latest_status'] = row['status']
        return samples_dict

    def run(self) -> dict:
        query_results = self.query_for_batch_job_info()
        return self.create_full_samples_dicts(query_results)


def get_args() -> Namespace:
    parser = ArgumentParser(description='Query BQ table with GCS bucket object metadata inventory.')

    parser.add_argument('-g', '--gcp_project', type=str, help='Google project used for BigQuery.',
                        choices=['gp-cloud-dragen-dev', 'gp-cloud-dragen-prod'], required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    gcp_project = args.gcp_project

    samples_dict = GetSampleInfo(google_project=gcp_project).run()
    # TODO: Once we figure out what tables will look like the next step is create new samples tsv and uploading to workspace
