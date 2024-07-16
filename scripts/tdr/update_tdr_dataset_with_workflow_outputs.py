"""This script identifies Terra workflow outputs to be returned to TDR and performs that ingest.
"""

import argparse
import json

from firecloud import api as fapi
from google.cloud import storage as gcs
from pprint import pprint
from tqdm import tqdm

from soft_delete_rows import soft_delete_rows
from tdr_utils import get_all_attributes_by_sample_id, get_single_attribute, get_fq_table, \
    call_ingest_dataset, recode_json_with_filepaths
from terra_utils import get_workspace_bucket
from utils import open_config_file, write_file_to_bucket, get_current_timestamp_str


def write_load_json_to_bucket(bucket, recoded_rows_json, timestamp):
    """Write the list of recoded rows (list of dictionaries) to a file and copy to workspace bucket."""

    loading_json_filename = f"{timestamp}_recoded_ingestDataset.json"

    # write load json to the workspace bucket
    with open(loading_json_filename, 'w') as final_newline_json:
        for sample_json in recoded_rows_json:
            for key, value in sample_json.items():
                if value == "None":
                    sample_json[key] = None
            final_newline_json.write(json.dumps(sample_json))
            final_newline_json.write("\n")

    return write_file_to_bucket(loading_json_filename, bucket)


def extract_metrics_data(metrics_file_path):
    parent_bucket_full = '/'.join(metrics_file_path.split('/')[:3])
    parent_bucket = parent_bucket_full.replace('gs://', '')
    blob_path = metrics_file_path.replace(parent_bucket_full + '/', '')

    storage_client = gcs.Client()
    bucket = storage_client.get_bucket(parent_bucket)

    blob = bucket.blob(blob_path)

    local_metrics_file_name = 'metrics.txt'
    blob.download_to_filename(local_metrics_file_name)

    metrics_data = {}
    with open(local_metrics_file_name, 'r') as infile:
        for row in infile:
            key, value = row.rstrip('\n').split('\t')
            metrics_data[key] = value

    return metrics_data


def get_update_json_for_samples(new_data_to_ingest_json, new_version_timestamp, sample_id_to_datarepo_row_id, dataset_sample_table_fq, gcp_bq_project):
    # get lists of sample_ids and datarepo_row_ids
    sample_ids = list(sample_id_to_datarepo_row_id.keys())
    # datarepo_row_ids = list(sample_id_to_datarepo_row_id.values())

    # get a list of dicts containing all sample attributes
    # previous_data_to_update_list = get_all_attributes(dataset_sample_table_fq, gcp_bq_project, datarepo_row_ids, fmt='json')
    previous_data_to_update_list = get_all_attributes_by_sample_id(dataset_sample_table_fq, gcp_bq_project, sample_ids, fmt='json')

    sample_json_for_ingest = []

    # convert recoded_ingest_json list to a dict with key = sample_id
    recoded_json_for_ingest = {}
    for recoded_json in new_data_to_ingest_json:
        recoded_json_for_ingest[recoded_json['sample_id']] = recoded_json

    # populate list of old rows that can be soft deleted
    old_row_ids = []
    for previous_sample_data in previous_data_to_update_list:
        sample_id = previous_sample_data['sample_id']
        recoded_json = recoded_json_for_ingest[sample_id]

        # add in the updated fields (workflow outputs and submission info)
        previous_sample_data.update(recoded_json)
        previous_sample_data['version_timestamp'] = new_version_timestamp

        # remove 'datarepo_row_id' field
        old_datarepo_row_id = previous_sample_data.pop('datarepo_row_id')

        sample_json_for_ingest.append(previous_sample_data)
        old_row_ids.append(old_datarepo_row_id)

    return sample_json_for_ingest, old_row_ids


def update_tdr_with_workflow_outputs(config_file_path, dataset_id, submission_id, ws_name, ws_project, gcp_bq_project, verbose=True):
    # get the configuration for this workflow's outputs for ingest to TDR
    workflow_outputs_config = open_config_file(config_file_path)

    workflow_name = workflow_outputs_config['workflowName']
    workflow_outputs_dict = workflow_outputs_config['workflowOutputsMap']
    metrics_file_output_name = workflow_outputs_config.get('metricsFileOutputName', None)

    # extract information from the submission whose outputs you're importing back to TDR
    sub_info = fapi.get_submission(ws_project, ws_name, submission_id).json()
    snapshot_id = sub_info['externalEntityInfo']['dataStoreId']

    # find all successful workflows
    # NOTE: this assumes you're going to have exactly one successful workflow per sample, so...
    # TODO: build in some edge case handling here
    workflows = {}  # format will be {datarepo_row_id: workflow_id}
    for workflow in sub_info['workflows']:
        if workflow['status'] != 'Succeeded':
            continue
        datarepo_row_id = workflow['workflowEntity']['entityName']
        workflows[datarepo_row_id] = workflow['workflowId']

    ## get the new data associated with these workflow outputs that needs to be uploaded back to TDR

    new_rows_to_upload = []
    sample_id_to_datarepo_row_id = {}

    snapshot_sample_table_fq = get_fq_table(snapshot_id, 'sample', 'snapshot')

    for datarepo_row_id, workflow_id in tqdm(workflows.items()):
        # retrieve sample_id from snapshot data
        sample_id = get_single_attribute(snapshot_sample_table_fq, gcp_bq_project, datarepo_row_id, 'sample_id')
        sample_data_to_upload = {'sample_id': sample_id}
        sample_id_to_datarepo_row_id[sample_id] = datarepo_row_id

        # get workflow outputs
        workflow_outputs_json = fapi.get_workflow_outputs(ws_project, ws_name, submission_id, workflow_id).json()
        workflow_outputs = workflow_outputs_json['tasks'][workflow_name]['outputs']

        # pull out all the desired workflow outputs (defined in workflow_outputs_dict) and save them in data_to_upload
        for output_name, output_value in workflow_outputs.items():
            if output_name in workflow_outputs_dict.keys():
                sample_data_to_upload[workflow_outputs_dict[output_name]] = output_value

        # TODO add optional submission and workflow id assignment to config
        # sample_data_to_upload["submission_id"] = submission_id
        # sample_data_to_upload["workflow_id"] = workflow_id

        # extract metrics file output
        if metrics_file_output_name is not None:
            metrics_file_path = workflow_outputs[f"{workflow_name}.{metrics_file_output_name}"]
            metrics_file_outputs = extract_metrics_data(metrics_file_path)
            sample_data_to_upload.update(metrics_file_outputs)

        new_rows_to_upload.append(sample_data_to_upload)

    if verbose:
        pprint(new_rows_to_upload)

    target_table_name = "sample"

    ## recode the data to the required source/target path formulation for file ingests
    recoded_ingest_json = recode_json_with_filepaths(new_rows_to_upload)

    # update version_timestamp batch-wide
    new_version_timestamp = get_current_timestamp_str()
    dataset_sample_table_fq = get_fq_table(dataset_id, target_table_name, 'dataset')

    # get the existing data from TDR for each row so we ingest a complete row
    sample_json_to_update, old_row_ids = get_update_json_for_samples(recoded_ingest_json, new_version_timestamp, sample_id_to_datarepo_row_id, dataset_sample_table_fq, gcp_bq_project)
    pprint(old_row_ids)


    # generate load json
    ws_bucket = get_workspace_bucket(ws_project, ws_name)
    control_file_path = write_load_json_to_bucket(ws_bucket, sample_json_to_update, new_version_timestamp)

    # ingest new data - this waits for success
    response = call_ingest_dataset(control_file_path, target_table_name, dataset_id)

    # soft delete old rows
    soft_delete_rows(old_row_ids, target_table_name, dataset_id, ws_bucket)

    print('done!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')

    parser.add_argument('--config_file_path', '-c', type=str, help='file path to config defining workflow outputs to be ingested to TDR')
    parser.add_argument('--dataset_id', '-d', type=str, help='TDR dataset id (uuid) where data should be ingested')
    parser.add_argument('--submission_id', '-s', type=str, help='Terra submission id containing workflow outputs to ingest to TDR')
    parser.add_argument('--workspace_name', '-w', type=str, help='source Terra workspace name')
    parser.add_argument('--workspace_project', '-p', type=str, help='source Terra project name')
    parser.add_argument('--gcp_bq_project', '-g', type=str, default='dsp-ops-gp-development', help='GCP project to use to query BQ')

    parser.add_argument('--verbose', '-v', action='store_true', help='print progress text')

    args = parser.parse_args()

    update_tdr_with_workflow_outputs(args.config_file_path, args.dataset_id, args.submission_id,
                            args.workspace_name, args.workspace_project,
                            args.gcp_bq_project, args.verbose)