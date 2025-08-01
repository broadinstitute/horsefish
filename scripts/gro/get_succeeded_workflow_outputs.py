import argparse
import json
import requests

from firecloud import api as fapi
from oauth2client.client import GoogleCredentials
import time


def write_outputs_to_file(outputs_list):
    """Write list of outputs to file."""
    
    print(f"Starting write of all outputs to output.txt.")
    # format list to write to file
    # if item in overall list of outputs is a list, unnest it and hold in separate list
    unnested_array_items = [output for output_item in outputs_list if isinstance(output_item,list) for output in output_item]
    
    # if item in overall list of outputs is not a list, hold in separate list
    non_array_items = [output_item for output_item in outputs_list if not isinstance(output_item,list)]

    # concatenated both flattened lists together
    final_outputs_list = unnested_array_items + non_array_items

    with open('outputs.txt','w') as outfile:
	    outfile.write('\n'.join(final_outputs_list))


def get_workflow_outputs(ws_project, ws_name, submission_id, workflow_id):
    """Get outputs for a single workflow"""

    response = fapi.get_workflow_outputs(ws_project, ws_name, submission_id, workflow_id)
    status_code = response.status_code

    # return empty dictionary if not able to get workflow outputs
    if status_code != 200:
        return response.text, False
    
    return response.json(), True



def get_all_outputs(ws_project, ws_name, workflows_by_submission):
    """Get list of workflow outputs."""

    print(f"Starting extraction of outputs for successfully completed workflows.")
    # capture all outputs in wf level outputs
    all_outputs_list = []

    # {submission_id: [workflow_ids]}
    for submission_id, workflows in workflows_by_submission.items():
        for workflow_id in workflows:
            # get workflow's output metadata
            workflow_metadata, workflow_metadata_exists = get_workflow_outputs(ws_project, ws_name, submission_id, workflow_id)
           
            # if there is any workflow metadata - metadata for workflows over a year old is deleted
            if workflow_metadata_exists:

                # get list of keys ("task") that have an outputs section in returned workflow metadata
                all_tasks = list(workflow_metadata["tasks"].keys())
                tasks_with_outputs = [task for task in all_tasks if "outputs" in workflow_metadata["tasks"][task].keys()]

                for task in tasks_with_outputs:
                    # get workflow level outputs
                    workflow_outputs = workflow_metadata["tasks"][task]["outputs"]
                    
                    for wf_output_name, wf_output_value in workflow_outputs.items():
                        all_outputs_list.append(wf_output_value)

    return all_outputs_list


def get_succeeded_workflows(ws_project, ws_name, submissions_json):
    """Get list of all submission ids containing Succeeded workflows and successful workflow ids."""

    print("Starting extraction of succeeded only workflows in each submission.")
    # init list to collect succeeded wf ids 
    wfs_by_submission = {}

    for sub in submissions_json:
        submission_id = sub["submissionId"]
        # get list of workflow statuses in single submission
        sub_wf_statuses = list(sub["workflowStatuses"].keys())

        # if sub. has succeeded workflows, get workflow ids
        if "Succeeded" in sub_wf_statuses:
            # get all workflows in successful submission
            all_workflows = fapi.get_submission(ws_project, ws_name, submission_id).json()["workflows"]

            # get list of successful workflow ids for submission
            successful_workflows = []
            for wf in all_workflows:
                if wf["status"] == "Succeeded":
                    successful_workflows.append(wf["workflowId"])
            
            wfs_by_submission[submission_id] = successful_workflows
                    
    # if no successful workflows in workspace
    if not wfs_by_submission:
        raise ValueError("No successful workflows across all submissions in this workspace.")
    
    return wfs_by_submission


def get_workspace_submissions(ws_project, ws_name):
    """Get list of all submissions from Terra workspace."""

    print("Starting extraction of all submissions.")
    # get the submission data
    sub_details_json = fapi.list_submissions(ws_project, ws_name).json()

    # no submissions in workspace - returns empty list
    if not sub_details_json:
        raise ValueError(f"No submissions found in {ws_project}/{ws_name}.")

    return sub_details_json


def main(ws_project, ws_name):
    """Get failed and/or aborted workflows from submission and re-ingest to TDR dataset table for WFL submission retry."""
    
    # get all submissions in workspace
    all_submissions = get_workspace_submissions(ws_project, ws_name)
    
    # get all succeeded workflows across all submissions
    succeeded_wfs_by_sub = get_succeeded_workflows(ws_project, ws_name, all_submissions)
    
    # query each successful workflow for workflow outputs
    all_succeeded_wf_outputs = get_all_outputs(ws_project, ws_name, succeeded_wfs_by_sub)

    # write outputs to file
    write_outputs_to_file(all_succeeded_wf_outputs)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create new snapshot from rows that failed a Terra workflow submission')
    
    parser.add_argument('-p', '--ws_project', required=True, help='workspace project/namespace')
    parser.add_argument('-w', '--ws_name', required=True, help='workspace name')

    args = parser.parse_args()

    main(args.ws_project, args.ws_name)